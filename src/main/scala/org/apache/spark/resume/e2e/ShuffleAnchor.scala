package org.apache.spark.resume.e2e

import java.io._
import java.nio.channels.FileChannel

/** Proof-of-concept slice of LLD S7.1's `Anchor` record: L1 + L2 only (metadata + shuffle
  * adoption), no SQL layer. `key` stands in for `PlanDigest` (LLD S3.1) -- the caller supplies a
  * value stable across the two JVMs by construction (in the demo: a fixed logical name for the
  * one shuffle in the pipeline), rather than this project computing a real structural digest of
  * a Spark plan. That is a named, deliberate gap, not an oversight: S13.1 calls fingerprint
  * stability against real connector-backed scans the go/no-go question for the *design*, and it
  * is unrelated to whether the adoption mechanism below is correct. See resume-poc/README.md.
  *
  * `fileGroups` is Celeborn's own committed catalog for the shuffle -- partitionId -> the
  * protobuf-serialized PartitionLocations holding that partition's data -- captured verbatim
  * from the crashed driver's LifecycleManager and replayed into the new one
  * (LifecycleManager.adoptShuffle). This is what actually determines which bytes get read; see
  * MapOutputTrackerAccess for why the Spark-side MapStatus objects registered alongside it do
  * not need the same byte-for-byte fidelity in a Celeborn-backed shuffle.
  *
  * Stored as `Array[Byte]` (each one a serialized `PbPartitionLocation`, via
  * `CelebornAdoption.{serializeFileGroups,deserializeFileGroups}`), not as live `PartitionLocation`
  * objects: `PartitionLocation.storageInfo` carries a shaded-protobuf field
  * (`org.apache.celeborn.shaded.com.google.protobuf.LongArrayList`) that is not
  * `java.io.Serializable`, so a plain Java-serialization round-trip of the live object graph
  * fails. Celeborn already has a real wire format for exactly this object (used for every
  * getReducerFileGroup RPC response) -- reusing it here is more correct than it is convenient. */
case class ShuffleAnchor(
    schemaVersion: String,
    queryId: String,
    generation: Long,
    key: String,
    appShuffleId: Int,
    appShuffleIdentifier: String,
    celebornShuffleId: Int,
    numMaps: Int,
    numReduces: Int,
    fileGroups: Map[Int, Array[Array[Byte]]],
    // Captured verbatim from the crashed driver's CommitHandler, not synthesized: a mapper that
    // committed on a retry/speculative attempt has attemptId > 0, and Celeborn's reader filters
    // records by exactly this array (CommitHandler.getMapperAttempts). An all-zero placeholder
    // would silently drop that mapper's real data on the adopting side. See advisor note in
    // ReducePartitionCommitHandler.adoptCommittedShuffle.
    mapperAttempts: Array[Int],
    createdAtMs: Long,
    // Added in this project, not present in resume-poc's ShuffleAnchor -- found by hitting a
    // real, serious bug, not anticipated in design. `MapOutputStatistics.bytesByPartitionId`
    // (what `adopt` feeds MapOutputTrackerMaster/AQE's cost model) is NOT what
    // `ShuffleExchangeExec.runtimeStatistics` reads -- that reads `metrics("dataSize")` /
    // `metrics(SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN)`, plain SQLMetric
    // accumulators only ever incremented by an ACTUAL map task running
    // (`UnsafeRowSerializer`/`ShuffleWriteProcessor`). An adopted stage never runs one, so those
    // accumulators sit at their zero-initialized default -- and `AQEPropagateEmptyRelation`'s
    // `getEstimatedRowCount` reads EXACTLY `runtimeStatistics.rowCount`, so it sees 0 and
    // rewrites the entire downstream plan to `LocalTableScan <empty>`, discarding real, correctly
    // adopted data. Caught empirically (`run-e2e-demo.sh`'s first end-to-end pass: a matched
    // digest, a real Celeborn adopt, fewer tasks run, and a WRONG empty final result), not
    // predicted by design-aqe-and-corrupted-rerun.md S1, which only names
    // `MapOutputStatistics`-driven decisions. `E2EStageHook` captures these two real values at
    // materialization time (the same moment `onShuffleStageMaterialized` fires, after a REAL run,
    // so they are genuine) and, on adopt, writes them into the ADOPTING stage's own
    // `ShuffleExchangeExec.metrics` SQLMetric accumulators before returning -- see
    // `E2EStageHook.adoptAnchor`.
    dataSize: Long,
    rowCount: Long)
    extends Serializable

/** LLD S7's store contract, the three operations `ResumeCoordinator` actually needs. Two
  * implementations: `FileAnchorStore` (originally the only one -- a store dependency should
  * never gate testing the adoption mechanism, S9.4's `onUnavailable=degrade` says as much) and
  * `RedisAnchorStore` (LLD S7.3's real target, S7.4's real Lua CAS fencing, tested against a real
  * multi-writer race -- see RedisAnchorStore.scala). */
trait AnchorStore {
  def acquireGeneration(queryId: String): Long
  def putAnchor(gen: Long, anchor: ShuffleAnchor): Boolean
  def loadAnchors(queryId: String): Seq[ShuffleAnchor]
}

/** LLD S7 hot-path anchor store, S7.4 fencing -- file-backed instead of Redis, because the point
  * of this project is the adoption mechanism, not a store implementation, and a store dependency
  * should never gate testing that mechanism (S9.4's `onUnavailable=degrade` says as much: the
  * store is optional infrastructure). Fencing here uses a real OS file lock in place of Redis's
  * Lua CAS (S7.4) -- same contract: acquireGeneration is a monotonic fence, and a write for a
  * stale generation is refused rather than applied, so a zombie writer past its generation
  * writes nothing (invariant F-1). Not safe across a networked multi-writer store; that
  * property, not durability, is what Redis buys over this in production -- see
  * RedisAnchorStore for the version that actually is. */
class FileAnchorStore(root: File) extends AnchorStore {
  root.mkdirs()

  private def genFile(queryId: String): File = new File(root, s"$queryId.gen")
  private def dir(queryId: String): File = { val d = new File(root, queryId); d.mkdirs(); d }
  private def anchorFile(queryId: String, key: String): File =
    new File(dir(queryId), s"${key.replaceAll("[^A-Za-z0-9_.-]", "_")}.anchor")

  /** Monotonic fence (LLD invariant F-1). Every subsequent write for this queryId is gated on
    * the generation returned here. */
  def acquireGeneration(queryId: String): Long = withLock(genFile(queryId)) { raf =>
    val current = readLong(raf)
    val next = current + 1
    writeLong(raf, next)
    next
  }

  private def currentGeneration(queryId: String): Long = withLock(genFile(queryId))(readLong)

  /** Fence-gated write: refused (returns false) if `gen` is not the current generation for this
    * queryId. A losing writer must stop for the remainder of the query (LLD S7.4) -- callers are
    * responsible for that; this method only enforces the single-write check. */
  def putAnchor(gen: Long, anchor: ShuffleAnchor): Boolean = {
    if (gen != currentGeneration(anchor.queryId)) {
      false
    } else {
      val f = anchorFile(anchor.queryId, anchor.key)
      val tmp = new File(f.getParentFile, f.getName + ".tmp")
      val oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(tmp)))
      try oos.writeObject(anchor)
      finally oos.close()
      tmp.renameTo(f) // atomic on the same filesystem -- no reader ever observes a partial write
      true
    }
  }

  def loadAnchors(queryId: String): Seq[ShuffleAnchor] = {
    val d = dir(queryId)
    val files = Option(d.listFiles((_, name) => name.endsWith(".anchor"))).getOrElse(Array.empty)
    files.toSeq.flatMap { f =>
      val ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(f)))
      try Some(ois.readObject().asInstanceOf[ShuffleAnchor])
      catch {
        case e: Exception =>
          // Loud, not swallowed: a deserialization failure here (e.g. PartitionLocation's graph
          // -- StorageInfo, mapIdBitMap, peer -- failing to round-trip) must not present as "no
          // anchor found", which sends debugging in the wrong direction entirely.
          System.err.println(s"resume: failed to load anchor $f: $e")
          e.printStackTrace()
          None
      }
      finally ois.close()
    }
  }

  private def withLock[T](f: File)(body: RandomAccessFile => T): T = {
    if (!f.exists()) f.createNewFile()
    val raf = new RandomAccessFile(f, "rw")
    val channel: FileChannel = raf.getChannel
    val lock = channel.lock()
    try body(raf)
    finally { lock.release(); raf.close() }
  }

  private def readLong(raf: RandomAccessFile): Long = {
    raf.seek(0)
    if (raf.length() == 0) 0L else raf.readLong()
  }

  private def writeLong(raf: RandomAccessFile, v: Long): Unit = {
    raf.seek(0)
    raf.writeLong(v)
    raf.setLength(8)
  }
}
