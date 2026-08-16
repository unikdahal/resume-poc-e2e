package org.apache.spark.resume.e2e

import java.util

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.celeborn.SparkShuffleManager

import org.apache.celeborn.client.LifecycleManager
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.network.TransportContext
import org.apache.celeborn.common.network.client.{TransportClient, TransportClientFactory}
import org.apache.celeborn.common.network.protocol.TransportMessage
import org.apache.celeborn.common.network.server.BaseMessageHandler
import org.apache.celeborn.common.protocol.{
  MessageType, PartitionLocation, PbOpenStream, PbOpenStreamList, PbOpenStreamListResponse,
  PbPartitionLocation, PbStreamHandler, PbStreamHandlerOpt, TransportModuleConstants}
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.{PbSerDeUtils, Utils => CelebornUtils}

/** Ported verbatim from resume-poc's `CelebornAdoption.scala` (commit `08498f7`, including the
  * rung 7.5 freshness check) -- only the package changed, so this project's `E2EStageHook` can
  * call the exact same, already-tested logic instead of a second implementation drifting from it.
  * Thin wrapper around the real, patched Celeborn LifecycleManager (see patch notes in
  * ./celeborn: LifecycleManager.adoptShuffle / confirmAlive, CommitHandler.adoptCommittedShuffle /
  * exportFileGroups, SparkShuffleManager.getLifecycleManager). LLD S6.2 / patch 10. */
object CelebornAdoption extends Logging {

  def lifecycleManager(sc: SparkContext): LifecycleManager =
    sc.env.shuffleManager.asInstanceOf[SparkShuffleManager].getLifecycleManager

  /** Read the celebornShuffleId Celeborn currently has on file for a Spark shuffle id, in
    * *this* driver -- used at capture time, in the still-alive run, to learn what to persist. */
  def resolveCelebornShuffleId(lcm: LifecycleManager, appShuffleId: Int): Option[(String, Int)] = {
    Option(lcm.getShuffleIdMapping.get(appShuffleId)).flatMap { ids =>
      ids.collectFirst { case (identifier, (celebornShuffleId, true)) => (identifier, celebornShuffleId) }
    }
  }

  /** New in this project, not present in `resume-poc`'s `CelebornAdoption` -- found by hitting a
    * real race, not anticipated in design. Celeborn's own map-side stage-end commit
    * (`LifecycleManager.handleMapperEnd`: the last mapper to finish triggers
    * `self.send(StageEnd(shuffleId))`, an ASYNCHRONOUS RPC-endpoint message, not a blocking call)
    * is NOT guaranteed complete by the time a map stage's own `MapOutputStatistics` future
    * resolves -- `self.send` only enqueues the message; `handleStageEnd` (which calls
    * `commitManager.tryFinalCommit` -> `collectResult`, the method that actually populates
    * `reducerFileGroupsMap`, which `exportFileGroups`/`serializeFileGroups` read from) runs
    * whenever the `LifecycleManager`'s own dispatcher thread gets to it. `resume-poc`'s RDD-level
    * capture never hits this: it only ever captures after `.collect()` returns, which waits for
    * the REDUCE stage too, giving the async message the whole reduce stage's wall-clock time to
    * land. A hook capturing at the EARLIEST possible point -- right as AQE's per-stage
    * materialization callback fires, before any reduce task exists yet -- has no such luxury, and
    * a naive `serializeFileGroups` call there can observe a genuinely-empty
    * `reducerFileGroupsMap` for a map stage that, from Spark's own point of view, completed
    * successfully. `CommitHandler.waitStageEnd` is Celeborn's own existing, public method for
    * exactly this (a bounded poll loop, `celeborn.client.push.stageEnd.timeout`) -- reused here,
    * not a new patch, and not called by anything in `resume-poc` because nothing there needed
    * it. MUST be called before `serializeFileGroups`, every time, for a hook capturing at
    * stage-materialization granularity. */
  def waitStageEnd(lcm: LifecycleManager, celebornShuffleId: Int): Boolean = {
    val (timedOut, costMs) = lcm.commitManager.getCommitHandler(celebornShuffleId).waitStageEnd(celebornShuffleId)
    if (timedOut) {
      logWarning(s"resume-e2e: waitStageEnd TIMED OUT for celebornShuffleId=$celebornShuffleId " +
        s"after ${costMs}ms -- Celeborn's own async stage-end commit did not land in time")
    }
    !timedOut
  }

  /** See ShuffleAnchor's doc comment: converts to the wire format (`PbPartitionLocation` bytes)
    * rather than persisting live `PartitionLocation` objects, because `PartitionLocation`'s
    * object graph is not `java.io.Serializable` end-to-end. */
  def serializeFileGroups(lcm: LifecycleManager, celebornShuffleId: Int): Map[Int, Array[Array[Byte]]] = {
    val raw = lcm.commitManager.getCommitHandler(celebornShuffleId).exportFileGroups(celebornShuffleId)
    raw.asScala.map { case (partitionId, locs) =>
      partitionId.intValue -> locs.asScala.map(PbSerDeUtils.toPbPartitionLocation(_).toByteArray).toArray
    }.toMap
  }

  private def deserializeFileGroups(
      fileGroups: Map[Int, Array[Array[Byte]]]): util.Map[Integer, util.Set[PartitionLocation]] = {
    val out = new util.HashMap[Integer, util.Set[PartitionLocation]]()
    fileGroups.foreach { case (partitionId, bytesArr) =>
      val locs: util.Set[PartitionLocation] = new util.HashSet()
      bytesArr.foreach { bytes =>
        locs.add(PbSerDeUtils.fromPbPartitionLocation(PbPartitionLocation.parseFrom(bytes)))
      }
      out.put(Integer.valueOf(partitionId), locs)
    }
    out
  }

  /** See ShuffleAnchor.mapperAttempts -- must be captured verbatim, not synthesized. */
  def exportMapperAttempts(lcm: LifecycleManager, celebornShuffleId: Int): Array[Int] =
    lcm.commitManager.getMapperAttempts(celebornShuffleId)

  /** LLD S2.3 "pin MapOutputStatistics": the real per-reducer-partition byte total, straight out
    * of data `serializeFileGroups` already captures -- no new capture mechanism, just reading a
    * field of what's already there. `PartitionLocation.storageInfo.fileSize` is Celeborn's own
    * on-disk file-size bookkeeping (`StorageInfo.java`), and a reducer partition's total is the
    * sum of every `PartitionLocation` covering it (a partition can have more than one location --
    * splits/epochs). Reads straight from the serialized `PbPartitionLocation` bytes rather than
    * the full `fromPbPartitionLocation` conversion `deserializeFileGroups` does -- cheaper, and
    * this is the only field needed here.
    *
    * Why this matters, concretely: `MapOutputTrackerMaster.getStatistics` sums
    * `MapStatus.getSizeForBlock(i)` ACROSS every mapper's entry for reducer `i` -- it does not
    * care how that total is distributed per-mapper (verified by reading
    * `MapOutputTracker.getStatistics`'s source, not assumed). So a byte-accurate PER-PARTITION
    * total is sufficient to feed AQE's cost decisions (`ShuffleQueryStageExec.mapStats`,
    * consumed via `DAGScheduler.handleMapStageSubmitted`'s `markMapStageJobAsFinished` when the
    * stage is already available -- the SAME `containsShuffle`/`isAvailable` check this project's
    * `seedAdopted` already satisfies, so AQE's own `submitMapStage` short-circuits for free, with
    * no additional Spark-core patch, PROVIDED the sizes it reads back out are real) -- exact
    * per-mapper attribution is not needed and this project makes no attempt to reconstruct it. */
  def partitionByteSizes(fileGroups: Map[Int, Array[Array[Byte]]]): Map[Int, Long] =
    fileGroups.map { case (partitionId, bytesArr) =>
      partitionId -> bytesArr.map(b => PbPartitionLocation.parseFrom(b).getStorageInfo.getFileSize).sum
    }

  /** Acceptance-ladder rung 7 (LLD S3.6, invariant S-1): Celeborn is the authority, never the
    * anchor store. A raw TCP reachability check ONLY -- see `confirmFresh` below, rung 7.5, for
    * the gap this leaves: a worker can be perfectly reachable while the exact file this anchor
    * points at has been superseded (Celeborn's own `stageRerun`/revive, triggered by a fetch
    * failure between capture and this check) or truncated/corrupted underneath it.
    * design-aqe-and-corrupted-rerun.md S2 is the design writeup this implements. */
  def confirmAlive(lcm: LifecycleManager, anchor: ShuffleAnchor): Boolean =
    lcm.confirmAlive(Map(anchor.celebornShuffleId -> deserializeFileGroups(anchor.fileGroups)))
      .contains(anchor.celebornShuffleId)

  /** A standalone data-plane `TransportClientFactory`, built the same way
    * `ShuffleClientImpl.initDataClientFactoryIfNeeded` builds its own (module = DATA_MODULE) --
    * not shared with any live Spark shuffle-read path, so this freshness probe can never contend
    * with or be starved by real reduce-task fetches, and callers own its lifecycle (`close()`
    * when done, same as any `Closeable`). */
  def buildDataClientFactory(conf: CelebornConf): TransportClientFactory = {
    val module = TransportModuleConstants.DATA_MODULE
    val transportConf = CelebornUtils.fromCelebornConf(conf, module, conf.networkIoThreads(module))
    val context = new TransportContext(
      transportConf, new BaseMessageHandler(), conf.clientCloseIdleConnections)
    context.createClientFactory()
  }

  /** Ground truth for one `PartitionLocation`, read live off the worker holding it, right now --
    * not from anything captured earlier. Reuses Celeborn's existing `OPEN_STREAM` RPC (the same
    * one a real reduce-task fetch opens first, see `WorkerPartitionReader`), called here purely
    * to read the `PbStreamHandler` reply's `chunkOffsets` -- no new Celeborn wire message, no
    * server-side patch. `startIndex=0, endIndex=Int.MaxValue` is Celeborn's own documented
    * sentinel for "every mapper's data in this file" (`FetchHandler.handleOpenStreamInternal`),
    * not a value this project invented.
    *
    * `readLocalShuffle=true` is load-bearing, not cosmetic -- checked by reading
    * `FetchHandler.handleReduceOpenStreamInternal`'s source, not assumed. The DEFAULT branch of
    * that handler (the one a real remote reduce-task fetch takes) calls
    * `makeStreamHandler(streamId, meta.getNumChunks)` with NO `offsets` argument at all --
    * `chunkOffsets` comes back genuinely empty on the wire regardless of the file's real length,
    * even though `numChunks` is populated. Only the `readLocalShuffle` branch calls
    * `makeStreamHandler(streamId, meta.getNumChunks, meta.getChunkOffsets, ...)`, passing the
    * real `ReduceFileMeta.chunkOffsets` list. That list is finalized to end with the file's true
    * total byte length by `PartitionMetaHandler.afterClose()` (`updateChunkOffset(fileLength,
    * force = true)` when `isChunkOffsetValid()` says the chunk-boundary bookkeeping didn't
    * already land there) -- guaranteed true once a file is committed, which every captured anchor
    * already is (capture only runs after the shuffle's map stage has committed, see
    * `ResumeCoordinator.capture`). This does not actually stream any bytes or read off the local
    * disk from here -- it only changes which branch of `FetchHandler` answers the RPC; the
    * `DiskFileInfo` cast that branch performs is safe for every backing store Celeborn supports
    * (`isHdfs`/`isS3`/`isOSS` are themselves fields ON `DiskFileInfo`, not a separate type), and
    * the `MemoryFileInfo` case is excluded by the handler itself, before this ever reaches wire.
    * A stream left open this way without a follow-up `CHUNK_FETCH`/close is not a leak: Celeborn's
    * own `ChunkStreamManager.registerStream` doc comment names exactly this --
    * "if stream is not properly closed, it will eventually be cleaned up by
    * `cleanupExpiredShuffleKey`" -- so a fire-and-forget freshness probe is a supported use, not
    * an abuse of the RPC.
    *
    * The LAST offset in the (now genuinely populated) `chunkOffsets` list is the file's current
    * total byte length. `getRawFileInfo` on the worker side is a live lookup made at request time,
    * so a location Celeborn has since revived/removed/replaced throws there, surfacing here as
    * `None`, exactly like a genuinely dead worker does at `confirmAlive`. `None` MUST be treated
    * as "not fresh" by every caller -- never optimistic.
    *
    * Empty `chunkOffsets` is NOT read as "zero-byte file" -- that reading was tried and is wrong.
    * `ReduceFileMeta`'s own constructor always seeds the list with a leading `0L`
    * (`chunkOffsets.add(0L)`), so even a genuinely empty file reports `chunkOffsets=[0]` (size 1,
    * `numChunks=0`), never size 0. A truly empty list on the wire means the `readLocalShuffle`
    * branch was NOT the one that answered -- e.g. `fileInfo` turned out to be a `MemoryFileInfo`,
    * which that branch's own guard excludes, silently falling through to the no-offsets default
    * branch this method exists to avoid. Treated as a probe failure (`None`), not a length,
    * consistent with A-1: an inconclusive read must never be read as "confirmed fresh". Likewise
    * cross-checked against `numChunks` (`ReduceFileMeta.getNumChunks = chunkOffsets.size - 1`,
    * by construction) -- any other relationship means the response didn't mean what this method
    * assumes it means, and the safe move is the same: don't guess, report `None`. */
  def currentFileLength(
      clientFactory: TransportClientFactory,
      shuffleKey: String,
      loc: PartitionLocation,
      timeoutMs: Int): Option[Long] =
    currentFileLengths(clientFactory, shuffleKey, Seq(loc), timeoutMs)(loc)

  /** Shared by both the single- and batched-probe paths: turns one worker's `PbStreamHandler`
    * reply into a length, or `None` if the reply's shape doesn't mean what a caller might assume
    * it means. See `currentFileLengths`'s doc comment for why each check exists. */
  private def lengthFromStreamHandler(fileName: String, handler: PbStreamHandler): Option[Long] = {
    val offsets = handler.getChunkOffsetsList
    logDebug(s"resume: freshness probe file=$fileName numChunks=${handler.getNumChunks} " +
      s"chunkOffsets=$offsets fullPath=${handler.getFullPath}")
    if (offsets.isEmpty) {
      logWarning(s"resume: freshness probe for $fileName got an empty chunkOffsets list -- " +
        "readLocalShuffle branch likely did not answer (e.g. MemoryFileInfo); treating as NOT " +
        "fresh rather than guessing 0 bytes")
      None
    } else if (offsets.size != handler.getNumChunks + 1) {
      logWarning(s"resume: freshness probe for $fileName got inconsistent " +
        s"numChunks=${handler.getNumChunks} vs chunkOffsets.size=${offsets.size} (expected " +
        s"numChunks+1) -- treating as NOT fresh rather than trusting an unexplained shape")
      None
    } else {
      Some(offsets.get(offsets.size - 1).longValue)
    }
  }

  /** Batched form of `currentFileLength`: ONE `BATCH_OPEN_STREAM` RPC per distinct
    * `(host, fetchPort)` among `locs`, not one `OPEN_STREAM` RPC per `PartitionLocation` --
    * matters because a real shuffle can have thousands of reducer partitions, each possibly with
    * more than one `PartitionLocation` (splits/epochs -- see `partitionByteSizes`'s doc comment),
    * and `confirmFresh` must probe every single one before an adopt can proceed; O(locations)
    * sequential round trips at `timeoutMs` each would make adopt-time cost scale with shuffle
    * width instead of worker count. `PbOpenStreamList` / `PbOpenStreamListResponse` /
    * `MessageType.BATCH_OPEN_STREAM{,_RESPONSE}` are Celeborn's own existing batched-fetch
    * messages (`FetchHandler`'s `case openStreamList: PbOpenStreamList =>` branch) -- reused
    * here for the same reason the single-location path reuses plain `OPEN_STREAM`: no new wire
    * message, no server-side patch. The worker answers each entry independently and in the same
    * order as the request lists (`0 until files.size() foreach { idx => ... }` in
    * `FetchHandler`), so `group.zip(opts.asScala)` below is a positional pairing, not a lookup --
    * verified by reading that loop, not assumed. A whole-batch RPC failure (e.g. the worker died
    * between `confirmAlive` and this call) fails every location in that group as `None`, exactly
    * as a per-location failure would have; a `PbStreamHandlerOpt` with a non-`SUCCESS` status for
    * one entry (e.g. that one file specifically was deleted, others on the same worker were not)
    * fails only that entry, matching per-location OPEN_STREAM's own per-request failure
    * granularity. */
  def currentFileLengths(
      clientFactory: TransportClientFactory,
      shuffleKey: String,
      locs: Seq[PartitionLocation],
      timeoutMs: Int): Map[PartitionLocation, Option[Long]] = {
    locs.groupBy(loc => (loc.getHost, loc.getFetchPort)).flatMap { case ((host, port), group) =>
      try {
        val client: TransportClient = clientFactory.createClient(host, port)
        val listBuilder = PbOpenStreamList.newBuilder().setShuffleKey(shuffleKey)
        group.foreach { loc =>
          listBuilder.addFileName(loc.getFileName)
          listBuilder.addStartIndex(0)
          listBuilder.addEndIndex(Int.MaxValue)
          listBuilder.addReadLocalShuffle(true)
        }
        val request = new TransportMessage(MessageType.BATCH_OPEN_STREAM, listBuilder.build().toByteArray)
        val response = client.sendRpcSync(request.toByteBuffer, timeoutMs)
        val listResponse = TransportMessage.fromByteBuffer(response)
          .getParsedPayload.asInstanceOf[PbOpenStreamListResponse]
        val opts = listResponse.getStreamHandlerOptList
        require(opts.size == group.size, s"resume: BATCH_OPEN_STREAM to $host:$port answered " +
          s"${opts.size} entries for a ${group.size}-entry request -- refusing to guess a " +
          "positional pairing")
        group.zip(opts.asScala).map { case (loc, opt: PbStreamHandlerOpt) =>
          if (opt.getStatus != StatusCode.SUCCESS.getValue) {
            logWarning(s"resume: freshness probe for ${loc.getFileName} on $host:$port failed: " +
              s"${opt.getErrorMsg} -- treating as NOT fresh")
            loc -> None
          } else {
            loc -> lengthFromStreamHandler(loc.getFileName, opt.getStreamHandler)
          }
        }
      } catch {
        case NonFatal(e) =>
          logWarning(s"resume: BATCH_OPEN_STREAM freshness probe failed for $host:$port " +
            s"(${group.size} location(s)) -- treating all as NOT fresh: ${e.getMessage}")
          group.map(loc => loc -> None)
      }
    }
  }

  /** Acceptance-ladder rung 7.5 (design-aqe-and-corrupted-rerun.md S2): every `PartitionLocation`
    * in the anchor must CURRENTLY report the exact byte length it had when captured, read live
    * off the worker that holds it -- not merely "the worker is up" (`confirmAlive`). Rejects
    * (conservatively, all-or-nothing -- consistent with A-1: losing state is safe, wrong state is
    * catastrophic) if ANY location has been superseded, truncated, or become unreachable since
    * capture. `shuffleKey` must match Celeborn's own convention exactly
    * (`Utils.makeShuffleKey(appUniqueId, celebornShuffleId)`) -- the same string Celeborn's own
    * `FetchHandler` derives server-side to look up the file. Probes every location across every
    * partition in ONE pass via `currentFileLengths` (batched per worker), not partition by
    * partition -- see that method's doc comment for why. */
  def confirmFresh(
      lcm: LifecycleManager,
      anchor: ShuffleAnchor,
      clientFactory: TransportClientFactory,
      timeoutMs: Int = 2000): Boolean = {
    val shuffleKey = CelebornUtils.makeShuffleKey(lcm.appUniqueId, anchor.celebornShuffleId)
    val fileGroups = deserializeFileGroups(anchor.fileGroups)
    if (fileGroups.asScala.exists { case (_, locs) => locs.isEmpty }) {
      logWarning("resume: freshness check rejecting -- at least one partition has zero captured locations")
      return false
    }
    val allLocs = fileGroups.asScala.valuesIterator.flatMap(_.asScala).toSeq
    val liveLengths = currentFileLengths(clientFactory, shuffleKey, allLocs, timeoutMs)
    val allFresh = allLocs.forall { loc =>
      val capturedLength = loc.getStorageInfo.getFileSize
      liveLengths(loc) match {
        case Some(liveLength) if liveLength == capturedLength => true
        case Some(liveLength) =>
          logWarning(s"resume: freshness mismatch file=${loc.getFileName} " +
            s"captured=$capturedLength live=$liveLength -- rejecting (superseded or truncated " +
              "since capture)")
          false
        case None => false
      }
    }
    allFresh
  }

  def adopt(lcm: LifecycleManager, newAppShuffleId: Int, anchor: ShuffleAnchor): Boolean =
    lcm.adoptShuffle(
      newAppShuffleId,
      s"resume-adopted-${anchor.key}",
      anchor.celebornShuffleId,
      anchor.numMaps,
      anchor.numReduces,
      deserializeFileGroups(anchor.fileGroups),
      anchor.mapperAttempts)
}
