package org.apache.spark.resume.e2e

import org.apache.spark.{MapOutputTrackerMaster, SparkContext}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.storage.BlockManagerId

/** LLD S6.1 -- "the single core-scheduler fact the entire design rests on": once
  * MapOutputTrackerMaster has a non-null MapStatus for every partition of a shuffle,
  * findMissingPartitions returns empty and DAGScheduler skips the map stage. No scheduler
  * change is required to exploit this.
  *
  * registerShuffle / registerMapOutput / findMissingPartitions are plain (non-private) defs on
  * MapOutputTrackerMaster, and the class itself is `private[spark]` -- Scala's qualified-private
  * grants access to the defining package AND its subpackages, and this file lives in
  * org.apache.spark.resume. So this compiles against the published spark-core jar with zero
  * Spark source changes; see resume-poc/pom.xml's header comment.
  *
  * For a Celeborn-backed shuffle, `CelebornShuffleReader` never consults `MapOutputTracker` for
  * data *location* at all (verified against ./celeborn: `CelebornShuffleReader.scala` reaches
  * data exclusively through `shuffleClient` / `LifecycleManager.getReducerFileGroup`) --
  * correctness of *what gets read* is carried entirely by the Celeborn-side fileGroups adoption
  * (`LifecycleManager.adoptShuffle`), which *is* byte-for-byte the crashed driver's catalog,
  * independent of anything registered here. `MapStatus.location` is therefore still a
  * placeholder (never dialed).
  *
  * `MapStatus` per-partition SIZE, though, is not inert: it feeds
  * `MapOutputTrackerMaster.getStatistics`, which is what `DAGScheduler.handleMapStageSubmitted`
  * hands to AQE (`ShuffleQueryStageExec.mapStats`) when a shuffle is already fully registered --
  * LLD S2.3's "pin MapOutputStatistics." A prior version of this file used a flat placeholder
  * size here, with the (correctly reasoned, at the time) justification that this project had no
  * SQL/AQE layer to feed. It now takes the real per-partition totals
  * `CelebornAdoption.partitionByteSizes` reads out of the exact same fileGroups data already
  * being adopted.
  *
  * The PER-MAPPER split of that total is not inert either -- checked by hitting a real, if
  * benign, consequence, not assumed from `getStatistics`'s own sum-across-mappers behavior
  * (which is correctly indifferent to the split, as `partitionByteSizes`'s doc comment says). A
  * prior version of this method put the whole real total on mapper 0 and a 1-byte placeholder on
  * every other mapper -- correct for `getStatistics`'s SUM, wrong for anything that reads
  * PER-MAPPER sizes. `OptimizeSkewedJoin`'s split-point algorithm
  * (`ShufflePartitionsUtil.splitSizeListByTargetSize`) does exactly that: given
  * `[realTotal, 1, 1, 1]`, its small-partition-merging logic collapses the trailing near-zero
  * placeholder mappers back into the one real chunk, concluding there is nothing worth splitting
  * -- so a partition byte-flagged as skewed (correctly, by total) silently never got an
  * `AQEShuffleReadExec` skew rewrite at all, for an adopted stage specifically (a real, live
  * shuffle would typically have each mapper contribute a roughly comparable share for this kind
  * of skew, which DOES produce a genuine multi-way split). Found building
  * `resume-poc-e2e/run-e2e-demo.sh`'s `skew-adopt` scenario: `skewedDuringCapture=true` (real
  * per-mapper distribution) but `skewedThisRun=false` on the very same query's adopted rerun.
  * Not a correctness bug -- `createSkewPartitionSpecs` either returns a full-mapper-range split
  * or `None` (falls back to reading the whole partition as one task), never a partial/wrong one
  * -- but it was a real, silent LOSS of AQE's skew optimization specifically for adopted
  * shuffles, which this project does not want to also silently mis-claim as "tested and
  * skew-split correctly" when the split path was never actually entered.
  *
  * Fixed by spreading each partition's real total EVENLY across all `numMaps` synthetic mapper
  * slots (`total/numMaps` per mapper, remainder distributed to the first few) instead of dumping
  * it all on mapper 0. This is strictly better on two independent axes, not a trade-off:
  * `getStatistics`'s per-partition SUM becomes exact (no more "+`numMaps`-1 byte" placeholder
  * noise the old scheme introduced), AND the per-mapper shape now resembles a real distribution
  * closely enough that `OptimizeSkewedJoin` can find a genuine split when one exists -- confirmed
  * by rerunning `skew-adopt` after this change, see `docs/APPROACH-AND-COVERAGE.md`.
  * `partitionSizes` defaults to empty for callers with no AQE-adjacent concern, which falls back
  * to a flat, evenly-1-byte-per-mapper shape -- this method's contract did not change, only what
  * a caller CAN now supply and how it's spread. */
object MapOutputTrackerAccess {

  def masterTracker(sc: SparkContext): MapOutputTrackerMaster =
    sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]

  // BlockManagerId asserts port > 0 whenever host is non-null; the port is otherwise never
  // dialed (see this file's doc comment -- Celeborn never consults it), so any positive value
  // does equally well.
  private val placeholderLoc = BlockManagerId("resume-adopted", "unused", 1)

  /** Seed the tracker for `shuffleId` as fully present. After this call
    * `findMissingPartitions(shuffleId)` is `Some(Seq.empty)` and the DAGScheduler will not
    * submit a single task for this shuffle's map stage. `partitionSizes`, keyed by reducer
    * partition id, are the real byte totals to report for AQE's sake (see class doc); a missing
    * entry for a given partition id falls back to size 1 (never 0 -- `HighlyCompressedMapStatus`
    * treats a 0-byte block as absent/empty, which would make AQE think that partition has no
    * data at all -- a wrong-shape signal, not merely an imprecise one). Spread evenly across
    * `numMaps` synthetic mapper slots -- see class doc for why that's load-bearing, not
    * cosmetic, for callers with a skew-splitting AQE rule in play. */
  def seedAdopted(
      tracker: MapOutputTrackerMaster,
      shuffleId: Int,
      numMaps: Int,
      numReduces: Int,
      partitionSizes: Map[Int, Long] = Map.empty): Unit = {
    // DAGScheduler.createShuffleMapStage performs the same containsShuffle check before
    // registering; ours only works if it runs first (LLD S8 P7 before P8), so guard rather than
    // let a mis-ordered caller crash on Spark's own "registered twice" instead of failing
    // clearly here.
    require(!tracker.containsShuffle(shuffleId),
      s"shuffle $shuffleId already registered -- seedAdopted must run before the DAGScheduler " +
        "sees this shuffle (LLD S8: P7 CONFIRM before P8 RUN)")
    tracker.registerShuffle(shuffleId, numMaps, numReduces)
    // Per reducer partition, split its real total as evenly as integer division allows across
    // `numMaps` synthetic mappers: base = total/numMaps on every mapper, plus one extra byte on
    // each of the first (total%numMaps) mappers so the per-mapper sizes sum to EXACTLY total,
    // not total+(numMaps-1) the way the old "everything on mapper 0" scheme did.
    val totals = Array.tabulate(numReduces)(r => partitionSizes.getOrElse(r, 1L).max(1L))
    val bases = totals.map(_ / numMaps)
    val remainders = totals.map(t => (t % numMaps).toInt)
    (0 until numMaps).foreach { mapIndex =>
      val sizes = Array.tabulate(numReduces) { reducerId =>
        bases(reducerId) + (if (mapIndex < remainders(reducerId)) 1L else 0L)
      }
      tracker.registerMapOutput(shuffleId, mapIndex, MapStatus(placeholderLoc, sizes, mapIndex.toLong))
    }
  }

  def missingPartitionCount(tracker: MapOutputTrackerMaster, shuffleId: Int): Int =
    tracker.findMissingPartitions(shuffleId).map(_.size).getOrElse(-1)
}
