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
  * being adopted -- see that method's doc comment for why putting a partition's whole real total
  * on one synthetic mapper entry and zero on the rest is sufficient (`getStatistics` sums across
  * mappers; it does not care about the per-mapper split). `partitionSizes` defaults to empty for
  * callers with no AQE-adjacent concern, which falls back to the original flat-1-byte behavior --
  * this method's contract did not change, only what a caller CAN now supply. */
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
    * data at all -- a wrong-shape signal, not merely an imprecise one). */
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
    (0 until numMaps).foreach { mapIndex =>
      // Whole real total on mapIndex 0, 1-byte placeholder elsewhere -- see class doc for why
      // the per-mapper split doesn't matter to anything that reads this back.
      val sizes = Array.tabulate(numReduces) { reducerId =>
        if (mapIndex == 0) partitionSizes.getOrElse(reducerId, 1L).max(1L) else 1L
      }
      tracker.registerMapOutput(shuffleId, mapIndex, MapStatus(placeholderLoc, sizes, mapIndex.toLong))
    }
  }

  def missingPartitionCount(tracker: MapOutputTrackerMaster, shuffleId: Int): Int =
    tracker.findMissingPartitions(shuffleId).map(_.size).getOrElse(-1)
}
