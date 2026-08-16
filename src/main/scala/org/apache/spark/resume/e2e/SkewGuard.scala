package org.apache.spark.resume.e2e

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.{PartialMapperPartitionSpec, PartialReducerPartitionSpec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.internal.SQLConf

/** Ported from `resume-poc`'s `SkewGuard.scala` -- only the package changed, plus this doc
  * comment, because `resume-poc-e2e` genuinely DOES have a `SparkPlan` in scope
  * (`E2EStageHook`/`Demo` operate on real `DataFrame`s, not raw RDDs), which is what
  * `resume-poc`'s version says it was missing. That turned out not to be enough to wire this in
  * the obvious place, though -- checked, not assumed, before building anything:
  *
  * `hasSkewPartitionSpecs` looks for `AQEShuffleReadExec` nodes, which wrap a materialized
  * `ShuffleQueryStageExec` from ABOVE, as its CONSUMER. `E2EStageHook.tryAdoptShuffleStage(stage:
  * ShuffleQueryStageExec)` only ever sees `stage.plan` -- that stage's own frozen subtree, which
  * by construction never contains anything that consumes it. Worse: `OptimizeSkewedJoin` decides
  * whether to skew-split a stage's output USING that stage's own just-materialized per-partition
  * byte stats -- for a stage we are about to ADOPT rather than materialize, those stats do not
  * exist yet at the moment `tryAdoptShuffleStage` must decide. There is no sound way to reject an
  * adoption in advance because it MIGHT later get skew-split; the information doesn't exist yet.
  *
  * So this is used as a POST-HOC check instead, on the run's FINAL converged plan (`Demo.scala`'s
  * `skew-adopt` mode), after real execution -- catching "did this run's adopted stage actually
  * get skew-split by its consumer" as a fact, not a prediction.
  *
  * Two rounds of empirical checking here, not one -- see `docs/APPROACH-AND-COVERAGE.md` for the
  * full account:
  *
  * 1. Structural safety, verified from source: `ShufflePartitionsUtil.createSkewPartitionSpecs`
  *    always partitions the FULL mapper-index range `[0, numMappers)` into contiguous,
  *    non-overlapping sub-ranges by construction -- so no per-mapper size estimate fed to it, no
  *    matter how wrong, can make a skew split skip or duplicate a mapper's real data. This holds
  *    regardless of what `seedAdopted` feeds it.
  * 2. A real, separate bug that structural safety alone didn't catch: `skew-adopt`'s first pass
  *    showed `skewedDuringCapture=true` (real per-mapper distribution) but `skewedThisRun=false`
  *    on the identical query's adopted rerun -- `seedAdopted`'s OLD per-mapper split (whole real
  *    total on mapper 0, 1-byte placeholders elsewhere) made `OptimizeSkewedJoin`'s own
  *    small-partition-merging logic conclude there was nothing worth splitting, so the skew-split
  *    code path this scenario exists to exercise was silently never entered at all -- not unsafe,
  *    but untested, and a real, silent loss of AQE's skew optimization for every adopted shuffle.
  *    Fixed in `MapOutputTrackerAccess.seedAdopted` (spread the real total evenly across mappers
  *    instead of dumping it on one) -- see that method's doc comment for the fix. With the fix,
  *    `skew-adopt` reliably reproduces `skewedThisRun=true` AND a correct result, so the
  *    structural argument in (1) is now confirmed in practice, against the actual code path,
  *    not just reasoned about from source. */
object SkewGuard {

  /** LLD S5.5: "assert local shuffle reader is off when Celeborn is the backend." Celeborn's own
    * `SparkShuffleManager` only *warns* when this is left on (`SparkShuffleManager.java`, "it's
    * highly recommended to disable it") -- it does not enforce it, so this is a real gap this
    * project's harness would otherwise leave open, not a check duplicating one Celeborn already
    * makes. `PartialMapperPartitionSpec`'s local-read semantics assume co-location with the
    * mapper's host, which is meaningless once the shuffle service is remote; leaving this on is
    * therefore not a performance-only misconfiguration when Celeborn is the backend, it is a
    * precondition this design depends on. Throws (does not merely log) on capture and on
    * adopt -- both are points a stale/misconfigured SparkConf could reach this check. */
  def assertLocalShuffleReaderOff(conf: SparkConf): Unit = {
    val key = SQLConf.LOCAL_SHUFFLE_READER_ENABLED.key
    val enabled = conf.getBoolean(key, SQLConf.LOCAL_SHUFFLE_READER_ENABLED.defaultValue.get)
    require(!enabled,
      s"resume: $key=true with Celeborn as the shuffle backend -- LLD S5.5 requires this off; " +
        "Celeborn only warns about it, it does not enforce it, so a resumable-driver capture or " +
        "adopt must refuse rather than silently proceed on a co-location assumption that does " +
        "not hold under a remote shuffle service")
  }

  /** LLD S5.5: AQE's `OptimizeSkewedJoin` produces `PartialReducerPartitionSpec` /
    * `PartialMapperPartitionSpec` -- ranges *within* a map output, not whole partitions. Restoring
    * correct `MapStatus`es while a different skew split gets recomputed means downstream
    * partitions read different byte ranges of the same map output than the ones the specs
    * describe: no error, wrong aggregates. Celeborn itself refuses to reuse a shuffle it
    * classifies as a skew shuffle or a child of one
    * (`LifecycleManager.isCelebornSkewShuffleOrChildShuffle`); this mirrors that refusal at the
    * Spark-plan level, walking every `AQEShuffleReadExec` reachable from `plan` (there can be more
    * than one -- every shuffle boundary in the query gets its own) rather than checking only the
    * root. */
  def hasSkewPartitionSpecs(plan: SparkPlan): Boolean = {
    def walk(p: SparkPlan): Boolean = {
    if (sys.env.contains("RESUME_POC_DEBUG_SKEW")) {
      System.err.println(s"RESUME-POC-SKEW-DEBUG walk node=${p.getClass.getName}")
    }
    p match {
      // AdaptiveSparkPlanExec extends LeafExecNode -- .children is Nil by construction, so
      // without this case the walk silently never reaches the real finalized plan tree
      // (currentPhysicalPlan/.executedPlan) it wraps, and every AQE query looks skew-free.
      // Caught only by actually running this against a real AQE skew plan, not by inspection.
      case adaptive: AdaptiveSparkPlanExec =>
        walk(adaptive.executedPlan)
      // Same LeafExecNode-wrapper shape as AdaptiveSparkPlanExec, one level down: every shuffle/
      // broadcast boundary materializes as a QueryStageExec whose actual subtree (which is where
      // nested AQEShuffleReadExec nodes for upstream joins live, per this project's own plan dump)
      // hangs off `.plan`, not `.children`.
      case stage: QueryStageExec =>
        walk(stage.plan)
      case read: AQEShuffleReadExec =>
        if (sys.env.contains("RESUME_POC_DEBUG_SKEW")) {
          read.partitionSpecs.foreach(s => System.err.println(s"RESUME-POC-SKEW-DEBUG spec=${s.getClass.getName} $s"))
        }
        read.partitionSpecs.exists {
          case _: PartialReducerPartitionSpec => true
          case _: PartialMapperPartitionSpec => true
          case _ => false
        } || walk(read.child)
      case other =>
        other.children.exists(walk)
    }
    }
    walk(plan)
  }
}
