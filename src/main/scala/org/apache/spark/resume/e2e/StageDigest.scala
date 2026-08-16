package org.apache.spark.resume.e2e

import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}

/** Ported verbatim from resume-poc-sql's `StageDigest.scala` (design-aqe-and-corrupted-rerun.md
  * S1, proven cross-JVM there in isolation, commit `a90e644`) -- only the package changed. This
  * is the piece that project's own "what's not wired yet" note named as missing: here it is
  * actually consulted from `E2EStageHook.tryAdoptShuffleStage`, driving a real Celeborn-backed
  * shuffle-stage skip, not just computed and compared standalone. See that file for the full
  * Merkle-chaining rationale; unchanged here. */
object StageDigest {

  def stageDigest(
      stage: QueryStageExec,
      knownDigests: scala.collection.mutable.Map[Int, String]): String =
    knownDigests.getOrElseUpdate(stage.id, computeOwnDigest(stage, knownDigests))

  private def computeOwnDigest(
      stage: QueryStageExec,
      knownDigests: scala.collection.mutable.Map[Int, String]): String = {
    val sb = new StringBuilder
    walk(stage.plan, "0", sb, knownDigests)
    SafePlanKey.sha256Hex(sb.toString)
  }

  private def walk(
      plan: SparkPlan,
      path: String,
      sb: StringBuilder,
      knownDigests: scala.collection.mutable.Map[Int, String]): Unit = plan match {
    case adaptive: AdaptiveSparkPlanExec =>
      walk(adaptive.executedPlan, path, sb, knownDigests)
    case childStage: QueryStageExec =>
      sb.append(s"[$path]CHILDSTAGE:${stageDigest(childStage, knownDigests)}\n")
    case scan: FileSourceScanExec =>
      sb.append(s"[$path]SCAN:${SafePlanKey.fileDigest(scan)}\n")
    case node =>
      sb.append(s"[$path]NODE:${node.getClass.getSimpleName}:${SafePlanKey.exprString(node)}\n")
      node.children.zipWithIndex.foreach { case (child, i) =>
        walk(child, s"$path.$i", sb, knownDigests)
      }
  }
}
