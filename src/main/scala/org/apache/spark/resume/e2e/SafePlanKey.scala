package org.apache.spark.resume.e2e

import java.security.MessageDigest

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.types.StructType

/** Ported verbatim from resume-poc-sql's `SafePlanKey.scala` (design-aqe-and-corrupted-rerun.md
  * S1's node/scan-hashing primitives) -- only the package and the `private[sql]` -> `private[e2e]`
  * visibility changed, so `StageDigest.scala` in this package can reuse `exprString`/`fileDigest`/
  * `sha256Hex` the same way resume-poc-sql's `StageDigest.scala` does. Not used for whole-plan
  * gating in this project (there is no non-AQE `QueryResumeCoordinator` here, see this
  * directory's README) -- kept only because `StageDigest` depends on it. */
object SafePlanKey {

  def key(plan: SparkPlan): String = {
    val sb = new StringBuilder
    walk(plan, "0", sb)
    sha256Hex(sb.toString)
  }

  private def walk(plan: SparkPlan, path: String, sb: StringBuilder): Unit = plan match {
    case adaptive: AdaptiveSparkPlanExec =>
      walk(adaptive.executedPlan, path, sb)
    case stage: QueryStageExec =>
      walk(stage.plan, path, sb)
    case scan: FileSourceScanExec =>
      sb.append(s"[$path]SCAN:${fileDigest(scan)}\n")
    case node =>
      sb.append(s"[$path]NODE:${node.getClass.getSimpleName}:${exprString(node)}\n")
      node.children.zipWithIndex.foreach { case (child, i) => walk(child, s"$path.$i", sb) }
  }

  private[e2e] def exprString(plan: SparkPlan): String =
    plan.expressions.map(_.canonicalized.toString).mkString(",")

  private[e2e] def fileDigest(scan: FileSourceScanExec): String = {
    val partitionSchema: StructType = scan.relation.location.partitionSchema
    val staticFilters = scan.partitionFilters.filterNot(isDynamicPruningFilter)
    val dirs = scan.relation.location.listFiles(staticFilters, scan.dataFilters)
    val entries = dirs.flatMap { pd =>
      val partVals = partitionValuesString(pd.values, partitionSchema)
      pd.files.map(f => s"${f.getPath}|${f.getLen}|${f.getModificationTime}|$partVals")
    }
    sha256Hex(entries.sorted.mkString(";"))
  }

  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.exists(_.isInstanceOf[org.apache.spark.sql.catalyst.expressions.PlanExpression[_]])

  private def partitionValuesString(row: InternalRow, schema: StructType): String =
    schema.fields.zipWithIndex.map { case (f, i) =>
      if (row.isNullAt(i)) s"${f.name}=NULL" else s"${f.name}=${row.get(i, f.dataType)}"
    }.mkString(",")

  private[e2e] def sha256Hex(s: String): String = {
    val md = MessageDigest.getInstance("SHA-256")
    md.digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString
  }
}
