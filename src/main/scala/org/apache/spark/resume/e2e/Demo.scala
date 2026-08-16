package org.apache.spark.resume.e2e

import java.io.File

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.execution.ResumeHooks
import org.apache.spark.sql.functions._
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}

/** The combined demo `docs/APPROACH-AND-COVERAGE.md` names as the missing piece: a real AQE SQL
  * query, backed by a real Celeborn cluster, where a second JVM's shuffle stage is genuinely
  * SKIPPED (not just "the digest matched in a unit test") because of a match on
  * `StageDigest` + a real `CelebornAdoption.confirmAlive`/`confirmFresh`/`adopt` sequence -- and
  * where that skip survives the SAME later-stage-takes-a-different-path pressure
  * `StageDigestDemo` already proved for the digest alone (design note S1), now with real shuffle
  * bytes and a real second JVM behind it (design note S2's rung 7.5 included, not bypassed).
  *
  * Query shape and the differing-join-strategy trick are unchanged in kind from resume-poc-sql's
  * `StageDigestDemo`: `fact.groupBy("dim_key").agg(sum("amount"))` joined to a 20-row `dim` table
  * (widened from that demo's 5 rows -- see `dimRows`'s doc comment for why: with only 5 distinct
  * keys, 2 of the fact-side aggregate's 4 shuffle partitions came back genuinely empty, which is
  * plausible on its own but indistinguishable from a capture-side bug silently dropping data;
  * 20 keys makes "all 4 partitions have real data" a safe assumption instead of a coin flip).
  * `capture` forces a shuffle join (`autoBroadcastJoinThreshold=1`); `adopt` forces a broadcast
  * join (`autoBroadcastJoinThreshold=10MB`) against the SAME fact/dim data. The fact-side
  * aggregate's shuffle stage cannot be affected by that threshold either way -- it is upstream of
  * the join entirely -- so its `StageDigest` is identical across both runs, while the join's own
  * shuffle stage(s) (present under SortMergeJoin, absent entirely under BroadcastHashJoin) are
  * not, and are never even looked up in the `adopt` run.
  *
  * Proof obligations, all checked (not just printed):
  *   1. `adopt`'s result equals `capture`'s result (independently checked against a
  *      non-Spark-computed expected value, not just "the two runs agree with each other").
  *   2. `adopt` runs strictly fewer tasks than a COLD run of the SAME broadcast-join query
  *      (`adopt-cold` mode, an empty anchor store) -- not fewer tasks than `capture`, which
  *      forces a structurally different join strategy and would show a task-count drop even
  *      with zero adoptions. Caught by the advisor before this was shipped as a claim: the first
  *      version of this check compared against `capture` and was confounded.
  *   3. `E2EStageHook.events` on the `adopt` run contains at least one `StageAdopted`, and its
  *      digest is one that also appears as a `StageCaptured` event from the `capture` run --
  *      not merely "fewer tasks ran" (which a totally different bug could also produce), but
  *      "fewer tasks ran BECAUSE this specific mechanism fired for this specific stage."
  * See `run-e2e-demo.sh` for the orchestration and exact assertions. */
object Demo {

  val QUERY_ID = "resume-poc-e2e-demo"

  def factPath(base: String) = s"$base/fact"
  def dimPath(base: String) = s"$base/dim"

  // 20 distinct keys, not 5: with only 5 distinct dim_key values, Spark's hash partitioning
  // (Murmur3 over 4 shuffle partitions) put real data in only 2 of the 4 partitions in practice --
  // plausible on its own ("some hash buckets legitimately got zero of five keys"), but also
  // indistinguishable from a REAL bug (`CelebornAdoption.waitStageEnd` returning after only a
  // partial commit landed, silently dropping partitions). Caught by the advisor, not by staring
  // at the numbers: fixed by widening the key domain enough that all 4 partitions are all but
  // certain to receive real data, so `fileGroups.size == 4` becomes a meaningful assertion
  // instead of an unfalsifiable one. See run-e2e-demo.sh's extra check.
  val dimRows: Seq[(Int, String)] = (0 until 20).map(i => (i, s"region$i"))
  val NUM_FACT_ROWS = 4000

  /** Independently computed (no Spark involved) so a correct-looking result on both runs can't
    * hide a bug both runs happen to share -- same standard resume-poc's `Demo.expected` and
    * resume-poc-sql's demos hold their own results to. */
  def expected: Map[String, Long] = {
    val sums = (0 until NUM_FACT_ROWS).groupBy(_ % dimRows.size)
      .map { case (k, is) => k -> is.map(_.toLong).sum }
    dimRows.map { case (id, name) => name -> sums.getOrElse(id, 0L) }.toMap
  }

  def newSession(base: String, masterEndpoint: String, appUniqueId: String, autoBroadcastThreshold: Long): SparkSession =
    SparkSession.builder().appName("resume-poc-e2e")
      .master("local[4]")
      .config("spark.shuffle.manager", "org.apache.spark.shuffle.celeborn.SparkShuffleManager")
      .config("spark.shuffle.service.enabled", "false")
      .config("spark.celeborn.master.endpoints", masterEndpoint)
      .config("spark.celeborn.client.application.uniqueId", appUniqueId)
      .config("spark.celeborn.client.application.uuidSuffix.enabled", "false")
      .config("spark.celeborn.client.spark.stageRerun.enabled", "true")
      .config("spark.celeborn.client.push.replicate.enabled", "false")
      .config("spark.celeborn.master.heartbeat.application.timeout", "600s")
      // LLD S5.5 -- required with Celeborn as the shuffle backend, same as resume-poc.
      .config("spark.sql.adaptive.localShuffleReader.enabled", "false")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.autoBroadcastJoinThreshold", autoBroadcastThreshold.toString)
      .getOrCreate()

  def setupTables(spark: SparkSession, base: String): Unit = {
    import spark.implicits._
    dimRows.toDF("id", "name").write.mode(SaveMode.Overwrite).parquet(dimPath(base))
    (0 until NUM_FACT_ROWS).map(i => (i % dimRows.size, i.toLong)).toDF("dim_key", "amount")
      .write.mode(SaveMode.Overwrite).parquet(factPath(base))
  }

  def query(spark: SparkSession, base: String): DataFrame = {
    val fact = spark.read.parquet(factPath(base))
    val dim = spark.read.parquet(dimPath(base))
    val grouped = fact.groupBy("dim_key").agg(sum("amount").as("total"))
    grouped.join(dim, grouped("dim_key") === dim("id")).select(dim("name"), col("total"))
  }

  def checkResult(rows: Array[org.apache.spark.sql.Row]): Unit = {
    val got = rows.map(r => r.getString(0) -> r.getLong(1)).toMap
    require(got == expected, s"result mismatch: got=$got expected=$expected")
  }

  class TaskCounter extends SparkListener {
    @volatile var count = 0
    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized { count += 1 }
  }

  // ---------------------------------------------------------------------------------------------
  // Skew scenario (see SkewGuard.scala's doc comment for why this exists and what it checks):
  // does adopting a shuffle stage whose consumer later skew-splits it produce a CORRECT result,
  // given the fabricated per-mapper size distribution `MapOutputTrackerAccess.seedAdopted` feeds
  // AQE (real total on mapper 0, 1-byte placeholders elsewhere)? And separately: does capturing
  // this shuffle's anchor BEFORE its own consumer skew-reads it (which re-sorts the underlying
  // Celeborn file) leave rung 7.5's later freshness probe -- always a WHOLE-file request -- still
  // able to compute the correct total length against that now-sorted file? Same fixture answers
  // both questions, per the advisor's suggestion. Recipe unchanged from resume-poc's `SkewDemo`.
  val SKEW_QUERY_ID = "resume-poc-e2e-skew-demo"
  val SKEW_EXPECTED_COUNT = 30008L

  def newSkewSession(masterEndpoint: String, appUniqueId: String): SparkSession =
    SparkSession.builder().appName("resume-poc-e2e-skew")
      .master("local[4]")
      .config("spark.shuffle.manager", "org.apache.spark.shuffle.celeborn.SparkShuffleManager")
      .config("spark.shuffle.service.enabled", "false")
      .config("spark.celeborn.master.endpoints", masterEndpoint)
      .config("spark.celeborn.client.application.uniqueId", appUniqueId)
      .config("spark.celeborn.client.application.uuidSuffix.enabled", "false")
      .config("spark.celeborn.client.spark.stageRerun.enabled", "true")
      .config("spark.celeborn.client.push.replicate.enabled", "false")
      .config("spark.celeborn.master.heartbeat.application.timeout", "600s")
      .config("spark.sql.adaptive.localShuffleReader.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.skewJoin.enabled", "true")
      // Same reasoning as SkewDemo: coalescing off avoids noise, small thresholds trip real
      // skew detection on demo-sized data, force SortMergeJoin (OptimizeSkewedJoin only rewrites
      // SortMergeJoinExec).
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "100")
      .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2")
      .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "100")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.sql.join.preferSortMergeJoin", "true")
      .getOrCreate()

  /** `big`'s own shuffle exchange is the stage this project captures/adopts -- analogous to
    * `fact` in the main demo. Key 0 gets 30000 rows; keys 1-7 get 8 rows total -- an extreme,
    * obvious skew ratio, same as `SkewDemo`, so the point is tripping real AQE skew detection,
    * not finding the minimum ratio that does. */
  def skewedJoin(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val bigKeys = (0 until 30000).map(_ => 0) ++ (0 until 8).map(i => 1 + i % 7)
    val big = bigKeys.zipWithIndex.map { case (k, i) => (k, i.toLong, s"payload-$i-${"x" * 200}") }
      .toDF("k", "v", "payload")
    val small = (0 until 8).map(i => (i, s"name$i")).toDF("k", "name")
    big.join(small, "k").agg(count("*"))
  }

  def main(args: Array[String]): Unit = {
    val mode = args(0) // setup | capture | adopt | adopt-cold | skew-capture | skew-adopt | adopt-kill-before-fetch
    val base = args(1)
    // args(2) is the anchor-store directory, required for capture/adopt (setup writes no
    // anchors and doesn't need one). args(3), if present, overrides the Celeborn master
    // endpoint -- default matches conf/celeborn-defaults.conf's celeborn.master.port.
    val masterEndpoint = if (args.length > 3) args(3) else "localhost:9097"

    mode match {
      case "setup" =>
        val spark = newSession(base, masterEndpoint, QUERY_ID + "-setup", autoBroadcastThreshold = 1)
        setupTables(spark, base)
        spark.stop()
        println("RESUME-POC-E2E SETUP-DONE")

      case "capture" =>
        val store = new FileAnchorStore(new File(args(2)))
        val spark = newSession(base, masterEndpoint, QUERY_ID, autoBroadcastThreshold = 1)
        val hook = new E2EStageHook(spark.sparkContext, store, QUERY_ID, HookMode.Capture)
        ResumeHooks.stage = Some(hook)
        val counter = new TaskCounter
        spark.sparkContext.addSparkListener(counter)
        try {
          val rows = query(spark, base).collect()
          checkResult(rows)
          val captured = hook.events.collect { case e: StageCaptured => e.digest }
          println(s"RESUME-POC-E2E CAPTURE tasksRun=${counter.count} result=OK " +
            s"stagesCaptured=${captured.size} digests=${captured.mkString(",")}")
        } finally {
          hook.close()
          ResumeHooks.stage = None
        }
        // Deliberately not spark.stop() -- same reasoning as resume-poc's Demo.capture: a
        // graceful shutdown is Celeborn's cue to reclaim this app's shuffles, which is exactly
        // the byte-availability guarantee the adopt run depends on, and this scenario is a
        // crash, not an orderly shutdown.
        Runtime.getRuntime.halt(0)

      case "adopt" =>
        val store = new FileAnchorStore(new File(args(2)))
        val spark = newSession(base, masterEndpoint, QUERY_ID, autoBroadcastThreshold = 10L * 1024 * 1024)
        val hook = new E2EStageHook(spark.sparkContext, store, QUERY_ID, HookMode.Adopt)
        ResumeHooks.stage = Some(hook)
        val counter = new TaskCounter
        spark.sparkContext.addSparkListener(counter)
        try {
          val df = query(spark, base)
          val rows = df.collect()
          val adopted = hook.events.collect { case e: StageAdopted => e.digest }
          val rejected = hook.events.collect { case e: StageRejected => e }
          val missed = hook.events.collect { case e: StageMissNoAnchor => e.digest }
          val plan = df.queryExecution.executedPlan.toString
          // Printed BEFORE checkResult deliberately: if checkResult fails, these diagnostics
          // must still be on stdout to explain why, not lost to an uncaught exception.
          println(s"RESUME-POC-E2E ADOPT tasksRun=${counter.count} " +
            s"stagesAdopted=${adopted.size} digests=${adopted.mkString(",")} " +
            s"missed=${missed.mkString(",")} rejected=${rejected.mkString(";")}")
          println(s"RESUME-POC-E2E ADOPT plan=\n$plan")
          require(adopted.nonEmpty,
            "expected at least one shuffle stage to be adopted -- see hook.events for the miss/reject reasons")
          checkResult(rows)
          println("RESUME-POC-E2E ADOPT result=OK")
        } finally {
          hook.close()
          ResumeHooks.stage = None
          spark.stop()
        }

      // The control the advisor caught was missing: SAME query shape/threshold as `adopt`
      // (broadcast join), but pointed at an anchor store with nothing in it (args(2) must be an
      // EMPTY directory -- run-e2e-demo.sh uses a directory `capture` never wrote to), so nothing
      // can be adopted regardless of whether the mechanism works. This is the cold baseline
      // "adopt runs fewer tasks" must be measured against -- NOT the `capture` run, which forces
      // a structurally different join strategy (SortMergeJoin, an extra dim-side shuffle stage)
      // and would show a task-count drop even with zero adoptions.
      case "adopt-cold" =>
        val store = new FileAnchorStore(new File(args(2)))
        val spark = newSession(base, masterEndpoint, QUERY_ID + "-cold", autoBroadcastThreshold = 10L * 1024 * 1024)
        val hook = new E2EStageHook(spark.sparkContext, store, QUERY_ID, HookMode.Adopt)
        ResumeHooks.stage = Some(hook)
        val counter = new TaskCounter
        spark.sparkContext.addSparkListener(counter)
        try {
          val rows = query(spark, base).collect()
          val adopted = hook.events.collect { case e: StageAdopted => e.digest }
          checkResult(rows)
          require(adopted.isEmpty,
            s"adopt-cold is supposed to be a COLD baseline (empty anchor store) -- something " +
              s"got adopted anyway: $adopted -- is args(2) really empty?")
          println(s"RESUME-POC-E2E ADOPT-COLD tasksRun=${counter.count} result=OK")
        } finally {
          hook.close()
          ResumeHooks.stage = None
          spark.stop()
        }

      // See this file's "Skew scenario" section and SkewGuard.scala's doc comment for what this
      // pair of modes checks and why it's a post-hoc plan check rather than a pre-adoption
      // rejection. skew-capture: real run, hook captures big's shuffle stage BEFORE the join
      // consumes it (which may skew-split-read it, re-sorting the underlying Celeborn file --
      // exactly the scenario rung 7.5's freshness probe must survive later). halt(0), same
      // reasoning as `capture`.
      case "skew-capture" =>
        val store = new FileAnchorStore(new File(args(2)))
        val spark = newSkewSession(masterEndpoint, SKEW_QUERY_ID)
        val hook = new E2EStageHook(spark.sparkContext, store, SKEW_QUERY_ID, HookMode.Capture)
        ResumeHooks.stage = Some(hook)
        try {
          val df = skewedJoin(spark)
          val rows = df.collect()
          val count = rows.head.getLong(0)
          require(count == SKEW_EXPECTED_COUNT, s"skew-capture result mismatch: got=$count expected=$SKEW_EXPECTED_COUNT")
          val captured = hook.events.collect { case e: StageCaptured => e.digest }
          val skewedDuringCapture = SkewGuard.hasSkewPartitionSpecs(df.queryExecution.executedPlan)
          println(s"RESUME-POC-E2E SKEW-CAPTURE result=OK count=$count " +
            s"stagesCaptured=${captured.size} digests=${captured.mkString(",")} " +
            s"skewedDuringCapture=$skewedDuringCapture")
        } finally {
          hook.close()
          ResumeHooks.stage = None
        }
        Runtime.getRuntime.halt(0)

      // Second JVM, SAME skewed query. Proves, in one run: (1) rung 7.5's freshness probe does
      // NOT falsely reject an anchor whose underlying file capture's OWN later consumption
      // already re-sorted (a whole-file probe against a sorted file, exercised for real, not
      // reasoned about from source alone -- see design-aqe-and-corrupted-rerun.md S2's "sorted
      // file" open item); (2) the adopted stage's fabricated per-mapper stats
      // (`seedAdopted`'s doc comment) do not corrupt the result when AQE skew-splits it using
      // them, confirming `ShufflePartitionsUtil.createSkewPartitionSpecs`'s full-mapper-range
      // coverage guarantee (verified from source in SkewGuard.scala's doc comment) empirically,
      // not just structurally.
      case "skew-adopt" =>
        val store = new FileAnchorStore(new File(args(2)))
        val spark = newSkewSession(masterEndpoint, SKEW_QUERY_ID)
        val hook = new E2EStageHook(spark.sparkContext, store, SKEW_QUERY_ID, HookMode.Adopt)
        ResumeHooks.stage = Some(hook)
        try {
          val df = skewedJoin(spark)
          val rows = df.collect()
          val adopted = hook.events.collect { case e: StageAdopted => e.digest }
          val rejected = hook.events.collect { case e: StageRejected => e }
          val missed = hook.events.collect { case e: StageMissNoAnchor => e.digest }
          val plan = df.queryExecution.executedPlan
          val skewedThisRun = SkewGuard.hasSkewPartitionSpecs(plan)
          // Printed before the correctness/adoption requires, same reasoning as `adopt`: these
          // diagnostics must survive an assertion failure, not be lost to it.
          println(s"RESUME-POC-E2E SKEW-ADOPT stagesAdopted=${adopted.size} " +
            s"digests=${adopted.mkString(",")} missed=${missed.mkString(",")} " +
            s"rejected=${rejected.mkString(";")} skewedThisRun=$skewedThisRun")
          println(s"RESUME-POC-E2E SKEW-ADOPT plan=\n${plan.toString.take(4000)}")
          require(adopted.nonEmpty,
            "expected big's shuffle stage to be adopted -- if it was CELEBORN_STALE, that is " +
              "exactly the false-positive this scenario exists to catch (rung 7.5 vs. a file " +
              "sorted by this anchor's own capture-time consumption) -- see rejected= above")
          require(skewedThisRun,
            "expected AQE to skew-split the adopted stage in THIS run too -- if it didn't, this " +
              "scenario never actually exercised the fabricated-per-mapper-stats risk, and a " +
              "passing result below would not mean what this test claims it means")
          val count = rows.head.getLong(0)
          require(count == SKEW_EXPECTED_COUNT, s"skew-adopt result mismatch: got=$count expected=$SKEW_EXPECTED_COUNT " +
            "-- this is the failure mode this scenario exists to catch: a skew-split read against " +
            "an adopted stage's fabricated per-mapper stats producing a wrong answer")
          println(s"RESUME-POC-E2E SKEW-ADOPT result=OK count=$count")
        } finally {
          hook.close()
          ResumeHooks.stage = None
          spark.stop()
        }

      // Residual-risk test (design-aqe-and-corrupted-rerun.md S2), the AQE-pipeline sibling of
      // resume-poc's run-kill-before-fetch-test.sh -- and NOT obviously the same test, checked
      // rather than assumed: an adopted ShuffleQueryStageExec's `resultOption` is already frozen
      // (`StageSuccess` synthesized, `stage.cleanupResources()` called) by the time AQE finishes
      // replanning everything downstream of it, all before a single real reduce task exists. Does
      // Spark's fetch-failure recovery still correctly resubmit and recompute that stage's REAL
      // map tasks -- which, unlike the RDD case, were never part of any `submitMapStage` call at
      // all, only ever registered directly on `MapOutputTrackerMaster` -- when the worker holding
      // the adopted data dies before the join ever reads it? `onAdopted` (see `E2EStageHook`'s
      // constructor doc) is the only synchronous pause point AQE's own driver-side loop offers;
      // signalling ready and blocking there is the AQE equivalent of resume-poc's demo pausing
      // between `tryAdopt` and `.collect()`.
      case "adopt-kill-before-fetch" =>
        val store = new FileAnchorStore(new File(args(2)))
        val readySignal = new File(args(4))
        val killSignal = new File(args(5))
        val spark = newSession(base, masterEndpoint, QUERY_ID, autoBroadcastThreshold = 10L * 1024 * 1024)
        val onAdopted = () => {
          readySignal.getParentFile.mkdirs()
          val w = new java.io.PrintWriter(readySignal)
          try { w.write("ready") } finally { w.close() }
          println("RESUME-POC-E2E RUN-KILL signalled ready -- waiting for worker-killed signal")
          val deadline = System.currentTimeMillis() + 30000
          while (!killSignal.exists() && System.currentTimeMillis() < deadline) Thread.sleep(100)
          require(killSignal.exists(), "timed out waiting for worker-killed signal from orchestrator")
          println("RESUME-POC-E2E RUN-KILL worker-killed signal received -- resuming AQE " +
            "replanning/execution now (expect a fetch failure downstream, map-stage resubmit, " +
            "then a correct result)")
        }
        val hook = new E2EStageHook(spark.sparkContext, store, QUERY_ID, HookMode.Adopt, onAdopted)
        ResumeHooks.stage = Some(hook)
        val counter = new TaskCounter
        spark.sparkContext.addSparkListener(counter)
        try {
          val rows = query(spark, base).collect()
          val adopted = hook.events.collect { case e: StageAdopted => e.digest }
          println(s"RESUME-POC-E2E RUN-KILL tasksRun=${counter.count} stagesAdopted=${adopted.size}")
          require(adopted.nonEmpty,
            "expected the fact-side stage to be adopted (worker must be alive at adopt time for " +
              "this test to mean anything) -- see hook.events for the miss/reject reason")
          println("RESUME-POC-E2E RUN-KILL ADOPT-CONFIRMED-ALIVE")
          checkResult(rows)
          println("RESUME-POC-E2E RUN-KILL OK-RECOMPUTED-AFTER-FETCH-FAILURE")
        } finally {
          hook.close()
          ResumeHooks.stage = None
          spark.stop()
        }

      case other => throw new IllegalArgumentException(s"unknown mode $other")
    }
  }
}
