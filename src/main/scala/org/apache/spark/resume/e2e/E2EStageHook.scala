package org.apache.spark.resume.e2e

import scala.collection.mutable

import org.apache.spark.{MapOutputStatistics, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.StageResumeHook
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.metric.SQLShuffleWriteMetricsReporter

sealed trait HookMode
object HookMode {
  case object Capture extends HookMode
  case object Adopt extends HookMode
}

sealed trait StageOutcome { def shuffleId: Int; def digest: String }
case class StageCaptured(shuffleId: Int, digest: String) extends StageOutcome
case class StageAdopted(shuffleId: Int, digest: String) extends StageOutcome
case class StageMissNoAnchor(shuffleId: Int, digest: String) extends StageOutcome
case class StageRejected(shuffleId: Int, digest: String, reason: String) extends StageOutcome

/** This is the piece `docs/APPROACH-AND-COVERAGE.md` names as the biggest gap: "the
  * `StageResumeHook`/`StageDigest` machinery is built and unit-proven in isolation but has never
  * driven a real shuffle stage's skip against Celeborn-backed data." Here it actually does --
  * `tryAdoptShuffleStage`/`onShuffleStageMaterialized` are `ResumeHooks.stage`'s real contract
  * (`spark/.../ResumeHooks.scala`), called from inside `AdaptiveSparkPlanExec`'s own
  * stage-materialization loop for every `ShuffleQueryStageExec`, and everything on the Celeborn
  * side is `CelebornAdoption` ported unchanged from `resume-poc` (rungs 7 and 7.5 both apply
  * here, not a weaker substitute).
  *
  * One hook instance is per-JVM-run, not per-stage: `knownDigests` (see `StageDigest`'s own doc
  * comment) must be shared across every stage of the one query this JVM runs, so a stage
  * referenced twice (self-join, reused exchange) is hashed once. `mode` picks which half of the
  * `StageResumeHook` contract is live -- a single hook implementing both, gated by a run mode,
  * rather than two separate classes, because the CAPTURE JVM and the ADOPT JVM in this demo are
  * still the same code path exercising the same digest/Celeborn machinery, just at opposite ends.
  *
  * `tryAdoptShuffleStage` is the ENTIRE acceptance ladder for this hook, assembled inline rather
  * than reusing `ResumeCoordinator.tryAdopt` (that class lives in `resume-poc`, keyed by a fixed
  * demo string, not a `StageDigest`, and returns `AdoptOutcome`, not a `MapOutputStatistics` --
  * porting its SHAPE here while reusing its actual Celeborn calls via `CelebornAdoption` was more
  * honest than forcing an awkward shared abstraction across two different call sites). Same rung
  * order as `ResumeCoordinator`: `confirmAlive` (rung 7) before `confirmFresh` (rung 7.5) before
  * `adopt` -- cheapest, most-likely-to-reject check first, exactly why `ResumeCoordinator` orders
  * them that way too. */
class E2EStageHook(
    sc: SparkContext,
    store: AnchorStore,
    queryId: String,
    mode: HookMode) extends StageResumeHook with Logging {

  private val SCHEMA_VERSION = "resume-poc-e2e-1"
  private val knownDigests = mutable.Map[Int, String]()

  @volatile private var eventLog: List[StageOutcome] = Nil
  private def record(e: StageOutcome): Unit = synchronized { eventLog = e :: eventLog }
  def events: List[StageOutcome] = eventLog.reverse

  private lazy val lcm = CelebornAdoption.lifecycleManager(sc)
  // Built lazily, only if tryAdoptShuffleStage ever actually needs it (mode == Adopt AND at
  // least one stage reaches confirmFresh) -- a capture-mode run never touches this, so it never
  // pays for a TransportClientFactory it has no use for.
  private var freshnessClientOpt: Option[org.apache.celeborn.common.network.client.TransportClientFactory] = None
  private def freshnessClient(): org.apache.celeborn.common.network.client.TransportClientFactory =
    freshnessClientOpt.getOrElse {
      val c = CelebornAdoption.buildDataClientFactory(lcm.conf)
      freshnessClientOpt = Some(c)
      c
    }

  /** Must be called once the query is done (whether or not any stage actually reached
    * `confirmFresh`) -- symmetrical with `CelebornAdoption.buildDataClientFactory`'s own
    * `Closeable` contract. */
  def close(): Unit = freshnessClientOpt.foreach(_.close())

  private def shuffleExchangeOf(stage: ShuffleQueryStageExec): Option[ShuffleExchangeExec] =
    stage.shuffle match {
      case s: ShuffleExchangeExec => Some(s)
      case other =>
        logWarning(s"resume-e2e: stage ${stage.id}'s exchange is ${other.getClass.getName}, not " +
          "a plain ShuffleExchangeExec (e.g. a reused exchange) -- this demo's hook does not " +
          "handle that shape, skipping")
        None
    }

  // logInfo/logWarning go through SLF4J, which this project's classpath has no binding for
  // (Celeborn's shaded client jar deliberately does not ship one, to avoid clashing with
  // whatever the embedding application uses) -- so they are silently swallowed rather than
  // lost-looking-like-a-bug. println for the events this hook's own correctness depends on
  // being visible is deliberate, not leftover debugging -- matches every other demo's
  // "RESUME-POC..." marker convention in this project (grep'd by every run script's assertions).

  override def tryAdoptShuffleStage(stage: ShuffleQueryStageExec): Option[MapOutputStatistics] = {
    if (mode != HookMode.Adopt) return None
    val digest = StageDigest.stageDigest(stage, knownDigests)
    shuffleExchangeOf(stage).flatMap { shuffleExec =>
      val newShuffleId = shuffleExec.shuffleDependency.shuffleId
      store.loadAnchors(queryId).find(_.key == digest) match {
        case None =>
          record(StageMissNoAnchor(newShuffleId, digest))
          println(s"RESUME-POC-E2E-STAGE stage=${stage.id} digest=$digest MISS (no anchor) -- recomputing")
          None
        case Some(anchor) =>
          println(s"RESUME-POC-E2E-STAGE stage=${stage.id} digest=$digest MATCHED anchor " +
            s"celebornShuffleId=${anchor.celebornShuffleId} numMaps=${anchor.numMaps} " +
            s"numReduces=${anchor.numReduces} newShuffleId=$newShuffleId " +
            s"thisStageNumMappers=${shuffleExec.numMappers} thisStageNumPartitions=${shuffleExec.numPartitions}")
          adoptAnchor(shuffleExec, newShuffleId, digest, anchor)
      }
    }
  }

  private def adoptAnchor(
      shuffleExec: ShuffleExchangeExec,
      newShuffleId: Int,
      digest: String,
      anchor: ShuffleAnchor): Option[MapOutputStatistics] = {
    def reject(reason: String): None.type = {
      println(s"RESUME-POC-E2E-STAGE shuffleId=$newShuffleId digest=$digest REJECTED: $reason -- recomputing")
      record(StageRejected(newShuffleId, digest, reason))
      None
    }
    if (anchor.schemaVersion != SCHEMA_VERSION) {
      return reject(s"SCHEMA_MISMATCH: anchor=${anchor.schemaVersion} runtime=$SCHEMA_VERSION")
    }
    if (!CelebornAdoption.confirmAlive(lcm, anchor)) {
      return reject(s"CELEBORN_EXPIRED: celebornShuffleId=${anchor.celebornShuffleId} not reachable")
    }
    if (!CelebornAdoption.confirmFresh(lcm, anchor, freshnessClient())) {
      return reject(s"CELEBORN_STALE: celebornShuffleId=${anchor.celebornShuffleId} reachable but stale")
    }
    if (!CelebornAdoption.adopt(lcm, newShuffleId, anchor)) {
      return reject(s"appShuffleId=$newShuffleId already registered in this driver")
    }
    val tracker = MapOutputTrackerAccess.masterTracker(sc)
    val partitionSizes = CelebornAdoption.partitionByteSizes(anchor.fileGroups)
    MapOutputTrackerAccess.seedAdopted(tracker, newShuffleId, anchor.numMaps, anchor.numReduces, partitionSizes)
    val missing = MapOutputTrackerAccess.missingPartitionCount(tracker, newShuffleId)
    require(missing == 0, s"resume-e2e: seeded shuffle $newShuffleId still reports $missing " +
      "missing partitions -- LLD S6.1's core tripwire, kept loud here too")
    // Read the REAL MapOutputStatistics back off the tracker (same object shape/values a
    // genuine materialize would have produced) rather than hand-constructing one -- see class
    // doc comment: `stats` must be genuine, this is how genuine is guaranteed rather than
    // asserted.
    val stats = tracker.getStatistics(shuffleExec.shuffleDependency)
    // See ShuffleAnchor.dataSize/rowCount's doc comment: MapOutputStatistics alone is not
    // sufficient. ShuffleExchangeExec.runtimeStatistics (what AQE's own
    // AQEPropagateEmptyRelation/EliminateJoinToEmptyRelation/etc. actually read) comes from
    // plain SQLMetric accumulators that only ever get incremented by a real map task running --
    // an adopted stage never runs one, so without this they sit at zero and AQE concludes this
    // stage produced no rows, silently discarding genuinely-adopted, genuinely-correct data.
    // Must happen before returning Some(stats): AdaptiveSparkPlanExec reads runtimeStatistics
    // off the LogicalQueryStage wrapping this ShuffleQueryStageExec on the very next replanning
    // pass, which can happen as soon as this call returns.
    shuffleExec.metrics("dataSize").set(anchor.dataSize)
    shuffleExec.metrics(SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN).set(anchor.rowCount)
    println(s"RESUME-POC-E2E-STAGE ADOPTED shuffleId=$newShuffleId digest=$digest <- " +
      s"celebornShuffleId=${anchor.celebornShuffleId} bytesByPartitionId=${stats.bytesByPartitionId.mkString(",")} " +
      s"dataSize=${anchor.dataSize} rowCount=${anchor.rowCount}")
    record(StageAdopted(newShuffleId, digest))
    Some(stats)
  }

  override def onShuffleStageMaterialized(stage: ShuffleQueryStageExec, stats: MapOutputStatistics): Unit = {
    if (mode != HookMode.Capture) return
    val digest = StageDigest.stageDigest(stage, knownDigests)
    shuffleExchangeOf(stage).foreach { shuffleExec =>
      val appShuffleId = stats.shuffleId
      CelebornAdoption.resolveCelebornShuffleId(lcm, appShuffleId) match {
        case None =>
          logWarning(s"resume-e2e: no live celebornShuffleId for appShuffleId=$appShuffleId " +
            s"(stage ${stage.id}, digest=$digest) -- cannot capture this stage")
        case Some((identifier, celebornShuffleId)) =>
          // Must happen before serializeFileGroups -- see CelebornAdoption.waitStageEnd's doc
          // comment: capturing at stage-materialization granularity (this call site) races
          // Celeborn's own async map-side stage-end commit, unlike resume-poc's post-.collect()
          // capture, which never hits this race.
          val landed = CelebornAdoption.waitStageEnd(lcm, celebornShuffleId)
          if (!landed) {
            logWarning(s"resume-e2e: stage ${stage.id} digest=$digest -- Celeborn stage-end " +
              "commit never landed, capturing anyway (will likely be an empty/short anchor)")
          }
          val fileGroups = CelebornAdoption.serializeFileGroups(lcm, celebornShuffleId)
          val mapperAttempts = CelebornAdoption.exportMapperAttempts(lcm, celebornShuffleId)
          // See ShuffleAnchor.dataSize/rowCount's doc comment -- these are real (a genuine map
          // stage just ran, these SQLMetric accumulators are exactly what it updated), read the
          // same way ShuffleExchangeExec.runtimeStatistics itself reads them.
          val dataSize = shuffleExec.metrics("dataSize").value
          val rowCount = shuffleExec.metrics(SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN).value
          val gen = store.acquireGeneration(queryId)
          val anchor = ShuffleAnchor(
            SCHEMA_VERSION, queryId, gen, digest, appShuffleId, identifier, celebornShuffleId,
            shuffleExec.numMappers, shuffleExec.numPartitions, fileGroups, mapperAttempts,
            System.currentTimeMillis(), dataSize, rowCount)
          require(store.putAnchor(gen, anchor),
            s"resume-e2e: lost the generation fence for $queryId while capturing digest=$digest")
          println(s"RESUME-POC-E2E-STAGE CAPTURED stage=${stage.id} digest=$digest -> " +
            s"appShuffleId=$appShuffleId celebornShuffleId=$celebornShuffleId " +
            s"fileGroups.size=${fileGroups.size} dataSize=$dataSize rowCount=$rowCount")
          record(StageCaptured(appShuffleId, digest))
      }
    }
  }
}
