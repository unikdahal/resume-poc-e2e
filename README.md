# resume-poc-e2e

The combined AQE + SQL + Celeborn end-to-end adoption demo that `docs/APPROACH-AND-COVERAGE.md`
named as the largest open gap: `resume-poc` proves Celeborn-backed shuffle bytes survive a driver
restart, but only at the RDD level (no AQE, no per-stage digest). `resume-poc-sql` proves the
`StageResumeHook`/`StageDigest` digest-chaining mechanism (design note S1) in isolation, but only
against plain Spark shuffle (no Celeborn, nothing survives a real cross-process restart). Neither
alone shows a second JVM's shuffle stage actually getting SKIPPED, with a real Celeborn cluster
underneath it, because a real `StageDigest` matched. This project is that combined harness.

## What this proves

Run `./run-e2e-demo.sh` (needs `../spark` built as `3.5.10-SNAPSHOT` and `../celeborn` built as
`1.0.0-SNAPSHOT` -- see `pom.xml`'s header comment for the exact build commands, same
prerequisites `resume-poc` and `resume-poc-sql` each already need separately). 9/9 checks pass,
two query shapes:

1. **Correctness.** The adopted run's result matches an independently (non-Spark) computed
   expected value.
2. **All 4 reduce partitions genuinely have data**, in both captured stages -- not "2 empty, 2
   real, and we assumed the empty ones were legitimate." The first version of this demo's `dim`
   table had only 5 distinct join keys, and only 2 of 4 hash partitions ended up with real file
   groups. That was *plausible* (5 keys can leave buckets empty) but *indistinguishable* from
   silently losing 2 partitions of real data to the race described in point 4 below. Widened to
   20 distinct keys so this is a safe assumption, not a coin flip -- caught by an advisor review
   before it shipped as a passing check that wasn't actually checking what it claimed to.
3. **A genuine task-count drop, measured against the correct baseline.** `adopt`'s task count is
   compared against `adopt-cold` (same broadcast-join query, empty anchor store), NOT against
   `capture` (which forces a structurally different join strategy -- SortMergeJoin, with an extra
   shuffle stage -- and would show a task-count drop even with zero adoptions). This confound was
   caught by the same advisor review, in the same pass, before being shipped as a real proof.
4. **The mechanism, not a coincidence.** The `adopt` run's `E2EStageHook.events` contains a real
   `StageAdopted` whose digest also appears as a `StageCaptured` event from the `capture` run.
5. **It survives the exact pressure design note S1 exists for.** `capture` forces a shuffle join
   (`SortMergeJoin`); `adopt` forces a broadcast join (`BroadcastHashJoin`) against the *same*
   fact/dim data. The fact-side aggregate's `StageDigest` is identical either way (it's upstream
   of the join, which can't reach back and change it); the join's own shuffle stage(s), which
   *are* affected, are simply never looked up on the `adopt` side, because a broadcast join has no
   join-side shuffle stage to look up.
6. **rung 8's real danger, both halves, empirically -- not just reasoned about (`skew-capture`/
   `skew-adopt` modes, see `SkewGuard.scala`'s doc comment for the full account).** A real
   AQE-skewed join (`SkewDemo`'s recipe, ported from `resume-poc`): one key holds 30000 of 30008
   rows. `skew-capture` captures the skewed side's shuffle stage BEFORE its own join consumption
   skew-reads it (which re-sorts the underlying Celeborn file). `skew-adopt`, second JVM, same
   query, checks: (a) rung 7.5's freshness probe does NOT false-reject an anchor whose file was
   sorted by capture's own later, unrelated consumption; (b) AQE genuinely skew-splits the
   ADOPTED stage using `MapOutputTrackerAccess.seedAdopted`'s fabricated per-mapper stats; (c) the
   result is still correct. (b) failed the first time this was run -- see the bug below -- and (c)
   would have been meaningless if (b) hadn't first been forced to genuinely happen.

## Three real bugs found building this, not anticipated by design

Both are documented in depth at their call sites (`CelebornAdoption.waitStageEnd`,
`E2EStageHook.adoptAnchor`, `ShuffleAnchor.dataSize`/`rowCount`, and the `StageResumeHook` trait
doc comment in `../spark`) -- summarized here:

1. **Celeborn's own map-side stage-end commit is asynchronous relative to the map stage's own
   `MapOutputStatistics` future resolving.** `resume-poc`'s RDD-level capture never hits this --
   it only ever captures after `.collect()`, which waits for the reduce stage too, giving
   Celeborn's async `StageEnd` message the whole reduce stage's wall-clock time to land. A hook
   capturing at the earliest possible point -- `AdaptiveSparkPlanExec`'s own per-stage
   materialization callback, before any reduce task exists -- has no such luxury, and hit exactly
   this: `exportFileGroups` returning genuinely empty for a map stage that, from Spark's own point
   of view, had already succeeded. Fixed with Celeborn's own existing, public
   `CommitHandler.waitStageEnd` (a bounded poll), not a new patch.
2. **`MapOutputStatistics` is not the only stats channel AQE reads, and adopting a stage does not
   populate the other one.** `ShuffleExchangeExec.runtimeStatistics` -- what
   `AQEPropagateEmptyRelation` and similar row-count-driven AQE rules actually consult -- comes
   from plain `SQLMetric` accumulators (`dataSize`, `shuffleRecordsWritten`) that only a REAL map
   task running ever increments. The first end-to-end pass of this demo adopted correctly, ran
   fewer tasks, and still produced a WRONG, silently-empty result: AQE read zero rows off those
   never-incremented accumulators and rewrote the entire downstream plan to
   `LocalTableScan <empty>`, discarding genuinely-adopted, genuinely-correct data. Fixed by
   capturing `dataSize`/`shuffleRecordsWritten` at materialization time (real values, from a real
   run) into `ShuffleAnchor`, and setting them on the adopting stage's own `ShuffleExchangeExec`
   before returning from `tryAdoptShuffleStage`. This was serious enough to also fix the
   `StageResumeHook` trait's own doc comment in `../spark` -- it previously implied
   `MapOutputStatistics` alone was sufficient, which is what led to writing this bug in the first
   place.
3. **`seedAdopted`'s fabricated per-mapper split (real total on mapper 0, 1-byte placeholders
   elsewhere) silently defeated AQE's skew-splitting for every adopted shuffle.** Not a
   correctness bug (see `SkewGuard.scala`'s doc comment for why full-mapper-range coverage is
   structurally guaranteed regardless) -- a silent LOSS of the optimization, and worse, a silent
   loss of test coverage: `skew-adopt`'s first run showed `skewedDuringCapture=true` but
   `skewedThisRun=false` on the identical query's adopted rerun, because `OptimizeSkewedJoin`'s
   own small-partition-merging logic collapsed the near-zero placeholder mappers back into the
   one real chunk and concluded there was nothing worth splitting. The scenario this test exists
   to exercise was never actually entered. Fixed in `MapOutputTrackerAccess.seedAdopted` by
   spreading each partition's real total EVENLY across all mapper slots instead of dumping it on
   one -- strictly better on two axes, not a trade-off: `getStatistics`'s per-partition sum
   becomes exact (no more placeholder-noise overcounting), and the per-mapper shape now resembles
   a real distribution closely enough for `OptimizeSkewedJoin` to find a genuine split. With the
   fix, `skew-adopt` reliably reproduces `skewedThisRun=true` and a correct result.

## What this still does not prove

- **Rung 8's skew check is a POST-HOC detector, not a pre-adoption gate, and there is no sound way
  to make it one.** `hasSkewPartitionSpecs` runs on the run's final converged plan, after real
  execution -- it can tell you a skew split against an adopted stage happened and was correct, but
  it cannot refuse an adoption in advance on the suspicion that a later consumer MIGHT skew-split
  it, because the information needed to know that doesn't exist at `tryAdoptShuffleStage` time
  (see `SkewGuard.scala`'s doc comment). This project treats that as acceptable given the
  structural + empirical safety proof, not as a gap it papers over -- but it means there is still
  no coordinator anywhere in this whole effort that REJECTS an adoption for skew reasons; there is
  only a fact-check after the fact.
- **Only `ShuffleExchangeExec` is handled** (`stage.shuffle match { case s: ShuffleExchangeExec =>
  ...}`); a `ReusedExchangeExec`-wrapped stage falls through to "not adopted, recompute" via
  `shuffleExchangeOf`'s `None` case, untested here.
- **`dataSize`/`rowCount` (the `AQEPropagateEmptyRelation` fix) are whole-stage totals, not
  per-partition.** `bytesByPartitionId` (the PER-PARTITION granularity `OptimizeSkewedJoin`'s
  skew-detection actually reads, now proven to work end-to-end above) and the per-MAPPER split
  within it (`seedAdopted`, now spread evenly, also proven to work end-to-end above) are both
  exercised for real. What's still untested: `CoalesceShufflePartitions`, a different AQE rule
  that also reads per-partition sizes, for a different purpose (merging small partitions rather
  than splitting large ones) -- no test here specifically targets it.
- **`waitStageEnd` is bounded** by `celeborn.client.push.stageEnd.timeout` and this project
  captures anyway on timeout, producing a deliberately-empty anchor that will simply fail to match
  or fail rung 7.5 later (safe under A-1, not silently wrong) -- but a capture that always times
  out would silently never contribute a usable anchor, and nothing here alerts on that pattern
  specifically.
- **Real crash/restart across separate host processes is not exercised here** the way
  `resume-poc`'s two-JVM `run-demo.sh` does (`capture`/`adopt` are still two separate `java`
  invocations here too, same as every other demo in this project -- but this project has not
  added its own kill-mid-flight test the way `resume-poc/run-kill-before-fetch-test.sh` did; that
  test's finding -- Spark's own fetch-failure recovery is the real safety net -- is inherited by
  construction (same underlying mechanism) but not re-verified against this specific SQL/AQE
  pipeline.
- **Two query shapes now (a groupBy/join with differing downstream strategy, and a skewed join),
  one hook implementation, one small local cluster.** Not a stress test, not a real multi-stage
  query with many simultaneously-adoptable stages, not a load test of `BATCH_OPEN_STREAM` fan-out
  at realistic partition counts.
