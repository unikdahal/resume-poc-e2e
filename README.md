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
prerequisites `resume-poc` and `resume-poc-sql` each already need separately). 5/5 checks pass:

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

## Two real bugs found building this, not anticipated by design

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

## What this still does not prove

- **Only one query shape, one hook implementation, one small local cluster.** Not a stress test,
  not a skew scenario, not a real multi-stage query with many adoptable stages at once.
- **Only `ShuffleExchangeExec` is handled** (`stage.shuffle match { case s: ShuffleExchangeExec =>
  ...}`); a `ReusedExchangeExec`-wrapped stage falls through to "not adopted, recompute" via
  `shuffleExchangeOf`'s `None` case, untested here.
- **`dataSize`/`rowCount` are whole-stage totals, not per-partition.** Sufficient for
  `AQEPropagateEmptyRelation`'s row-count check (what this demo actually hit and fixed); untested
  against `CoalesceShufflePartitions`/`OptimizeSkewedJoin`, which read the PER-PARTITION
  `bytesByPartitionId` this project does supply (via `CelebornAdoption.partitionByteSizes`, with
  the same "+`numMaps`-1 placeholder noise" approximation `resume-poc` already documents and
  accepts) -- whether that's precise enough for skew-aware replanning specifically is open.
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
- **Rung 8's skew-spec half (`SkewGuard.hasSkewPartitionSpecs`) is still not wired in anywhere,**
  including here -- this demo's query has no skew.
