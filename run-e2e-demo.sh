#!/usr/bin/env bash
# The combined AQE + SQL + Celeborn end-to-end adoption demo (docs/APPROACH-AND-COVERAGE.md's
# "no combined AQE+SQL+Celeborn end-to-end adoption demo" gap). See README.md for what this
# closes and this file's header comment in each generated log for exactly what's asserted.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CELEBORN_DIR="$(cd "$HERE/../celeborn" && pwd)"

export JAVA_HOME="${JAVA_HOME:-$HOME/.sdkman/candidates/java/17.0.11-tem}"
export PATH="$JAVA_HOME/bin:$PATH"

ADD_OPENS=(
  --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
  --add-opens=java.base/java.io=ALL-UNNAMED
  --add-opens=java.base/java.net=ALL-UNNAMED
  --add-opens=java.base/java.nio=ALL-UNNAMED
  --add-opens=java.base/java.util=ALL-UNNAMED
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
  --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED
  --add-opens=java.base/sun.security.action=ALL-UNNAMED
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED
)

WORK="$(mktemp -d /tmp/resume-poc-e2e.XXXXXX)"
echo "workdir: $WORK"
CONF_DIR="$WORK/conf"; mkdir -p "$CONF_DIR"
ANCHOR_DIR="$WORK/anchors"; mkdir -p "$ANCHOR_DIR"
TABLES_DIR="$WORK/tables"
LOG_DIR="$WORK/logs"; mkdir -p "$LOG_DIR"
mkdir -p "$WORK/worker-data"
sed "s#__WORKER_DATA_DIR__#$WORK/worker-data#" "$HERE/conf/celeborn-defaults.conf" > "$CONF_DIR/celeborn-defaults.conf"
cp "$HERE/conf/log4j2.xml" "$CONF_DIR/log4j2.xml"
LOG4J_OPT="-Dlog4j2.configurationFile=file:$CONF_DIR/log4j2.xml"

MASTER_ENDPOINT="localhost:9097"

echo "== building classpaths =="
MASTER_CP="$WORK/master-cp.txt"
WORKER_CP="$WORK/worker-cp.txt"
( cd "$CELEBORN_DIR" && mvn -q -Pspark-3.5 -pl master dependency:build-classpath -Dmdep.outputFile="$MASTER_CP" )
( cd "$CELEBORN_DIR" && mvn -q -Pspark-3.5 -pl worker dependency:build-classpath -Dmdep.outputFile="$WORKER_CP" )
( cd "$HERE" && mvn -q -o package -DskipTests )

MASTER_CLASSPATH="$CELEBORN_DIR/master/target/classes:$(cat "$MASTER_CP")"
WORKER_CLASSPATH="$CELEBORN_DIR/worker/target/classes:$(cat "$WORKER_CP")"
DEMO_CLASSPATH="$HERE/target/classes:$(cat "$HERE/target/classpath.txt")"

PIDS=()
cleanup() {
  echo "== tearing down =="
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

port_open() {
  (exec 3<>"/dev/tcp/localhost/$1") 2>/dev/null && { exec 3>&-; return 0; }
  return 1
}

if port_open 9097; then
  echo "FAILED: port 9097 already in use -- a leaked master from a previous run? kill it first" >&2
  exit 1
fi

echo "== starting master =="
CELEBORN_CONF_DIR="$CONF_DIR" java "${ADD_OPENS[@]}" "$LOG4J_OPT" -cp "$MASTER_CLASSPATH" \
  org.apache.celeborn.service.deploy.master.Master > "$LOG_DIR/master.log" 2>&1 &
MASTER_PID=$!
PIDS+=("$MASTER_PID")

wait_for_port() {
  local port="$1" name="$2" tries=60
  while (( tries > 0 )); do
    port_open "$port" && return 0
    sleep 1; tries=$((tries - 1))
  done
  echo "FAILED: $name never opened port $port -- see $LOG_DIR" >&2
  tail -n 80 "$LOG_DIR"/*.log >&2
  exit 1
}
wait_for_port 9097 master
echo "master rpc port up"

echo "== starting worker =="
CELEBORN_CONF_DIR="$CONF_DIR" java "${ADD_OPENS[@]}" "$LOG4J_OPT" -cp "$WORKER_CLASSPATH" \
  -Dceleborn.worker.storage.dirs="$WORK/worker-data" \
  org.apache.celeborn.service.deploy.worker.Worker > "$LOG_DIR/worker.log" 2>&1 &
WORKER_PID=$!
PIDS+=("$WORKER_PID")

tries=60
until grep -q "Registered worker" "$LOG_DIR/master.log" 2>/dev/null; do
  (( tries-- )) || { echo "FAILED: worker never registered -- see $LOG_DIR" >&2; tail -n 80 "$LOG_DIR"/*.log >&2; exit 1; }
  sleep 1
done
echo "worker registered"

run_demo() {
  java "${ADD_OPENS[@]}" "$LOG4J_OPT" -cp "$DEMO_CLASSPATH" org.apache.spark.resume.e2e.Demo "$@"
}

echo
echo "== setup: write fact/dim parquet tables =="
run_demo setup "$TABLES_DIR" 2>&1 | tee "$LOG_DIR/setup.log" | grep --line-buffered "RESUME-POC-E2E"
grep -q "SETUP-DONE" "$LOG_DIR/setup.log"

echo
echo "== CAPTURE: real AQE query, shuffle join forced (autoBroadcastJoinThreshold=1), real Celeborn =="
run_demo capture "$TABLES_DIR" "$ANCHOR_DIR" "$MASTER_ENDPOINT" 2>&1 | tee "$LOG_DIR/capture.log" | grep --line-buffered "RESUME-POC-E2E"
grep -q "RESUME-POC-E2E CAPTURE .*result=OK" "$LOG_DIR/capture.log"

echo
echo "== ADOPT: second JVM, SAME query but broadcast join forced (autoBroadcastJoinThreshold=10MB) =="
run_demo adopt "$TABLES_DIR" "$ANCHOR_DIR" "$MASTER_ENDPOINT" 2>&1 | tee "$LOG_DIR/adopt.log" | grep --line-buffered "RESUME-POC-E2E"
grep -q "RESUME-POC-E2E ADOPT result=OK" "$LOG_DIR/adopt.log"

echo
echo "== ADOPT-COLD: control -- SAME broadcast-join query, EMPTY anchor store (nothing can adopt) =="
# The baseline "adopt runs fewer tasks" must be measured against: capture forces a structurally
# DIFFERENT join strategy (SortMergeJoin, an extra dim-side shuffle stage), so comparing adopt's
# task count directly against capture's is confounded by that shape difference alone, regardless
# of whether adoption fired. This runs the exact same broadcast-join plan adopt runs, but points
# at a directory capture never wrote to, so zero stages can possibly be adopted.
EMPTY_ANCHOR_DIR="$WORK/anchors-empty"; mkdir -p "$EMPTY_ANCHOR_DIR"
run_demo adopt-cold "$TABLES_DIR" "$EMPTY_ANCHOR_DIR" "$MASTER_ENDPOINT" 2>&1 | tee "$LOG_DIR/adopt-cold.log" | grep --line-buffered "RESUME-POC-E2E"
grep -q "RESUME-POC-E2E ADOPT-COLD .*result=OK" "$LOG_DIR/adopt-cold.log"

CAPTURE_TASKS=$(grep -oP 'RESUME-POC-E2E CAPTURE tasksRun=\K[0-9]+' "$LOG_DIR/capture.log")
ADOPT_TASKS=$(grep -oP 'RESUME-POC-E2E ADOPT tasksRun=\K[0-9]+' "$LOG_DIR/adopt.log")
COLD_TASKS=$(grep -oP 'RESUME-POC-E2E ADOPT-COLD tasksRun=\K[0-9]+' "$LOG_DIR/adopt-cold.log")
CAPTURE_DIGESTS=$(grep -oP 'RESUME-POC-E2E CAPTURE .*digests=\K\S*' "$LOG_DIR/capture.log")
ADOPT_DIGESTS=$(grep -oP 'RESUME-POC-E2E ADOPT .*digests=\K\S*' "$LOG_DIR/adopt.log")
CAPTURED_PARTITION_COUNTS=$(grep -oP 'RESUME-POC-E2E-STAGE CAPTURED .*fileGroups\.size=\K[0-9]+' "$LOG_DIR/capture.log")

echo
echo "captureTasks=$CAPTURE_TASKS adoptTasks=$ADOPT_TASKS coldBroadcastTasks=$COLD_TASKS"
echo "captureDigests=$CAPTURE_DIGESTS"
echo "adoptDigests=$ADOPT_DIGESTS"
echo "capturedPartitionCounts=$CAPTURED_PARTITION_COUNTS"

pass=0; fail=0
check() {
  if [ "$2" = "true" ]; then echo "PASS: $1"; pass=$((pass+1)); else echo "FAIL: $1"; fail=$((fail+1)); fi
}

check "adopt run produced the correct result (checked against an independently computed expected value)" \
  "$(grep -q 'RESUME-POC-E2E ADOPT result=OK' "$LOG_DIR/adopt.log" && echo true || echo false)"

ALL_FULL=true
if [ -z "$CAPTURED_PARTITION_COUNTS" ]; then ALL_FULL=false; fi
for n in $CAPTURED_PARTITION_COUNTS; do
  if [ "$n" -ne 4 ]; then ALL_FULL=false; fi
done
check "every captured shuffle stage has data in ALL 4 reduce partitions (rules out a silent partial-commit capture, not just 'plausibly empty buckets')" \
  "$ALL_FULL"

check "adopt run ran STRICTLY FEWER tasks than the COLD baseline of the SAME broadcast-join query (a shuffle stage was genuinely skipped, not just a cheaper join strategy)" \
  "$([ "$ADOPT_TASKS" -lt "$COLD_TASKS" ] && echo true || echo false)"

SHARED=false
IFS=',' read -ra AD <<< "$ADOPT_DIGESTS"
for d in "${AD[@]}"; do
  if [[ ",$CAPTURE_DIGESTS," == *",$d,"* ]] && [ -n "$d" ]; then SHARED=true; fi
done
check "at least one ADOPTED stage digest in the adopt run was CAPTURED in the capture run (mechanism, not coincidence)" \
  "$SHARED"

check "adopt run's plan actually took a DIFFERENT downstream join strategy than capture (proves this isn't a trivial identical-plan replay)" \
  "$(grep -q 'BroadcastHashJoin' "$LOG_DIR/adopt.log" && echo true || echo false)"

echo
echo "== SKEW-CAPTURE: real skewed join, big's shuffle stage captured before its own consumer skew-reads it =="
run_demo skew-capture "$TABLES_DIR" "$ANCHOR_DIR" "$MASTER_ENDPOINT" 2>&1 | tee "$LOG_DIR/skew-capture.log" | grep --line-buffered "RESUME-POC-E2E"
grep -q "RESUME-POC-E2E SKEW-CAPTURE .*result=OK" "$LOG_DIR/skew-capture.log"

echo
echo "== SKEW-ADOPT: second JVM, SAME skewed query -- rung 7.5 vs. a since-sorted file, skew-split vs. fabricated per-mapper stats =="
run_demo skew-adopt "$TABLES_DIR" "$ANCHOR_DIR" "$MASTER_ENDPOINT" 2>&1 | tee "$LOG_DIR/skew-adopt.log" | grep --line-buffered "RESUME-POC-E2E"
grep -q "RESUME-POC-E2E SKEW-ADOPT result=OK" "$LOG_DIR/skew-adopt.log"

check "skew-capture's own join consumption actually skew-split the captured stage (the scenario this fixture exists to create actually occurred)" \
  "$(grep -oP 'skewedDuringCapture=\K\S+' "$LOG_DIR/skew-capture.log" | grep -q true && echo true || echo false)"

check "skew-adopt: the shuffle stage was adopted despite its file having been sorted by capture's own consumption (rung 7.5 did NOT false-reject)" \
  "$(grep -q 'RESUME-POC-E2E SKEW-ADOPT stagesAdopted=0' "$LOG_DIR/skew-adopt.log" && echo false || echo true)"

check "skew-adopt: AQE skew-split the ADOPTED stage in this run too (the fabricated-per-mapper-stats risk was actually exercised, not sidestepped)" \
  "$(grep -oP 'skewedThisRun=\K\S+' "$LOG_DIR/skew-adopt.log" | grep -q true && echo true || echo false)"

check "skew-adopt produced the CORRECT count despite a skew-split read against fabricated per-mapper stats (the full-mapper-range-coverage guarantee holds in practice, not just by source reading)" \
  "$(grep -q 'RESUME-POC-E2E SKEW-ADOPT result=OK' "$LOG_DIR/skew-adopt.log" && echo true || echo false)"

echo
echo "== summary: $pass passed, $fail failed =="
echo "logs kept at: $LOG_DIR"
[ "$fail" -eq 0 ]
