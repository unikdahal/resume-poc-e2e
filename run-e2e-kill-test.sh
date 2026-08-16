#!/usr/bin/env bash
# The AQE-pipeline sibling of resume-poc/run-kill-before-fetch-test.sh -- NOT assumed to be the
# same test just because the underlying mechanism (Spark's fetch-failure recovery) is unmodified
# in both. Adopt for real (worker genuinely alive, confirmAlive genuinely passes), kill the
# worker holding the adopted data in the narrow window AFTER adoption and BEFORE any real reduce
# task reads a byte -- but this time the adopted stage is a ShuffleQueryStageExec whose
# resultOption is already frozen and whose downstream AQE replanning has already happened, and
# whose map tasks were never part of any submitMapStage call at all (only ever registered
# directly on MapOutputTrackerMaster via seedAdopted). See Demo.scala's adopt-kill-before-fetch
# mode doc comment for exactly what's being checked and why it isn't obviously the same as the
# RDD case.
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

WORK="$(mktemp -d /tmp/resume-poc-e2e-kill.XXXXXX)"
echo "workdir: $WORK"
CONF_DIR="$WORK/conf"; mkdir -p "$CONF_DIR"
ANCHOR_DIR="$WORK/anchors"; mkdir -p "$ANCHOR_DIR"
TABLES_DIR="$WORK/tables"
LOG_DIR="$WORK/logs"; mkdir -p "$LOG_DIR"
mkdir -p "$WORK/worker-data" "$WORK/worker2-data"
sed "s#__WORKER_DATA_DIR__#$WORK/worker-data#" "$HERE/conf/celeborn-defaults.conf" > "$CONF_DIR/celeborn-defaults.conf"
cp "$HERE/conf/log4j2.xml" "$CONF_DIR/log4j2.xml"
LOG4J_OPT="-Dlog4j2.configurationFile=file:$CONF_DIR/log4j2.xml"
# Second worker kept alive throughout -- same reasoning as resume-poc's kill test: a resubmitted
# map task needs somewhere to push to. A single-worker topology killing its only worker leaves
# zero capacity, which is Celeborn correctly refusing unsafe work, not a gap in the recovery path
# this test actually wants to isolate.

MASTER_ENDPOINT="localhost:9097"
READY_SIGNAL="$WORK/ready.signal"
KILL_SIGNAL="$WORK/killed.signal"

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

echo "== starting worker2 (stays alive for the whole test) =="
CELEBORN_CONF_DIR="$CONF_DIR" java "${ADD_OPENS[@]}" "$LOG4J_OPT" -cp "$WORKER_CLASSPATH" \
  -Dceleborn.worker.storage.dirs="$WORK/worker2-data" \
  -Dceleborn.worker.http.port=9099 \
  org.apache.celeborn.service.deploy.worker.Worker > "$LOG_DIR/worker2.log" 2>&1 &
WORKER2_PID=$!
PIDS+=("$WORKER2_PID")

tries=60
until [ "$(grep -c "Registered worker" "$LOG_DIR/master.log" 2>/dev/null || echo 0)" -ge 2 ]; do
  (( tries-- )) || { echo "FAILED: worker2 never registered -- see $LOG_DIR" >&2; tail -n 80 "$LOG_DIR"/*.log >&2; exit 1; }
  sleep 1
done
echo "worker2 registered"

run_demo() {
  java "${ADD_OPENS[@]}" "$LOG4J_OPT" -cp "$DEMO_CLASSPATH" org.apache.spark.resume.e2e.Demo "$@"
}

echo
echo "== setup: write fact/dim parquet tables =="
run_demo setup "$TABLES_DIR" 2>&1 | tee "$LOG_DIR/setup.log" | grep --line-buffered "RESUME-POC-E2E"
grep -q "SETUP-DONE" "$LOG_DIR/setup.log"

echo
echo "== CAPTURE: real AQE query, real Celeborn, first driver =="
run_demo capture "$TABLES_DIR" "$ANCHOR_DIR" "$MASTER_ENDPOINT" 2>&1 | tee "$LOG_DIR/capture.log" | grep --line-buffered "RESUME-POC-E2E"
grep -q "RESUME-POC-E2E CAPTURE .*result=OK" "$LOG_DIR/capture.log"

echo
echo "== RUN-KILL: second JVM, adopt for real, signal, kill worker BEFORE fetch, then let AQE proceed =="
rm -f "$READY_SIGNAL" "$KILL_SIGNAL"
run_demo adopt-kill-before-fetch "$TABLES_DIR" "$ANCHOR_DIR" "$MASTER_ENDPOINT" "$READY_SIGNAL" "$KILL_SIGNAL" \
  > "$LOG_DIR/run-kill.log" 2>&1 &
DEMO_PID=$!

tries=200
until [ -f "$READY_SIGNAL" ]; do
  (( tries-- )) || { echo "FAILED: demo never signalled ready -- see $LOG_DIR/run-kill.log" >&2; tail -n 150 "$LOG_DIR/run-kill.log" >&2; exit 1; }
  sleep 0.3
done
echo "demo signalled ready -- fact-side stage adopted against the live worker, no fetch has happened yet"

echo "== killing the worker now, in the window before any reduce task reads a byte =="
kill -9 "$WORKER_PID"
wait "$WORKER_PID" 2>/dev/null || true
PIDS=("$MASTER_PID" "$WORKER2_PID")
: > "$KILL_SIGNAL"

echo "== waiting for the demo to finish (AQE must resubmit the adopted stage's real map tasks and recompute) =="
if ! wait "$DEMO_PID"; then
  echo "FAILED: demo process did not exit cleanly -- Spark's fetch-failure recovery did not " \
       "recover the adopted-but-now-unreachable AQE shuffle stage -- see $LOG_DIR/run-kill.log" >&2
  tail -n 200 "$LOG_DIR/run-kill.log" >&2
  exit 1
fi
grep "RESUME-POC-E2E RUN-KILL" "$LOG_DIR/run-kill.log"

pass=0; fail=0
check() {
  if [ "$2" = "true" ]; then echo "PASS: $1"; pass=$((pass+1)); else echo "FAIL: $1"; fail=$((fail+1)); fi
}
check "the fact-side AQE stage was genuinely adopted before the worker died (this test is only meaningful if so)" \
  "$(grep -q "ADOPT-CONFIRMED-ALIVE" "$LOG_DIR/run-kill.log" && echo true || echo false)"
check "job completed with the CORRECT result after the worker died mid-flight, post-adoption, post-AQE-replan (Spark's native fetch-failure recovery holds for an adopted AQE stage too)" \
  "$(grep -q "OK-RECOMPUTED-AFTER-FETCH-FAILURE" "$LOG_DIR/run-kill.log" && echo true || echo false)"

echo
echo "== summary: $pass passed, $fail failed =="
echo "logs kept at: $LOG_DIR"
[ "$fail" -eq 0 ]
