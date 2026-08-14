#!/usr/bin/env bash
#
# demo-partition-ceiling.sh — the repo's headline finding, made runnable.
#
# Claim: every consumption pattern in this repo shares one throughput ceiling — the number of
# partitions on the topic. Adding consumers beyond that count produces idle consumers, not
# throughput.
#
# This script proves it twice over, with two independent kinds of evidence:
#
#   1. ASSIGNMENT (deterministic).  Run N consumers against a 6-partition topic and count how many
#      were assigned zero partitions. At N=9 exactly 3 sit idle, every time. Kafka assigns each
#      partition to at most one consumer in a group, so this cannot come out any other way.
#
#   2. DRAIN RATE (empirical).  Pre-load a fixed backlog, then time how long each configuration
#      takes to consume it. Going 6 -> 9 consumers should not make it meaningfully faster.
#
# Evidence (1) is the real proof. Evidence (2) is corroboration and is noisier — see the caveat
# printed with the results.
#
# Usage:
#   scripts/demo-partition-ceiling.sh                 # 6 then 9 consumers, 6 partitions
#   scripts/demo-partition-ceiling.sh 1 3 6 9 12      # custom consumer counts, shows the knee
#   PARTITIONS=3 scripts/demo-partition-ceiling.sh    # move the ceiling and watch it follow
#
# Prerequisites (the script checks all of them and tells you what is missing):
#   docker compose up -d
#   temporal server start-dev
#   mvn -DskipTests install
#   mvn -f order-email-worker/pom.xml spring-boot:run
#   mvn -f producer/pom.xml spring-boot:run
#
# The script starts and stops consumer-activity itself, once per configuration. Pattern 3 is used
# because its consumer count is a single property, so one process hosts N consumers. See
# "Reproducing with the other patterns" at the bottom of this file.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=lib/common.sh
source "$REPO_ROOT/scripts/lib/common.sh"

# ---------------------------------------------------------------------------- configuration

PARTITIONS="${PARTITIONS:-6}"          # the ceiling under test
# Large enough that each drain runs for tens of seconds. Short runs make the rate figure mostly
# timing noise, which reads as a throughput difference that is not there.
BACKLOG="${BACKLOG:-3000}"
CONSUMER_PORT="${CONSUMER_PORT:-8099}" # deliberately not 8084, so a running Pattern 3 app is untouched
DRAIN_TIMEOUT_S="${DRAIN_TIMEOUT_S:-300}"

CONSUMER_COUNTS=("$@")
if [ ${#CONSUMER_COUNTS[@]} -eq 0 ]; then
  CONSUMER_COUNTS=("$PARTITIONS" "$(( PARTITIONS + 3 ))")
fi

JAR=""
CONSUMER_PID=""
CONSUMER_WORKFLOW_ID=""
RESULTS=()

# ---------------------------------------------------------------------------- helpers
#
# Logging, kafka(), now_ms, total_lag, idle_members, and the workflow-termination helpers all live
# in scripts/lib/common.sh, shared with load-test.sh. terminate_consumer_workflow() in particular is
# correctness-critical and must not drift between the two scripts.

cleanup() {
  terminate_consumer_workflow "$CONSUMER_WORKFLOW_ID"
  if [ -n "$CONSUMER_PID" ] && kill -0 "$CONSUMER_PID" 2>/dev/null; then
    kill "$CONSUMER_PID" 2>/dev/null || true
    wait "$CONSUMER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------- preconditions

log "Checking prerequisites"

check_common_prereqs
JAR=$(find_jar "consumer-activity" "$REPO_ROOT")
info "Using $(basename "$JAR")"

terminate_orphans "kafka-consumer-activity-ceiling-"

# ---------------------------------------------------------------------------- one run

run_configuration() {
  local consumers="$1"
  local run_id="ceiling-$(date +%s)-$consumers"
  local group="ceiling-demo-$consumers-$$"

  log "Run: $consumers consumer(s) against $PARTITIONS partition(s)"

  # Fresh topic each run, so every configuration drains an identical, uncontended backlog and no
  # run inherits another's committed offsets.
  info "Recreating topic with $PARTITIONS partitions"
  recreate_topic "$PARTITIONS"

  # Pre-load the backlog BEFORE the consumers start. This is what makes the consumers the
  # bottleneck: they never wait on the producer, so drain time reflects consumption capacity
  # rather than production rate.
  info "Pre-loading $BACKLOG messages"
  curl -sf -X POST "$PRODUCER_URL/orders/batch?count=$BACKLOG" -o /dev/null \
    || die "failed to pre-load backlog"

  info "Starting consumer app with parallel-consumers=$consumers"
  CONSUMER_WORKFLOW_ID="kafka-consumer-activity-$run_id"
  SERVER_PORT="$CONSUMER_PORT" \
  CONSUMER_INSTANCE_ID="$run_id" \
  APP_CONSUMER_GROUP_ID="$group" \
  APP_CONSUMER_PARALLEL_CONSUMERS="$consumers" \
  APP_CONSUMER_POLL_TIMEOUT_MS=500 \
    "$JAVA_BIN" -jar "$JAR" > "/tmp/ceiling-$consumers.log" 2>&1 &
  CONSUMER_PID=$!

  # Wait until the group exists and has been assigned partitions, then start the clock. Excludes
  # JVM startup, which would otherwise be counted as consumption time.
  local waited=0
  until [ "$(total_lag "$group")" != "999999" ]; do
    kill -0 "$CONSUMER_PID" 2>/dev/null || die "consumer exited early — see /tmp/ceiling-$consumers.log"
    sleep 1
    waited=$((waited + 1))
    [ "$waited" -lt 120 ] || die "consumer group '$group' never appeared — see /tmp/ceiling-$consumers.log"
  done

  local start_ms; start_ms=$(now_ms)

  # Sample the assignment repeatedly through the drain and keep the fullest picture seen.
  #
  # A single snapshot is unreliable: consumers join over a few seconds and Kafka rebalances as they
  # do, so an early sample catches a partial group and reports a member count that is simply wrong.
  # Taking the maximum observed membership is stable regardless of when the group settles.
  local idle=0 total=0
  sample_members() {
    local s_idle s_total
    read -r s_idle s_total <<<"$(idle_members "$group")"
    if [ "$s_total" -gt "$total" ]; then total="$s_total"; idle="$s_idle"; fi
  }

  info "Draining..."
  until [ "$(total_lag "$group")" -eq 0 ] 2>/dev/null; do
    kill -0 "$CONSUMER_PID" 2>/dev/null || die "consumer exited during drain — see /tmp/ceiling-$consumers.log"
    sample_members
    sleep 1
    [ $(( ($(now_ms) - start_ms) / 1000 )) -lt "$DRAIN_TIMEOUT_S" ] \
      || die "drain exceeded ${DRAIN_TIMEOUT_S}s"
  done
  sample_members
  local drain_ms=$(( $(now_ms) - start_ms ))
  [ "$drain_ms" -gt 0 ] || drain_ms=1

  local drain_s rate
  drain_s=$(awk -v ms="$drain_ms" 'BEGIN{printf "%.1f", ms/1000}')
  rate=$(awk -v n="$BACKLOG" -v ms="$drain_ms" 'BEGIN{printf "%.1f", n/(ms/1000)}')

  info "Drained $BACKLOG messages in ${drain_s}s (~${rate} msg/s)"
  info "Group members: $total, of which idle (0 partitions): $idle"

  RESULTS+=("$consumers|$total|$idle|$drain_s|$rate")

  # Terminate the workflow first, then the process. Reversed, the workflow would survive and its
  # consumers would be resurrected by the next run's worker.
  cleanup
  CONSUMER_PID=""
  CONSUMER_WORKFLOW_ID=""
  sleep 5   # let the consumer group fully dissolve before the next run
}

# ---------------------------------------------------------------------------- main

log "Partition-ceiling demonstration"
info "Topic:      $TOPIC ($PARTITIONS partitions)"
info "Backlog:    $BACKLOG messages per run"
info "Configs:    ${CONSUMER_COUNTS[*]} consumers"
info "Pattern:    3 (long-running activity)"

for n in "${CONSUMER_COUNTS[@]}"; do
  run_configuration "$n"
done

# ---------------------------------------------------------------------------- results

log "Results — $PARTITIONS partitions"

# Baseline for the relative column: the first configuration run.
IFS='|' read -r _ _ _ _ base_rate <<<"${RESULTS[0]}"

printf '\n'
printf '    %-11s %-9s %-7s %-10s %-13s %-10s\n' \
  "CONSUMERS" "MEMBERS" "IDLE" "DRAIN(s)" "RATE(msg/s)" "vs BASE"
printf '    %-11s %-9s %-7s %-10s %-13s %-10s\n' \
  "---------" "-------" "----" "--------" "-----------" "-------"
for row in "${RESULTS[@]}"; do
  IFS='|' read -r consumers total idle drain rate <<<"$row"
  relative=$(awk -v r="$rate" -v b="$base_rate" 'BEGIN{printf "%.2fx", (b>0 ? r/b : 0)}')
  printf '    %-11s %-9s %-7s %-10s %-13s %-10s\n' \
    "$consumers" "$total" "$idle" "$drain" "$rate" "$relative"
done

printf '\n'
log "What this shows"
cat <<EOF

    Any run whose consumer count exceeds $PARTITIONS shows IDLE > 0. Those consumers joined the
    group, were assigned nothing, and did no work — Kafka gives each partition to at most one
    consumer in a group, so past $PARTITIONS there is nothing left to hand out. Throughput does
    not rise with them, and typically dips slightly: more members means more coordination for the
    same amount of work.

    The knee in the "vs BASE" column sits on the partition count. That is the ceiling, and it is
    identical for all three patterns in this repo. A fleet of external client applications, a
    fleet of consumer workflows, and a fleet of long-running activities are bounded the same way.
    The choice between the patterns is about visibility, Action cost, and what you want to
    operate — not throughput.

    If throughput is your problem, add partitions. Re-run with PARTITIONS=12 and watch the
    ceiling move with it.

    Two honest caveats about the RATE column — the IDLE column needs neither:

      * Scaling below the ceiling is real but sub-linear. Doubling consumers rarely doubles the
        rate, because something else starts to bind first: the target workflows, the worker pool,
        or the namespace's Actions-per-second limit. Consumption is rarely the part worth
        optimising.

      * Once one of those is the binding constraint, the rate flattens for reasons that have
        nothing to do with partitions. Only the IDLE column proves the partition ceiling itself.

EOF

log "Reproducing with the other patterns"
cat <<'EOF'

    Pattern 3 is used here because its consumer count is one property, so a single process hosts
    all N. The ceiling is a property of Kafka, not of the pattern, so the other two behave the
    same way — they just take more processes to demonstrate:

    Pattern 1 (consumer-external) — one process, N listener threads:
        APP_CONSUMER_CONCURRENCY=9 APP_CONSUMER_GROUP_ID=ceiling-ext \
          mvn -f consumer-external/pom.xml spring-boot:run

    Pattern 2 (consumer-workflow) — N processes, one per instance, because each instance needs its
    own task queue and exactly one worker:
        for i in $(seq 1 9); do
          CONSUMER_INSTANCE_ID=$i SERVER_PORT=$((8100+i)) APP_CONSUMER_GROUP_ID=ceiling-wf \
            mvn -f consumer-workflow/pom.xml spring-boot:run &
        done

    In both cases, inspect the assignment the same way:
        docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
          --bootstrap-server localhost:19092 --describe --group <group> --members

EOF
