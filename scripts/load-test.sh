#!/usr/bin/env bash
#
# load-test.sh — measure all three consumption patterns under the same load.
#
# Implements the methodology in PRD section 12. For each (pattern, rate, scale) case it recreates
# the topic, starts that pattern's consumer(s) on a fresh consumer group, drives the producer at a
# fixed rate for a fixed window, and records:
#
#   * sustained throughput, and whether the consumer kept up (lag flat vs growing)
#   * p50 / p95 / p99 latency from event timestamp to workflow start
#   * modeled consumption Actions/sec, against the namespace's Actions/sec limit
#   * RESOURCE_EXHAUSTED rate observed by the SDK
#   * worker CPU and heap while the case ran
#   * for Pattern 2: event-history growth and continue-as-new count
#
# Results print as a table and are appended to a CSV for later analysis.
#
# Usage:
#   scripts/load-test.sh                                   # all patterns, PRD's rate ladder
#   RATES="50 200" scripts/load-test.sh                    # quick pass
#   PATTERNS="wf" RATES="100" scripts/load-test.sh          # one pattern
#   SCALES="1 3 6" RATES="300" scripts/load-test.sh         # scale-out matrix at one rate
#
# Environment:
#   PATTERNS   patterns to test: ext (external app), wf (workflow), act (activity). Default all.
#   RATES      producer rates in events/sec. Default "10 50 100 150"; see the note at the
#              assignment before raising it, and check DELIV in the output if you do.
#   SCALES     consumers per pattern. Default "1". Use "1 3 6" for the scale-out matrix.
#   DURATION   seconds to hold each rate. Default 45.
#   PARTITIONS topic partitions. Default 6.
#   OUT        CSV output path. Default results/load-test-<timestamp>.csv
#
# Prerequisites: docker compose up -d; temporal server start-dev; mvn -DskipTests install;
# order-email-worker and producer running. The script starts and stops the consumers itself.
#
# NOTE ON WHAT THIS MEASURES: these numbers describe a laptop running a dev-server Temporal, a
# single-broker Kafka, and every component on one machine. They are useful for COMPARING the three
# patterns against each other under identical conditions — which is the point — and useless as
# absolute capacity figures. Size real deployments from your own environment.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=lib/common.sh
source "$REPO_ROOT/scripts/lib/common.sh"

# ---------------------------------------------------------------------------- configuration

PATTERNS="${PATTERNS:-ext wf act}"
# Stops at 150 because the producer on a single laptop tops out around 160 events/s: a case
# requesting 400/s delivered 157.3/s. Rungs above that measure the producer, not the consumer, and a
# ladder run to 250 and 500 offered the same load at both, identical cases wearing different labels.
# Raise it only where you have confirmed the producer can feed it, and watch DELIV when you do.
RATES="${RATES:-10 50 100 150}"
SCALES="${SCALES:-1}"
DURATION="${DURATION:-45}"
PARTITIONS="${PARTITIONS:-6}"
SETTLE_S="${SETTLE_S:-8}"          # let the group finish rebalancing before the clock starts
NAMESPACE_ACTION_LIMIT="${NAMESPACE_ACTION_LIMIT:-500}"
# Pattern 2's records-per-poll. The repo pins this to 1 so the implementation validates the
# reference architecture's 3-Actions-per-message figure; raise it here to measure what that pinning
# costs in throughput. Affects Pattern 2 only.
BATCH_SIZE="${BATCH_SIZE:-1}"
OUT="${OUT:-$REPO_ROOT/results/load-test-$(date +%Y%m%d-%H%M%S).csv}"

BASE_PORT=8110                      # consumer ports; avoids the 8082-8084 defaults

PIDS=()
WORKFLOW_IDS=()
RESULTS=()

# ---------------------------------------------------------------------------- pattern metadata

pattern_name() {
  case "$1" in
    ext) printf 'Pattern 1 — External Application' ;;
    wf)  printf 'Pattern 2 — Workflow' ;;
    act) printf 'Pattern 3 — Long-Running Activity' ;;
  esac
}

pattern_module() {
  case "$1" in
    ext) printf 'consumer-external' ;;
    wf)  printf 'consumer-workflow' ;;
    act) printf 'consumer-activity' ;;
  esac
}

# Actions consumed per message, counting the workflow start plus each activity execution.
# Excludes Actions consumed inside the target workflow, which are identical for all three patterns
# and would swamp the difference being measured.
#
# Every pattern pays 1 Action per message for the workflow start. Pattern 2 additionally pays for
# its loop, and that cost is per POLL CYCLE rather than per message: scheduling poll, start and
# commit is 3 Actions however many records the cycle returned. So its per-message cost is
# 1 + 3/batch — four Actions at one record per poll, 1.06 at fifty.
#
# Counting only the loop (3/batch) would leave this pattern's figure measuring something different
# from the other two, which is the same inconsistency the reference architecture's cost table has.
pattern_action_factor() {
  case "$1" in
    ext) printf '1' ;;   # the workflow start
    wf)  awk -v b="$BATCH_SIZE" 'BEGIN{printf "%.4f", 1 + 3/(b>0?b:1)}' ;;
    act) printf '1' ;;   # the workflow start; heartbeats are throttled and counted separately
  esac
}

# ---------------------------------------------------------------------------- lifecycle

stop_consumers() {
  local wf
  for wf in "${WORKFLOW_IDS[@]:-}"; do terminate_consumer_workflow "$wf"; done
  local pid
  for pid in "${PIDS[@]:-}"; do
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  PIDS=()
  WORKFLOW_IDS=()
}

LOCK_DIR="${TMPDIR:-/tmp}/kafka-consumption-load-test.lock"
HOLDS_LOCK=0

cleanup() {
  curl -sf -X DELETE "$PRODUCER_URL/orders/stream" -o /dev/null 2>/dev/null || true
  stop_consumers
  # Only release the lock if this process took it, so a run that exited on the guard cannot delete
  # the lock belonging to the run it just refused to race.
  [ "$HOLDS_LOCK" = "1" ] && rm -rf "$LOCK_DIR"
  return 0
}
trap cleanup EXIT INT TERM

# start_consumers <pattern> <scale> <group> <run_id>
#
# Pattern 2 needs one PROCESS per unit of scale: each instance owns a task queue served by exactly
# one worker (PRD FR-2.9). Patterns 1 and 3 scale inside a single process, via listener threads and
# parallel activities respectively.
start_consumers() {
  local pattern="$1" scale="$2" group="$3" run_id="$4"
  local module jar port i
  module=$(pattern_module "$pattern")
  jar=$(find_jar "$module" "$REPO_ROOT")

  case "$pattern" in
    ext)
      port=$BASE_PORT
      SERVER_PORT="$port" APP_CONSUMER_GROUP_ID="$group" APP_CONSUMER_CONCURRENCY="$scale" \
        "$JAVA_BIN" -jar "$jar" > "/tmp/lt-$pattern-$run_id.log" 2>&1 &
      PIDS+=("$!")
      ;;
    act)
      port=$BASE_PORT
      SERVER_PORT="$port" APP_CONSUMER_GROUP_ID="$group" \
      CONSUMER_INSTANCE_ID="$run_id" APP_CONSUMER_PARALLEL_CONSUMERS="$scale" \
      APP_CONSUMER_POLL_TIMEOUT_MS=500 \
        "$JAVA_BIN" -jar "$jar" > "/tmp/lt-$pattern-$run_id.log" 2>&1 &
      PIDS+=("$!")
      WORKFLOW_IDS+=("kafka-consumer-activity-$run_id")
      ;;
    wf)
      for ((i = 0; i < scale; i++)); do
        port=$((BASE_PORT + i))
        SERVER_PORT="$port" APP_CONSUMER_GROUP_ID="$group" \
        CONSUMER_INSTANCE_ID="$run_id-$i" APP_CONSUMER_POLL_TIMEOUT_MS=500 \
        APP_CONSUMER_BATCH_SIZE="$BATCH_SIZE" \
          "$JAVA_BIN" -jar "$jar" > "/tmp/lt-$pattern-$run_id-$i.log" 2>&1 &
        PIDS+=("$!")
        WORKFLOW_IDS+=("kafka-consumer-$run_id-$i")
      done
      ;;
  esac

  # Wait for the first consumer's actuator, then for the group to actually exist in Kafka.
  local waited=0
  until curl -sf "http://localhost:$BASE_PORT/actuator/health" >/dev/null 2>&1; do
    sleep 2; waited=$((waited + 2))
    [ "$waited" -lt 120 ] || die "consumer did not start — see /tmp/lt-$pattern-$run_id*.log"
  done
  # Readiness is "the group has members", not "the group reports lag". With no traffic yet there is
  # nothing to be behind on, so waiting for a lag figure would wait forever.
  waited=0
  until [ "$(group_member_count "$group")" -ge 1 ]; do
    sleep 2; waited=$((waited + 2))
    [ "$waited" -lt 120 ] || die "consumer group '$group' never appeared — see /tmp/lt-$pattern-$run_id*.log"
  done
  info "Consumer group '$group' has $(group_member_count "$group") member(s)"
}

# ---------------------------------------------------------------------------- one case

run_case() {
  local pattern="$1" rate="$2" scale="$3"
  local run_id="lt-$(date +%s)-$pattern-$rate-$scale"
  local group="loadtest-$pattern-$rate-$scale-$$"
  local url="http://localhost:$BASE_PORT"

  log "$(pattern_name "$pattern")  |  ${rate} events/s  |  scale ${scale}"

  recreate_topic "$PARTITIONS"
  start_consumers "$pattern" "$scale" "$group" "$run_id"

  # Producer first, then settle: the group finishes rebalancing while real traffic flows, so the
  # measurement window starts in steady state rather than mid-rebalance.
  curl -sf -X POST "$PRODUCER_URL/orders/stream?ratePerSecond=$rate" -o /dev/null \
    || die "could not start producer stream"
  sleep "$SETTLE_S"

  # --- baseline snapshot
  local consumed_0 started_0 exhausted_0 lag_0 produced_0 t0
  consumed_0=$(scrape_sum "$url" "^kafka_messages_consumed_total")
  started_0=$(scrape_sum "$url" "^temporal_workflows_started_total")
  exhausted_0=$(scrape_sum "$url" "^temporal_(long_)?request_failure.*RESOURCE_EXHAUSTED")
  lag_0=$(total_lag "$group")
  produced_0=$(topic_end_offset "$group")
  t0=$(now_ms)

  # --- hold the rate, sampling lag and worker load
  local samples=0 cpu_sum=0 heap_max=0 lag_last=$lag_0
  local elapsed=0
  while [ "$elapsed" -lt "$DURATION" ]; do
    sleep 5
    elapsed=$(( ($(now_ms) - t0) / 1000 ))
    lag_last=$(total_lag "$group")
    local cpu heap
    cpu=$(scrape "$WORKER_URL" "^process_cpu_usage")
    heap=$(scrape_sum "$WORKER_URL" '^jvm_memory_used_bytes\{.*area="heap"')
    cpu_sum=$(awk -v a="$cpu_sum" -v b="$cpu" 'BEGIN{printf "%.6f", a+b}')
    heap_max=$(awk -v a="$heap_max" -v b="$heap" 'BEGIN{printf "%.0f", (b>a?b:a)}')
    samples=$((samples + 1))
  done

  local t1; t1=$(now_ms)
  curl -sf -X DELETE "$PRODUCER_URL/orders/stream" -o /dev/null || true

  # --- final snapshot
  local consumed_1 started_1 exhausted_1 produced_1 p50 p95 p99
  produced_1=$(topic_end_offset "$group")
  consumed_1=$(scrape_sum "$url" "^kafka_messages_consumed_total")
  started_1=$(scrape_sum "$url" "^temporal_workflows_started_total")
  exhausted_1=$(scrape_sum "$url" "^temporal_(long_)?request_failure.*RESOURCE_EXHAUSTED")
  p50=$(scrape "$url" '^kafka_event_to_workflow_start_seconds\{.*quantile="0.5"')
  p95=$(scrape "$url" '^kafka_event_to_workflow_start_seconds\{.*quantile="0.95"')
  p99=$(scrape "$url" '^kafka_event_to_workflow_start_seconds\{.*quantile="0.99"')

  local window_s throughput started_rate cpu_avg actions kept_up delivered
  window_s=$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.3f", (b-a)/1000}')
  throughput=$(awk -v c0="$consumed_0" -v c1="$consumed_1" -v w="$window_s" \
                   'BEGIN{printf "%.1f", (w>0 ? (c1-c0)/w : 0)}')
  started_rate=$(awk -v s0="$started_0" -v s1="$started_1" -v w="$window_s" \
                   'BEGIN{printf "%.1f", (w>0 ? (s1-s0)/w : 0)}')
  cpu_avg=$(awk -v s="$cpu_sum" -v n="$samples" 'BEGIN{printf "%.1f", (n>0 ? 100*s/n : 0)}')
  actions=$(awk -v t="$throughput" -v f="$(pattern_action_factor "$pattern")" \
                   'BEGIN{printf "%.1f", t*f}')

  # What the producer actually put on the topic, which is the load the consumer was really offered.
  # Prints "-" rather than a wrong number if either offset snapshot came back unusable.
  delivered=$(awk -v a="$produced_0" -v b="$produced_1" -v w="$window_s" \
                  'BEGIN{ if (a<=0 || b<=0 || w<=0 || b<a) print "-"; else printf "%.1f", (b-a)/w }')

  # "Kept up" = the consumer is not falling behind. Lag below one second of throughput is noise from
  # the sampling boundary; lag well past that means the offered load exceeded capacity.
  #
  # Measured throughput, NOT the requested rate. Where the producer cannot deliver what was asked
  # for, the requested figure is an arbitrarily generous allowance: at 500/s requested it would pass
  # any lag under 500 records, while the same lag at 50/s requested would fail. That made the column
  # incomparable between rows, which is the one thing a yes/no column has to get right.
  kept_up=$(awk -v l="$lag_last" -v t="$throughput" 'BEGIN{print (l <= t ? "yes" : "NO")}')

  # Pattern 2 exposes loop internals worth recording: history growth is the direct cost of its
  # per-message visibility.
  local history="-" continuations="-"
  if [ "$pattern" = "wf" ]; then
    local status
    status=$(curl -sf "$url/consumer" 2>/dev/null || true)
    history=$(grep -o '"historyLength":[0-9]*' <<<"$status" | cut -d: -f2 || echo "-")
    continuations=$(grep -o '"continuations":[0-9]*' <<<"$status" | cut -d: -f2 || echo "-")
    [ -n "$history" ] || history="-"
    [ -n "$continuations" ] || continuations="-"
  fi

  local exhausted_delta
  exhausted_delta=$(awk -v a="$exhausted_0" -v b="$exhausted_1" 'BEGIN{printf "%.0f", b-a}')

  info "producer delivered ${delivered}/s of the ${rate}/s requested"
  info "throughput ${throughput} msg/s  |  starts ${started_rate}/s  |  lag ${lag_last}  |  kept up: ${kept_up}"
  info "latency p50/p95/p99: ${p50}s / ${p95}s / ${p99}s"

  # A producer that cannot hit the requested rate makes this case a measurement of the producer.
  # Say so at the point of measurement rather than leaving a reader to derive it from lag arithmetic
  # weeks later.
  if [ "$delivered" != "-" ] \
     && awk -v d="$delivered" -v r="$rate" 'BEGIN{exit !(d < 0.9*r)}'; then
    warn "Producer fell short of the requested rate, so this case bounds the PRODUCER, not the consumer."
    warn "Treat ${rate}/s as ${delivered}/s, and do not compare it against cases that were fully fed."
  fi
  info "modeled consumption Actions/s: ${actions}  |  RESOURCE_EXHAUSTED: ${exhausted_delta}"
  [ "$pattern" = "wf" ] && info "history length ${history}, continuations ${continuations}"

  local batch_col="-"
  [ "$pattern" = "wf" ] && batch_col="$BATCH_SIZE"

  RESULTS+=("$pattern|$rate|$scale|$throughput|$kept_up|$lag_last|$p50|$p95|$p99|$actions|$exhausted_delta|$cpu_avg|$heap_max|$history|$continuations|$batch_col|$delivered")
  # delivered_msg_s is appended rather than placed next to rate, so column positions stay stable for
  # anything already reading these files.
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$pattern" "$rate" "$scale" "$throughput" "$kept_up" "$lag_last" \
    "$p50" "$p95" "$p99" "$actions" "$exhausted_delta" "$cpu_avg" "$heap_max" \
    "$history" "$continuations" "$batch_col" "$delivered" >> "$OUT"

  stop_consumers
  sleep 5   # let the consumer group dissolve before the next case
}

# ---------------------------------------------------------------------------- main

log "Checking prerequisites"

# Refuse to run concurrently with another load test. Two runs share the topic, the consumer ports,
# and often the CSV, so a second invocation silently corrupts both sets of results — easy to do by
# accident and very hard to spot afterwards.
#
# A lock directory rather than pgrep: mkdir is atomic, and matching process names catches the
# invoking shell too (its command line contains this script's path), which produces a false positive
# on every run.
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  lock_pid=$(cat "$LOCK_DIR/pid" 2>/dev/null || echo "")
  if [ -n "$lock_pid" ] && kill -0 "$lock_pid" 2>/dev/null; then
    die "another load-test.sh is already running (pid $lock_pid).
       Concurrent runs share the topic, consumer ports, and output file, so their results would be
       meaningless. Wait for it to finish, or kill it."
  fi
  warn "Removing stale lock from a run that did not exit cleanly"
  rm -rf "$LOCK_DIR"
  mkdir "$LOCK_DIR"
fi
printf '%s' "$$" > "$LOCK_DIR/pid"
HOLDS_LOCK=1

check_common_prereqs
terminate_orphans "kafka-consumer"

# Chaos off: downstream failure injection would confound throughput with retry behavior.
if curl -sf -X POST "$WORKER_URL/chaos/reset" -o /dev/null 2>/dev/null; then
  info "Downstream failure injection reset to zero"
else
  warn "Could not reset chaos settings — results may include injected failures"
fi

read -ra PATTERN_LIST <<<"$PATTERNS"
read -ra RATE_LIST <<<"$RATES"
read -ra SCALE_LIST <<<"$SCALES"

total_cases=$(( ${#PATTERN_LIST[@]} * ${#RATE_LIST[@]} * ${#SCALE_LIST[@]} ))
est_min=$(awk -v n="$total_cases" -v d="$DURATION" -v s="$SETTLE_S" 'BEGIN{printf "%.0f", n*(d+s+30)/60}')

log "Load test plan"
info "Patterns:   ${PATTERN_LIST[*]}"
info "Rates:      ${RATE_LIST[*]} events/s"
info "Scales:     ${SCALE_LIST[*]}"
info "Duration:   ${DURATION}s per case"
info "Partitions: $PARTITIONS"
info "Cases:      $total_cases  (~${est_min} min)"
info "CSV:        $OUT"

mkdir -p "$(dirname "$OUT")"
printf 'pattern,rate,scale,throughput_msg_s,kept_up,final_lag,p50_s,p95_s,p99_s,actions_s_modeled,resource_exhausted,worker_cpu_pct,worker_heap_bytes,history_length,continuations,batch_size,delivered_msg_s\n' > "$OUT"

for scale in "${SCALE_LIST[@]}"; do
  for rate in "${RATE_LIST[@]}"; do
    for pattern in "${PATTERN_LIST[@]}"; do
      run_case "$pattern" "$rate" "$scale"
    done
  done
done

# ---------------------------------------------------------------------------- results

log "Results"
printf '\n'
printf '    %-6s %-7s %-8s %-6s %-6s %-10s %-8s %-8s %-9s %-9s %-11s %-6s\n' \
  "PATT" "RATE" "DELIV" "SCALE" "BATCH" "THRUPUT" "KEPT UP" "LAG" "p50(s)" "p99(s)" "ACTIONS/s" "CPU%"
printf '    %-6s %-7s %-8s %-6s %-6s %-10s %-8s %-8s %-9s %-9s %-11s %-6s\n' \
  "----" "----" "-----" "-----" "-----" "-------" "-------" "---" "------" "------" "---------" "----"
for row in "${RESULTS[@]}"; do
  IFS='|' read -r p r s thr kept lag p50 p95 p99 act exh cpu heap hist cont batch deliv <<<"$row"
  # Micrometer reports seconds at full float precision; three decimals is plenty and keeps the
  # table readable.
  p50=$(awk -v v="$p50" 'BEGIN{printf "%.3f", v}')
  p99=$(awk -v v="$p99" 'BEGIN{printf "%.3f", v}')
  printf '    %-6s %-7s %-8s %-6s %-6s %-10s %-8s %-8s %-9s %-9s %-11s %-6s\n' \
    "$p" "$r" "$deliv" "$s" "$batch" "$thr" "$kept" "$lag" "$p50" "$p99" "$act" "$cpu"
done
printf '\n'
info "Full results, including p95, RESOURCE_EXHAUSTED, heap, and Pattern 2 history growth: $OUT"

log "Reading these numbers"
cat <<EOF

    DELIV is what the producer actually put on the topic, against the RATE it was asked for. Compare
    THRUPUT against DELIV, not against RATE. Where DELIV falls short of RATE the case bounds the
    producer rather than the consumer, and a warning was printed above when that happened.

    THRUPUT vs DELIV — where throughput tracks what was delivered and KEPT UP is "yes", the pattern
    is comfortably within capacity. The first rate where KEPT UP flips to NO is that pattern's
    ceiling under these conditions. KEPT UP compares final lag against one second of measured
    throughput, so it means the same thing in every row regardless of the rate requested.

    ACTIONS/s is MODELED, not measured: throughput multiplied by the per-message cost. Every
    pattern pays 1 Action for the workflow start; Pattern 2 additionally pays 3 per poll cycle, so
    its per-message cost is 1 + 3/batch — 4 at one record per poll, 1.06 at fifty. Actions consumed
    inside the target workflow are excluded, being identical across all three. Compare against your
    namespace limit (assumed ${NAMESPACE_ACTION_LIMIT}/s): unbatched, Pattern 2 reaches it at a
    quarter of the message rate the other two do.

    RESOURCE_EXHAUSTED counts SDK-observed rate-limit rejections during the window. Non-zero means
    the namespace limit, not the consumer, is the binding constraint — every number in that row
    describes Temporal's throttle rather than the pattern.

    CPU% is the ORDER-EMAIL WORKER, shared by all three patterns. It is the same downstream work in
    every case, which is exactly why it is worth watching: when it saturates, consumption stops
    being the thing you are measuring.

    Latency is from the event's own timestamp to the workflow start being accepted, so it includes
    time the message spent waiting on the topic. Under sustained overload it grows without bound —
    that is queueing, not per-message cost.

EOF

log "Caveats worth stating before anyone quotes these"
cat <<'EOF'

    * One laptop, one broker, a dev-server Temporal, everything sharing a CPU. Useful for comparing
      the three patterns under identical conditions; useless as absolute capacity figures.

    * Consumption is rarely the binding constraint. Below the partition ceiling, scaling is already
      sub-linear because the worker pool and Action limits bind first — see
      scripts/demo-partition-ceiling.sh.

    * Pattern 2's cost assumes batch-size 1, which this repo pins deliberately so the implementation
      validates the reference architecture's published 3-Actions-per-message figure. Raising it
      changes that row and the doc must be updated to match.

    * Run-to-run variance at saturation is large. Two runs of one identical Pattern 3 configuration
      at 150/s differed by 1.95x on this hardware. Below saturation it is far tighter: six runs at
      100/s spanned 1.11x for Pattern 1 and 1.07x for Pattern 3. Treat any difference under about 2x
      at saturation as noise unless you have repeated the run. Differences of 10x or more (see
      Pattern 2 at batch-size 1) are real.

    * Tuning knobs that move these numbers, none of which are exercised here (PRD section 12):
      namespace Actions/s and per-task-queue rate limits; worker MaxConcurrentWorkflowTaskPollers,
      MaxConcurrentActivityTaskPollers, MaxConcurrentWorkflowTaskExecutionSize,
      MaxConcurrentActivityTaskExecutionSize, MaxCachedWorkflows, MaxWorkflowThreadCount; and
      replica count.

EOF
