#!/usr/bin/env bash
#
# Shared helpers for the scripts in this directory.
#
# Kept in one place rather than copied because some of it is correctness-critical — in particular
# terminate_consumer_workflow(), which is the only correct way to stop a Pattern 2 or 3 consumer.
# Killing the process leaves the workflow running, and the next worker to poll that task queue
# resurrects its activities.

# ---------------------------------------------------------------------------- shared defaults

TOPIC="${TOPIC:-orders.completed}"
DLT_TOPIC="${DLT_TOPIC:-orders.completed.DLT}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
BROKER="${BROKER:-localhost:19092}"
PRODUCER_URL="${PRODUCER_URL:-http://localhost:8080}"
WORKER_URL="${WORKER_URL:-http://localhost:8081}"
TEMPORAL_ADDRESS="${TEMPORAL_ADDRESS:-localhost:7233}"
TEMPORAL_NAMESPACE="${TEMPORAL_NAMESPACE:-default}"
JAVA_BIN="${JAVA_HOME:+$JAVA_HOME/bin/}java"

# ---------------------------------------------------------------------------- output

log()  { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
info() { printf '    %s\n' "$*"; }
warn() { printf '\033[33m    ! %s\033[0m\n' "$*"; }
die()  { printf '\n\033[31mERROR: %s\033[0m\n\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------- time

# Epoch milliseconds. GNU date supports %3N; BSD/macOS date does not, so fall back to perl and
# finally to whole seconds. Sub-second resolution matters: at typical drain times, second-
# granularity timing alone can manufacture a 20% "speedup" that does not exist.
now_ms() {
  local t
  t=$(date +%s%3N 2>/dev/null || true)
  if [[ "$t" =~ ^[0-9]+$ && "$t" != *N* ]]; then printf '%s' "$t"; return; fi
  if command -v perl >/dev/null 2>&1; then
    perl -MTime::HiRes -e 'printf "%d", Time::HiRes::time()*1000'; return
  fi
  printf '%s000' "$(date +%s)"
}

# ---------------------------------------------------------------------------- kafka

kafka() { docker exec "$KAFKA_CONTAINER" "/opt/kafka/bin/$1" --bootstrap-server "$BROKER" "${@:2}"; }

recreate_topic() {
  local partitions="$1"
  kafka kafka-topics.sh --delete --topic "$TOPIC" >/dev/null 2>&1 || true
  sleep 2
  kafka kafka-topics.sh --create --topic "$TOPIC" --partitions "$partitions" \
        --replication-factor 1 >/dev/null
  kafka kafka-topics.sh --create --topic "$DLT_TOPIC" --partitions "$partitions" \
        --replication-factor 1 >/dev/null 2>&1 || true
}

# Total lag across every partition of a group. An absent group prints a sentinel so callers never
# mistake "not started yet" for "fully drained".
#
# A group that has been assigned partitions but has not committed anything yet reports "-" for both
# CURRENT-OFFSET and LAG. That is not zero lag — it is "everything on the partition is outstanding" —
# so fall back to LOG-END-OFFSET in that case. Treating it as zero would report a fresh consumer as
# fully caught up before it had read a single record.
#
#   columns: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
#            $1    $2    $3        $4             $5             $6
total_lag() {
  local group="$1" out
  out=$(kafka kafka-consumer-groups.sh --describe --group "$group" 2>/dev/null || true)
  awk -v t="$TOPIC" '
    $2==t {
      seen=1
      if ($6 ~ /^[0-9]+$/)      s += $6
      else if ($5 ~ /^[0-9]+$/) s += $5
    }
    END { print (seen ? s+0 : 999999) }' <<<"$out"
}

# Number of members currently in the group. This, not lag, is the correct readiness signal: a group
# can have members long before it has any committed offsets to report.
group_member_count() {
  local group="$1" out
  out=$(kafka kafka-consumer-groups.sh --describe --group "$group" --members 2>/dev/null || true)
  awk 'NR>1 && NF>=5 && $NF ~ /^[0-9]+$/ {n++} END {print n+0}' <<<"$out"
}

# "<idle> <total>" — group members holding zero partitions, and members overall.
idle_members() {
  local group="$1" out
  out=$(kafka kafka-consumer-groups.sh --describe --group "$group" --members 2>/dev/null || true)
  awk 'NR>1 && NF>=5 && $NF ~ /^[0-9]+$/ {total++; if ($NF==0) idle++} END {printf "%d %d", idle+0, total+0}' <<<"$out"
}

# ---------------------------------------------------------------------------- temporal

# Stopping a Pattern 2 or 3 consumer means terminating its WORKFLOW, not killing the process.
#
# The consumer's lifecycle belongs to Temporal. Kill the JVM and the workflow survives, its
# activities merely stop heartbeating, and the next worker to poll that task queue picks them up and
# resumes consuming under the ORIGINAL settings. Left unhandled, consumers silently accumulate across
# runs until they exhaust the worker's activity slots.
terminate_consumer_workflow() {
  local wf="$1"
  [ -n "$wf" ] || return 0
  temporal workflow terminate \
    --address "$TEMPORAL_ADDRESS" --namespace "$TEMPORAL_NAMESPACE" \
    --workflow-id "$wf" --reason "script run finished" >/dev/null 2>&1 || true
}

# Clears consumers orphaned by an interrupted earlier run. Pass a workflow-ID prefix to match.
terminate_orphans() {
  local id_prefix="${1:-}"
  local ids
  ids=$(temporal workflow list \
        --address "$TEMPORAL_ADDRESS" --namespace "$TEMPORAL_NAMESPACE" \
        --query "(WorkflowType = 'KafkaConsumerActivityWorkflow' OR WorkflowType = 'KafkaConsumerWorkflow') AND ExecutionStatus = 'Running'" \
        --limit 200 --output json 2>/dev/null \
        | grep -o "\"workflowId\": *\"${id_prefix}[^\"]*\"" \
        | sed 's/.*: *"//; s/"$//' | sort -u || true)
  [ -n "$ids" ] || return 0
  info "Terminating $(grep -c . <<<"$ids") orphaned consumer workflow(s) from a previous run"
  while read -r id; do
    [ -n "$id" ] && terminate_consumer_workflow "$id"
  done <<<"$ids"
  sleep 2
}

# ---------------------------------------------------------------------------- metrics

# First value of a Prometheus metric line matching a pattern; 0 when absent.
#
# The trailing "|| true" on each pipeline is load-bearing, not defensive noise. Callers run under
# `set -euo pipefail`, and a metric that has not been emitted yet — a timer with no recordings, a
# RESOURCE_EXHAUSTED counter that has never fired — makes grep exit 1, which pipefail propagates and
# set -e turns into a silent abort mid-run. An absent metric means zero, not failure.
scrape() {
  local url="$1" pattern="$2" v
  v=$(curl -sf "$url/actuator/prometheus" 2>/dev/null | grep -E "$pattern" | head -1 | awk '{print $NF}' || true)
  [[ "$v" =~ ^-?[0-9.eE+]+$ ]] && printf '%s' "$v" || printf '0'
}

# Sum of all Prometheus metric lines matching a pattern; 0 when absent.
scrape_sum() {
  local url="$1" pattern="$2" v
  v=$(curl -sf "$url/actuator/prometheus" 2>/dev/null | grep -E "$pattern" | awk '{s+=$NF} END {printf "%.6f", s+0}' || true)
  [[ "$v" =~ ^-?[0-9.eE+]+$ ]] && printf '%s' "$v" || printf '0'
}

# ---------------------------------------------------------------------------- preconditions

require_http() { curl -sf "$1" >/dev/null 2>&1 || die "$2"; }

check_common_prereqs() {
  command -v docker >/dev/null || die "docker not found"
  docker exec "$KAFKA_CONTAINER" true 2>/dev/null \
    || die "Kafka container '$KAFKA_CONTAINER' is not running. Start it with:  docker compose up -d"
  info "Kafka container is up"

  require_http "$PRODUCER_URL/actuator/health" \
    "producer is not running on $PRODUCER_URL. Start it with:  mvn -f producer/pom.xml spring-boot:run"
  info "Producer is up"

  require_http "$WORKER_URL/actuator/health" \
    "order-email-worker is not running on $WORKER_URL. Start it with:  mvn -f order-email-worker/pom.xml spring-boot:run"
  info "order-email-worker is up"

  "$JAVA_BIN" -version 2>&1 | head -1 | grep -qE '"(2[1-9]|[3-9][0-9])' \
    || die "Java 21+ required. Set JAVA_HOME to a suitable JDK. Found: $("$JAVA_BIN" -version 2>&1 | head -1)"
  info "Java OK"

  command -v temporal >/dev/null \
    || die "temporal CLI not found — needed to terminate consumer workflows between runs"
  temporal operator namespace describe --address "$TEMPORAL_ADDRESS" "$TEMPORAL_NAMESPACE" >/dev/null 2>&1 \
    || die "Temporal is not reachable at $TEMPORAL_ADDRESS. Start it with:  temporal server start-dev"
  info "Temporal is up"
}

# Locates a module's built jar, or dies with the build command.
find_jar() {
  local module="$1" repo_root="$2" jar
  jar=$(ls "$repo_root/$module/target/$module"-*.jar 2>/dev/null | grep -v sources | head -1 || true)
  [ -n "$jar" ] || die "$module jar not found. Build it with:  mvn -DskipTests install"
  printf '%s' "$jar"
}
