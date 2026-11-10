#!/usr/bin/env bash
# A guided demo of the DLQ healing agent against a real model.
#
# It starts a throwaway Kafka (docker compose, project "dlqdemo") and a throwaway Postgres, runs the application, sends
# five failed orders to the DLQ, shows what the agent did with each, acts as the human reviewer for the one it held, and
# tears everything down again, also when something fails. Nothing outside this demo is touched.
#
# Needs: Docker, JDK 21, python3 and curl, a free port 9092 (Kafka), and OPENAI_API_KEY. The run makes about ten calls to the
# model (cents). The stack traces are hand-written and the model's answers vary a little from run to run.
#
#   export OPENAI_API_KEY=sk-...          # JAVA_HOME must point at a JDK 21, for example on macOS:
#   export JAVA_HOME=$(/usr/libexec/java_home -v 21)
#   ./scripts/demo.sh
#
# Optional: OPENAI_MODEL (default gpt-4o-mini), DEMO_DB_PORT (55432), DEMO_APP_PORT (8089).
set -uo pipefail
cd "$(dirname "$0")/.."

: "${OPENAI_API_KEY:?Set OPENAI_API_KEY first (it is used only for this run and never printed)}"
PROJECT=dlqdemo
DB_PORT="${DEMO_DB_PORT:-55432}"
APP_PORT="${DEMO_APP_PORT:-8089}"
PG=dlqdemo-pg
PASSWORD="demo-only-password"
WORK="$(mktemp -d)"
APP_PID=""
# the compose images are chosen for the native platform; a forced platform is what breaks a mixed arm64/amd64 setup
unset DOCKER_DEFAULT_PLATFORM

say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
note() { printf '    %s\n' "$*"; }
die() { printf '\nDemo stopped: %s\n' "$*" >&2; exit 1; }

cleanup() {
  [ -n "$APP_PID" ] && kill "$APP_PID" 2>/dev/null && wait "$APP_PID" 2>/dev/null
  docker compose -p "$PROJECT" down -v >/dev/null 2>&1
  docker rm -f "$PG" >/dev/null 2>&1
  rm -rf "$WORK"
}
trap cleanup EXIT

port_busy() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
for p in 9092 "$DB_PORT" "$APP_PORT"; do
  port_busy "$p" && die "port $p is already in use (set DEMO_DB_PORT / DEMO_APP_PORT, or stop what holds it)"
done
command -v python3 >/dev/null || die "python3 is needed to pretty-print JSON"

# ---------------------------------------------------------------------------------------------------------------------
say "Building the application (tests skipped; the full suite is 'mvn verify')"
./mvnw -B -q -DskipTests package || die "the build failed"
JAR="$(ls target/dlq-healing-agent-*.jar | grep -v original | head -1)"
note "built $JAR"

say "Starting a throwaway Kafka and Postgres"
docker compose -p "$PROJECT" up -d kafka >/dev/null 2>&1 || die "could not start Kafka"
docker run -d --name "$PG" -p "$DB_PORT:5432" -e POSTGRES_DB=dlqagent -e POSTGRES_USER=dlqagent \
  -e POSTGRES_PASSWORD=dlqagent postgres:16-alpine >/dev/null || die "could not start Postgres"
for i in $(seq 1 60); do
  docker exec "$PG" pg_isready -U dlqagent -d dlqagent >/dev/null 2>&1 &&
    [ "$(docker inspect -f '{{.State.Health.Status}}' "${PROJECT}-kafka-1" 2>/dev/null)" = healthy ] && break
  sleep 2
done
docker exec "$PG" pg_isready -U dlqagent -d dlqagent >/dev/null 2>&1 || die "Postgres did not become ready"
note "Kafka on :9092, Postgres on :$DB_PORT"

say "Starting the agent (model: ${OPENAI_MODEL:-gpt-4o-mini}; one stream: orders-dlq -> orders-topic)"
DB_URL="jdbc:postgresql://localhost:$DB_PORT/dlqagent" APPROVAL_PASSWORD="$PASSWORD" SERVER_PORT="$APP_PORT" \
  java -jar "$JAR" >"$WORK/app.log" 2>&1 &
APP_PID=$!
for i in $(seq 1 60); do
  curl -fs "http://localhost:$APP_PORT/actuator/health" 2>/dev/null | grep -q UP && break
  kill -0 "$APP_PID" 2>/dev/null || die "the application exited, see: $(tail -3 "$WORK/app.log" | tr '\n' ' ')"
  sleep 1
done
curl -fs "http://localhost:$APP_PORT/actuator/health" | grep -q UP || die "the application did not become healthy"
note "healthy at http://localhost:$APP_PORT"

# ---------------------------------------------------------------------------------------------------------------------
# One failed event on the DLQ, with the headers a dead-letter publisher would add (original topic, stack trace).
send() { # key correlation-id original-topic stack-trace payload
  printf 'kafka_dlt-original-topic=%s,x-correlation-id=%s,kafka_dlt-exception-stacktrace=%s\t%s:%s\n' "$3" "$2" "$4" "$1" "$5" |
    docker compose -p "$PROJECT" exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
      --topic orders-dlq --property parse.key=true --property key.separator=: \
      --property parse.headers=true --property headers.key.separator== 2>&1 | grep -v -E '^>|deprecated' || true
}
sql() { docker exec "$PG" psql -U dlqagent -d dlqagent -c "$1"; }

# Small pretty-printers, kept as plain Python so they are readable. Each reads JSON or console-consumer lines on stdin.
read -r -d '' PY_TOPIC <<'PY' || true
import sys, json
for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) < 3:
        continue
    headers, key, value = parts[0], parts[1], parts[2]
    flags = [h for h in headers.split(",") if h.startswith(("x-repaired", "x-retry", "x-approved"))]
    try:
        order = json.loads(value)
    except ValueError:
        continue
    print(f"  {key}: amount={order.get('amount')!r:>7} createdAt={order.get('createdAt')!r:<24} {' '.join(flags)}")
PY
read -r -d '' PY_FIND <<'PY' || true
import sys, json
for item in json.load(sys.stdin):
    if item["key"] == "demo-2":
        print(item["id"])
PY
read -r -d '' PY_ITEM <<'PY' || true
import sys, json
item = json.load(sys.stdin)
print("  status         :", item["status"])
print("  why a human    :", item["reason"][:110])
for c in item["changes"]:
    print(f"  proposed change: {c['field']}  {c['before']}  ->  {c['after']}")
print("  model summary  :", item["repairSummary"][:140])
PY
read -r -d '' PY_APPROVED <<'PY' || true
import sys, json
item = json.load(sys.stdin)
print("  status:", item["status"], "| reviewed by:", item["reviewedBy"], "|", item["reviewNotes"])
PY

# Everything on orders-topic, one line per message, with the headers the agent adds.
show_topic() {
  docker compose -p "$PROJECT" exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic orders-topic --from-beginning --timeout-ms 6000 --property print.key=true --property print.headers=true 2>/dev/null |
    python3 -c "$PY_TOPIC"
}

say "Sending five failed orders to orders-dlq"
note "1  demo-1  the amount arrived as text: \"21.50 USD\""
send demo-1 demo-c1 orders-topic \
  "org.springframework.dao.DataIntegrityViolationException: could not execute statement; constraint [chk_orders_amount] amount must be numeric" \
  '{"orderId":"demo-1","customerId":"c-1","amount":"21.50 USD","currency":"USD","status":"PAID","createdAt":"2025-04-03T10:00:00Z"}'
note "2  demo-2  the date is 03/04/2025: 3 April or 4 March?"
send demo-2 demo-c2 orders-topic \
  "java.time.format.DateTimeParseException: Text '03/04/2025' could not be parsed at index 0" \
  '{"orderId":"demo-2","customerId":"c-1","amount":42.50,"currency":"USD","status":"PAID","createdAt":"03/04/2025"}'
note "3  demo-3  a valid order; the consumer could not reach its database"
send demo-3 demo-c3 orders-topic \
  "org.springframework.dao.TransientDataAccessResourceException: Could not open JDBC Connection; nested exception is java.net.ConnectException: Connection refused" \
  '{"orderId":"demo-3","customerId":"c-1","amount":9.99,"currency":"USD","status":"NEW","createdAt":"2025-04-03T10:00:00Z"}'
note "4  demo-4  truncated JSON"
send demo-4 demo-c4 orders-topic \
  "com.fasterxml.jackson.core.JsonParseException: Unexpected end-of-input" \
  '{"orderId": "oops'
note "5  demo-5  a header claiming the event came from another topic (payments-topic)"
send demo-5 demo-c5 payments-topic \
  "jakarta.validation.ConstraintViolationException: Validation failed for classes [com.shop.orders.OrderEvent]" \
  '{"orderId":"demo-5","customerId":"c-1","amount":"5.00 USD","currency":"USD","status":"PAID","createdAt":"2025-04-03T10:00:00Z"}'

say "Waiting for the agent (planner, repair agent, gate, and the retry's backoff)"
for i in $(seq 1 120); do
  rows="$(docker exec "$PG" psql -U dlqagent -d dlqagent -Atc 'select count(*) from dlq_audit_log' 2>/dev/null || echo 0)"
  [ "${rows:-0}" -ge 6 ] && break
  sleep 1
done
[ "${rows:-0}" -ge 6 ] || die "only ${rows:-0} of 6 expected audit rows after 2 minutes; see $(basename "$WORK")/app.log"

say "What the agent decided (the audit log, written once per decision)"
sql "select kafka_key as key, strategy, tier, outcome, left(outcome_reason, 60) as reason
     from dlq_audit_log order by occurred_at, kafka_key"

say "What reached orders-topic (the only thing the agent ever writes back)"
show_topic
note "demo-4 and demo-5 were quarantined: nothing was replayed for them."

API="http://localhost:$APP_PORT/api/approvals"
ITEM="$(curl -s -u "reviewer:$PASSWORD" "$API" | python3 -c "$PY_FIND")"
if [ -z "$ITEM" ]; then
  say "The human step: nothing was held for demo-2 this time"
  note "The model declined to guess the date and the event went to quarantine for a person (see the audit table above)."
  note "That is a safe outcome too; models vary from run to run. Run the demo again to see the approval path."
else
  say "The human step: the held repair for demo-2, as the reviewer sees it"
  curl -s -u "reviewer:$PASSWORD" "$API/$ITEM" | python3 -c "$PY_ITEM"

  say "Approving it (a JSON body is required; reviewer notes go into the audit log)"
  curl -s -u "reviewer:$PASSWORD" -H 'Content-Type: application/json' \
    -d '{"notes":"3 April: confirmed with the sender"}' "$API/$ITEM/approve" |
    python3 -c "$PY_APPROVED"
  sleep 3
  say "demo-2 now on orders-topic: replayed after the approval, with the reviewer's name in a header"
  show_topic | grep -E '^  demo-2' || note "(not visible yet: the replay can take a moment)"
  sql "select stage, kafka_key as key, outcome, actor, left(outcome_reason, 50) as reason
       from dlq_audit_log where kafka_key = 'demo-2' order by occurred_at"
fi

say "Demo finished. Cleaning up the throwaway containers and the application."
