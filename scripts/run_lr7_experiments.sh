#!/usr/bin/env bash
# Прогон экспериментов ЛР №7: выбор лидера и механизм консенсуса в распределённой системе.
#
# Проект остался тем же, что в ЛР №4-№6: клиент и сервер говорят по TCP+JSON, данные лежат в
# PostgreSQL в схемах riot_primary и riot_replica, события публикуются через Publisher → Broker →
# Subscriber. Новое здесь - кластер узлов: каждый узел слушает свой порт на 127.0.0.1, узлы выбирают
# лидера, обнаруживают отказ по heartbeat, перевыбирают лидера и голосуют за BookingStatus.
#
#   createdb hotelbooking                             # один раз, база не создаётся сама
#   ./scripts/run_lr7_experiments.sh
#   caffeinate -i ./scripts/run_lr7_experiments.sh    # чтобы macOS не уснул в середине прогона
#
# Переменные окружения: PG_HOST, PG_PORT, PG_DB, PG_USER, PGPASSWORD, PORT, NODE_BASE, NODES,
#                       HEARTBEAT, ROOM, ROOM2, RESERVE_DELAY, PRIMARY_SCHEMA, REPLICA_SCHEMA.
# Пароль в файле не хранится: libpq берёт его из PGPASSWORD или ~/.pgpass.
set -u
cd "$(dirname "$0")/.."
ROOT="$(pwd)"

PG_HOST=${PG_HOST:-localhost}
PG_PORT=${PG_PORT:-5432}
PG_DB=${PG_DB:-hotelbooking}
PG_USER=${PG_USER:-$USER}
PRIMARY_SCHEMA=${PRIMARY_SCHEMA:-riot_primary}
REPLICA_SCHEMA=${REPLICA_SCHEMA:-riot_replica}

LOGS="$ROOT/docs/logs/lr7"
mkdir -p "$LOGS"

dotnet build -v q --nologo > /dev/null || exit 1

SERVER="$ROOT/Server/bin/Debug/net10.0/Server.dll"
CLIENT="$ROOT/Client/bin/Debug/net10.0/Client.dll"

PORT=${PORT:-5377}
RESERVE_DELAY=${RESERVE_DELAY:-200}
ROOM=${ROOM:-101}
ROOM2=${ROOM2:-102}

# Кластер узлов: N штук на портах NODE_BASE..NODE_BASE+N-1 и таймаут heartbeat.
NODES=${NODES:-5}
NODE_BASE=${NODE_BASE:-5411}
HEARTBEAT=${HEARTBEAT:-500}
NODE_LAST=$((NODE_BASE + NODES - 1))

SERVER_PID=""

# Свободными должны быть и порт сервера, и порты всех узлов, и порт, на который клиент сценария
# ошибок заведомо стучится: иначе «узел недоступен» в протоколе станет правдой про другую программу.
for checked_port in "$PORT" 5999 $(seq "$NODE_BASE" "$NODE_LAST"); do
  if lsof -nP -iTCP:"$checked_port" -sTCP:LISTEN > /dev/null 2>&1; then
    echo "Порт $checked_port занят. Свободный набор: PORT=5377 NODE_BASE=5411 ./scripts/run_lr7_experiments.sh"
    exit 1
  fi
done

stop_server() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null
    wait "$SERVER_PID" 2>/dev/null
  fi
  SERVER_PID=""
}
trap stop_server EXIT

PG_CONN="Host=$PG_HOST;Port=$PG_PORT;Database=$PG_DB;Username=$PG_USER${PGPASSWORD:+;Password=$PGPASSWORD}"
export PGPASSWORD

psql_db() { psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" "$@" 2>&1; }

# Схемы создаёт сервер при запуске, поэтому перед первым прогоном части они просто удаляются.
reset_cluster() {
  psql_db -q -v ON_ERROR_STOP=1 -c "DROP SCHEMA IF EXISTS $PRIMARY_SCHEMA CASCADE;
                                    DROP SCHEMA IF EXISTS $REPLICA_SCHEMA CASCADE;" > /dev/null || {
    echo "Не удалось очистить PostgreSQL-базу '$PG_DB' на $PG_HOST:$PG_PORT. Создайте её: createdb $PG_DB"
    exit 1
  }
}

# $1 = файл журнала сервера, $2 = reset|keep.
start_server() {
  local log="$1" mode="$2"
  [[ "$mode" == "reset" ]] && reset_cluster

  dotnet "$SERVER" --port "$PORT" --reserve-delay "$RESERVE_DELAY" \
    --storage postgres --replication on --pubsub on --cluster on \
    --primary-schema "$PRIMARY_SCHEMA" --replica-schema "$REPLICA_SCHEMA" \
    --primary-conn "$PG_CONN" --replica-conn "$PG_CONN" \
    --event-delay "Logger=1000" --event-delay "RoomService=3000" \
    --event-delay "NotificationService=2000" --event-retries 3 \
    --nodes "$NODES" --cluster-port "$NODE_BASE" --heartbeat-timeout "$HEARTBEAT" > "$log" 2>&1 &
  SERVER_PID=$!

  sleep 4
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "Сервер не запустился ($log):"
    tail -n 25 "$log"
    exit 1
  fi
}

run_scenario() {   # $1 = имя сценария, $2 = файл журнала клиента, $3 = заголовок прогона, [$4 = номер комнаты]
  echo "$3"
  local room=${4:-$ROOM}
  dotnet "$CLIENT" --port "$PORT" --name "Client" --scenario "$1" --room "$room" > "$2" 2>&1
}

# Состояние предметной области: статусы броней, платежи и доступность номеров. Нужно доказать,
# что без большинства узлов запись не происходит, а после консенсуса - происходит.
domain_state() {
  psql_db -c "SELECT id, room_id, guest_id, status FROM $PRIMARY_SCHEMA.bookings ORDER BY id;"
  psql_db -c "SELECT COUNT(*) AS payments FROM $PRIMARY_SCHEMA.payments;"
  psql_db -c "SELECT id, room_number, is_available FROM $PRIMARY_SCHEMA.rooms ORDER BY id;"
  psql_db -q -t -A -c "SELECT 'bookings hash = '||COALESCE(md5(string_agg(id||'|'||room_id||'|'||status, '' ORDER BY id)), 'empty')
                       FROM $PRIMARY_SCHEMA.bookings;"
}

echo "[1/11] Топология кластера: $NODES узлов на портах $NODE_BASE-$NODE_LAST, большинство и лидер"
start_server "$LOGS/01-server-cluster.log" reset
run_scenario cluster-status "$LOGS/01-cluster-status.log" "Состояние кластера узлов"
stop_server

echo "[2/11] Части 1-2: пять узлов, состояния, роли и выбор лидера"
start_server "$LOGS/02-server-nodes.log" keep
run_scenario cluster-nodes "$LOGS/02-part1-2-nodes.log" "Части 1-2: узлы и Leader election"
stop_server

echo "[3/11] Части 3-4: heartbeat, отказ лидера, обнаружение и повторный выбор"
start_server "$LOGS/03-server-failure.log" keep
run_scenario cluster-failure "$LOGS/03-part3-4-failure.log" "Части 3-4: heartbeat и повторный выбор лидера"
stop_server

echo "[4/11] Часть 5: предложение BookingStatus, голоса узлов и большинство"
start_server "$LOGS/04-server-vote.log" keep
run_scenario cluster-vote "$LOGS/04-part5-vote.log" "Часть 5: голосование и консенсус"
stop_server

echo "[5/11] Часть 6: эксперименты с отказами 5/5, 4/5, 3/5 и 2/5 узлов"
start_server "$LOGS/05-server-experiments.log" keep
run_scenario cluster-experiments "$LOGS/05-part6-experiments.log" "Часть 6: эксперименты с отказами"
stop_server

echo "[6/11] Требование 2: ситуации ошибок кластера"
start_server "$LOGS/06-server-errors.log" keep
run_scenario cluster-errors "$LOGS/06-errors.log" "Обработка ошибок кластера"
stop_server

echo "[7/11] Итоговый прогон всех частей подряд"
start_server "$LOGS/07-server-final.log" keep
run_scenario cluster-final "$LOGS/07-final-run.log" "Все части ЛР №7 одним прогоном"
stop_server

echo "[8/11] Регрессия ЛР №6: события предметной области работают рядом с консенсусом"
start_server "$LOGS/08-server-pubsub.log" keep
run_scenario pubsub-basic "$LOGS/08-pubsub-regression.log" "Pub/Sub поверх кластера: Publisher → Broker → Subscriber"
stop_server

echo "[9/11] Два клиента одновременно инициируют предложения"
start_server "$LOGS/09-server-two-clients.log" reset
dotnet "$CLIENT" --port "$PORT" --name "Client A" --scenario cluster-clients --room "$ROOM" \
  > "$LOGS/09a-client-a.log" 2>&1 &
CLIENT_A=$!
dotnet "$CLIENT" --port "$PORT" --name "Client B" --scenario cluster-clients --room "$ROOM2" \
  > "$LOGS/09b-client-b.log" 2>&1 &
CLIENT_B=$!
wait "$CLIENT_A" "$CLIENT_B"
{
  echo "--- предложения каждого клиента ---"
  cat "$LOGS/09a-client-a.log" "$LOGS/09b-client-b.log"
  echo "--- имена клиентов в журнале сервера ---"
  grep -o "\[[0-9:.]*\] \[Client [AB]\]" "$LOGS/09-server-two-clients.log" | sort | uniq -c
  echo "--- раунды голосования сервера ---"
  grep -E "PROPOSE|CONSENSUS ACHIEVED|CONSENSUS NOT ACHIEVED" "$LOGS/09-server-two-clients.log"
} > "$LOGS/09-two-clients.log" 2>&1
stop_server

echo "[10/11] Большинство не собрано: решение не принимается и ничего не записывается"
start_server "$LOGS/10-server-no-majority.log" reset
{
  echo "--- состояние предметной области до попыток ---"
  domain_state
} > "$LOGS/10-no-majority-proof.log" 2>&1
run_scenario cluster-refused "$LOGS/10a-refused-vote.log" "Голосование без большинства"
{
  echo ""
  echo "--- состояние предметной области после отказанных попыток (снимок psql) ---"
  domain_state
} >> "$LOGS/10-no-majority-proof.log" 2>&1
stop_server

echo "[11/11] Большинство собрано: согласованный BookingStatus записан"
start_server "$LOGS/11-server-applied.log" keep
run_scenario cluster-vote "$LOGS/11a-applied-vote.log" "Консенсус достигнут и значение применено"
{
  echo "--- состояние предметной области после консенсуса (снимок psql) ---"
  domain_state
  echo "--- строки сервера, где консенсус применяется к данным ---"
  grep -E "CONSENSUS ACHIEVED|DOMAIN" "$LOGS/11-server-applied.log" | tail -n 12
} > "$LOGS/11-applied-proof.log" 2>&1
stop_server

echo "[доп.] Ключевые строки журнала кластера по всем прогонам"
{
  for marker in "is listening on" "Election started" "elected as LEADER" "HEARTBEAT" "NO RESPONSE" \
                "FAILED" "role LEADER" "PROPOSE" "→ YES" "→ NO" "Majority =" "CONSENSUS ACHIEVED" \
                "CONSENSUS NOT ACHIEVED" "COMMIT" "DOMAIN" "ERROR"; do
    echo "--- $marker ---"
    grep -h "$marker" "$LOGS"/*server*.log 2>/dev/null | head -n 8
  done
} > "$LOGS/12-journal-digest.log" 2>&1

echo ""
echo "Готово. Логи и снимки данных: $LOGS"
