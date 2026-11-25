#!/usr/bin/env bash
# Прогон экспериментов ЛР №6: Pub/Sub - Publisher, Broker, Subscriber.
#
# Механизм событий реализован средствами C# (Kafka, RabbitMQ и ActiveMQ по заданию не нужны):
# сервер поднимает два Broker-а и четырёх подписчиков флагом --pubsub on. Предметная область
# остаётся той же, что в ЛР №4 и ЛР №5, - PostgreSQL, схемы riot_primary и riot_replica.
#
# Задержки подписчиков берутся из методички: Logger = 1 с, RoomService = 3 с, NotificationService = 2 с.
# Каждая часть проверяется на свежем сервере, поэтому в журнале видно и создание топологии, и опыт.
#
#   createdb hotelbooking                             # один раз, база не создаётся сама
#   ./scripts/run_lr6_experiments.sh
#   caffeinate -i ./scripts/run_lr6_experiments.sh    # чтобы macOS не уснул в середине прогона
#
# Переменные окружения: PG_HOST, PG_PORT, PG_DB, PG_USER, PGPASSWORD, PORT, RESERVE_DELAY, ROOM,
#                       PRIMARY_SCHEMA, REPLICA_SCHEMA, LOGGER_DELAY, ROOM_DELAY, NOTIFY_DELAY, RETRIES.
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

LOGS="$ROOT/docs/logs/lr6"
mkdir -p "$LOGS"

dotnet build -v q --nologo > /dev/null || exit 1

SERVER="$ROOT/Server/bin/Debug/net10.0/Server.dll"
CLIENT="$ROOT/Client/bin/Debug/net10.0/Client.dll"

PORT=${PORT:-5366}
RESERVE_DELAY=${RESERVE_DELAY:-300}
ROOM=${ROOM:-101}

# Искусственные задержки из Части 3 и число попыток доставки из Части 5.
LOGGER_DELAY=${LOGGER_DELAY:-1000}
ROOM_DELAY=${ROOM_DELAY:-3000}
NOTIFY_DELAY=${NOTIFY_DELAY:-2000}
RETRIES=${RETRIES:-3}

SERVER_PID=""

# Порт сервера и порт, на который клиент сценария ошибок заведомо стучится, должны быть свободны:
# иначе «сервер недоступен» в протоколе станет правдой про другую программу.
for checked_port in "$PORT" 5999; do
  if lsof -nP -iTCP:"$checked_port" -sTCP:LISTEN > /dev/null 2>&1; then
    echo "Порт $checked_port занят. Укажите свободный: PORT=5366 ./scripts/run_lr6_experiments.sh"
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

# Ошибки и уведомления psql идут в stderr, а они и есть доказательства: собираем их в общий поток.
psql_db() { psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" "$@" 2>&1; }

# Схемы создаёт сервер при запуске, поэтому перед экспериментом они просто удаляются.
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
    --storage postgres --replication on --pubsub on \
    --primary-schema "$PRIMARY_SCHEMA" --replica-schema "$REPLICA_SCHEMA" \
    --primary-conn "$PG_CONN" --replica-conn "$PG_CONN" \
    --event-delay "Logger=$LOGGER_DELAY" \
    --event-delay "RoomService=$ROOM_DELAY" \
    --event-delay "NotificationService=$NOTIFY_DELAY" \
    --event-retries "$RETRIES" > "$log" 2>&1 &
  SERVER_PID=$!

  sleep 3
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "Сервер не запустился ($log):"
    tail -n 20 "$log"
    exit 1
  fi
}

run_scenario() {   # $1 = имя сценария, $2 = файл журнала клиента, $3 = заголовок прогона
  echo "$3"
  dotnet "$CLIENT" --port "$PORT" --name "Client" --scenario "$1" --room "$ROOM" > "$2" 2>&1
}

# Отпечаток данных предметной области: счётчики и хеш строк каждой таблицы. Нужен, чтобы показать,
# что подписчики события только читают, а таблицы бронирования не меняют.
domain_fingerprint() {
  psql_db -q -t -A -c "SELECT 'Primary.bookings = '||COUNT(*)
                       ||' (active = '||COUNT(*) FILTER (WHERE status <> 'Cancelled')||')'
                       ||', hash = '||COALESCE(md5(string_agg(id||'|'||room_id||'|'||status, '' ORDER BY id)), 'empty')
                       FROM $PRIMARY_SCHEMA.bookings;"
  psql_db -q -t -A -c "SELECT 'Primary.payments = '||COUNT(*)
                       ||', hash = '||COALESCE(md5(string_agg(id||'|'||booking_id||'|'||amount, '' ORDER BY id)), 'empty')
                       FROM $PRIMARY_SCHEMA.payments;"
  psql_db -q -t -A -c "SELECT 'Primary.rooms = '||COUNT(*)
                       ||' (busy = '||COUNT(*) FILTER (WHERE NOT is_available)||')'
                       ||', hash = '||COALESCE(md5(string_agg(id||'|'||is_available::text, '' ORDER BY id)), 'empty')
                       FROM $PRIMARY_SCHEMA.rooms;"
  psql_db -q -t -A -c "SELECT 'Replica.bookings hash = '
                       ||COALESCE(md5(string_agg(id||'|'||room_id||'|'||status, '' ORDER BY id)), 'empty')
                       FROM $REPLICA_SCHEMA.bookings;"
}

echo "[1/11] Топология Pub/Sub: Publisher, два Broker-а и четыре Subscriber-а"
start_server "$LOGS/01-server-topology.log" reset
run_scenario pubsub-status "$LOGS/01-pubsub-status.log" "Состояние механизма событий"
stop_server

echo "[2/11] Часть 1: Publisher → Broker → Subscriber"
start_server "$LOGS/02-server-part1.log" keep
run_scenario pubsub-basic "$LOGS/02-part1-basic.log" "Часть 1: базовая модель Pub/Sub"
stop_server

echo "[3/11] Часть 2: несколько подписчиков, три типа событий, доставка только интересующих"
start_server "$LOGS/03-server-part2.log" keep
run_scenario pubsub-subscribers "$LOGS/03-part2-subscribers.log" "Часть 2: подписки и фильтрация событий"
stop_server

echo "[4/11] Часть 3: асинхронная обработка, отказ подписчика, очередь и повторная доставка"
start_server "$LOGS/04-server-part3.log" keep
run_scenario pubsub-failure "$LOGS/04-part3-failure.log" "Часть 3: независимая обработка и отказ подписчика"
stop_server

echo "[5/11] Часть 4: новый подписчик во время работы сервера"
start_server "$LOGS/05-server-part4-new.log" keep
run_scenario pubsub-new "$LOGS/05-part4-new-subscriber.log" "Часть 4: добавление Subscriber без остановки сервера"
stop_server

echo "[6/11] Часть 4: сравнение прямого взаимодействия и Pub/Sub, полный цикл"
start_server "$LOGS/06-server-part4-cycle.log" keep
run_scenario pubsub-cycle "$LOGS/06-part4-cycle.log" "Часть 4: цикл событий и сравнение способов связи"
stop_server

echo "[7/11] Часть 5: несколько Broker-ов, аналитика, три клиента одновременно, времена доставки"
start_server "$LOGS/07-server-part5.log" keep
run_scenario pubsub-brokers "$LOGS/07-part5-brokers.log" "Часть 5: два Broker-а, AnalyticsService и задержки"
stop_server

echo "[8/11] Часть 4, требование 5: тринадцать ситуаций ошибок"
start_server "$LOGS/08-server-errors.log" keep
run_scenario pubsub-errors "$LOGS/08-part4-errors.log" "Обработка ошибок Pub/Sub"
stop_server

echo "[9/11] Итоговый прогон всех частей подряд"
start_server "$LOGS/09-server-final.log" keep
run_scenario pubsub-final "$LOGS/09-final-cycle.log" "Все части ЛР №6 одним прогоном"
stop_server

echo "[10/11] Два клиента работают одновременно: события каждого идут под своим идентификатором"
start_server "$LOGS/10-server-two-clients.log" reset
dotnet "$CLIENT" --port "$PORT" --name "Client A" --scenario pubsub-basic --room "$ROOM" \
  > "$LOGS/10a-client-a.log" 2>&1 &
CLIENT_A=$!
dotnet "$CLIENT" --port "$PORT" --name "Client B" --scenario pubsub-basic --room "$ROOM" \
  > "$LOGS/10b-client-b.log" 2>&1 &
CLIENT_B=$!
wait "$CLIENT_A" "$CLIENT_B"
{
  echo "--- идентификаторы клиентов в журнале сервера ---"
  grep -o "\[[0-9:.]*\] \[Client [AB]\]" "$LOGS/10-server-two-clients.log" | sort | uniq -c
  echo "--- события каждого клиента ---"
  grep -E "\[Client [AB]\] → \[EVENT\]" "$LOGS/10-server-two-clients.log"
} > "$LOGS/10-two-clients.log" 2>&1
stop_server

echo "[11/11] Подписчики не меняют данные предметной области: отпечаток баз и после событий"
start_server "$LOGS/11-server-domain-proof.log" reset
{
  echo "--- отпечаток данных до публикации событий ---"
  domain_fingerprint
} > "$LOGS/11-domain-data-proof.log" 2>&1
run_scenario pubsub-basic "$LOGS/11a-events-only.log" "События публикуются, операции бронирования не выполняются"
{
  echo ""
  echo "--- отпечаток данных после того, как все подписчики отработали события ---"
  domain_fingerprint
  echo "--- содержимое таблиц предметной области ---"
  psql_db -c "SELECT id, room_number, is_available FROM $PRIMARY_SCHEMA.rooms ORDER BY id;" \
          -c "SELECT id, room_id, guest_id, status FROM $PRIMARY_SCHEMA.bookings ORDER BY id;" \
          -c "SELECT COUNT(*) AS payments FROM $PRIMARY_SCHEMA.payments;"
} >> "$LOGS/11-domain-data-proof.log" 2>&1
stop_server

echo "[доп.] Ключевые строки журнала сервера по всем прогонам"
{
  for marker in "\[PUBLISHER\] PUBLISH" "EVENT RECEIVED" "\[SUBSCRIBER\].*REGISTERED" "SUCCESS" \
                "UNAVAILABLE" "CONNECTION" "ATTEMPT\|Attempt" "QUEUE\|Redelivery" "\[STATS\]"; do
    echo "--- $marker ---"
    grep -h "$marker" "$LOGS"/*server*.log 2>/dev/null | head -n 8
  done
} > "$LOGS/12-journal-digest.log" 2>&1

echo ""
echo "Готово. Логи и дампы: $LOGS"
