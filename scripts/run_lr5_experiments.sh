#!/usr/bin/env bash
# Прогон экспериментов ЛР №5: репликация изменений, отказ Primary, Failover, восстановление.
#
# Копии данных — две схемы одной PostgreSQL-базы (riot_primary и riot_replica): содержимое у
# них независимое, но второй сервер и вторая база не нужны, а восстановление не требует createdb.
# Состояние обеих схем после каждого эксперимента дампируется утилитой самой СУБД (psql),
# поэтому цифры в отчёте — содержимое базы, а не утверждение приложения.
#
#   createdb hotelbooking                             # один раз, база не создаётся сама
#   ./scripts/run_lr5_experiments.sh
#   caffeinate -i ./scripts/run_lr5_experiments.sh    # чтобы macOS не уснул в середине прогона
#
# Переменные окружения: PG_HOST, PG_PORT, PG_DB, PG_USER, PGPASSWORD,
#                       PORT, RESERVE_DELAY, PRIMARY_SCHEMA, REPLICA_SCHEMA.
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

LOGS="$ROOT/docs/logs/lr5"
mkdir -p "$LOGS"

dotnet build -v q --nologo > /dev/null || exit 1

SERVER="$ROOT/Server/bin/Debug/net10.0/Server.dll"
CLIENT="$ROOT/Client/bin/Debug/net10.0/Client.dll"

PORT=${PORT:-5356}
RESERVE_DELAY=${RESERVE_DELAY:-300}
ROOM=${ROOM:-101}

SERVER_PID=""

# Порт сервера и порт, на который клиент сценария ошибок заведомо стучится, должны быть свободны:
# иначе «сервер недоступен» в протоколе станет правдой про другую программу.
for checked_port in "$PORT" 5999; do
  if lsof -nP -iTCP:"$checked_port" -sTCP:LISTEN > /dev/null 2>&1; then
    echo "Порт $checked_port занят. Укажите свободный: PORT=5356 ./scripts/run_lr5_experiments.sh"
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

# Одна конфигурация в двух форматах: ADO.NET-строка для сервера и опции для psql.
PG_CONN="Host=$PG_HOST;Port=$PG_PORT;Database=$PG_DB;Username=$PG_USER${PGPASSWORD:+;Password=$PGPASSWORD}"
export PGPASSWORD

# Строка подключения к несуществующей базе: через неё СУБД сама отказывает соединением (3D000).
MISSING_CONN="Host=$PG_HOST;Port=$PG_PORT;Database=${PG_DB}-missing;Username=$PG_USER"

# Ошибки и уведомления psql идут в stderr, а они и есть доказательства по двум проверкам:
# «relation does not exist» для физически отсутствующей копии. Собираем их в общий поток.
psql_db() { psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" "$@" 2>&1; }

# Обе схемы создаёт сервер при запуске, поэтому перед экспериментом они просто удаляются:
# прогон начинается с чистого кластера и без ручного createdb.
reset_cluster() {
  psql_db -q -v ON_ERROR_STOP=1 -c "DROP SCHEMA IF EXISTS $PRIMARY_SCHEMA CASCADE;
                                    DROP SCHEMA IF EXISTS $REPLICA_SCHEMA CASCADE;" > /dev/null || {
    echo "Не удалось очистить PostgreSQL-базу '$PG_DB' на $PG_HOST:$PG_PORT. Создайте её: createdb $PG_DB"
    exit 1
  }
}

# $1 = файл журнала сервера, $2 = reset|keep, остальное = дополнительные аргументы режима.
# Ключи, переданные дополнительно, перекрывают значения по умолчанию: CommandLine присваивает
# аргументы по порядку, поэтому последний --replica-conn выигрывает.
start_server() {
  local log="$1" mode="$2"
  shift 2
  [[ "$mode" == "reset" ]] && reset_cluster

  dotnet "$SERVER" --port "$PORT" --reserve-delay "$RESERVE_DELAY" \
    --storage postgres --replication on \
    --primary-schema "$PRIMARY_SCHEMA" --replica-schema "$REPLICA_SCHEMA" \
    --primary-conn "$PG_CONN" --replica-conn "$PG_CONN" "$@" > "$log" 2>&1 &
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
  dump_copies "$3" >> "$2"
}

# Содержимое обеих копий напрямую из базы: независимая проверка того, что видит приложение.
dump_copies() {   # $1 = подпись
  echo ""
  echo "--- psql dump: $1 ---"

  for schema in "$PRIMARY_SCHEMA" "$REPLICA_SCHEMA"; do
    echo "--- схема $schema ---"
    psql_db -c "SELECT id, room_number, type, price_per_night, is_available FROM $schema.rooms ORDER BY id;" \
            -c "SELECT id, room_id, guest_id, total_amount, status FROM $schema.bookings ORDER BY id;" \
            -c "SELECT id, booking_id, amount, is_successful FROM $schema.payments ORDER BY id;"
  done

  echo "--- сравнение копий глазами СУБД ---"
  psql_db -c "SELECT item, p_count AS \"primary\", r_count AS \"replica\", p_count = r_count AS match
              FROM (
                SELECT 1 AS ord, 'rooms busy' AS item,
                       (SELECT COUNT(*) FROM $PRIMARY_SCHEMA.rooms WHERE NOT is_available) AS p_count,
                       (SELECT COUNT(*) FROM $REPLICA_SCHEMA.rooms WHERE NOT is_available) AS r_count
                UNION ALL SELECT 2, 'bookings',
                       (SELECT COUNT(*) FROM $PRIMARY_SCHEMA.bookings),
                       (SELECT COUNT(*) FROM $REPLICA_SCHEMA.bookings)
                UNION ALL SELECT 3, 'bookings active',
                       (SELECT COUNT(*) FROM $PRIMARY_SCHEMA.bookings WHERE status <> 'Cancelled'),
                       (SELECT COUNT(*) FROM $REPLICA_SCHEMA.bookings WHERE status <> 'Cancelled')
                UNION ALL SELECT 4, 'payments',
                       (SELECT COUNT(*) FROM $PRIMARY_SCHEMA.payments),
                       (SELECT COUNT(*) FROM $REPLICA_SCHEMA.payments)
              ) t ORDER BY ord;"
}

echo "[1/8] Состояние кластера: две копии и штатный маршрут"
start_server "$LOGS/01-server-cluster.log" reset
dotnet "$CLIENT" --port "$PORT" --name "Client" --scenario repl-status > "$LOGS/01-cluster-status.log" 2>&1
dump_copies "начальное состояние кластера" >> "$LOGS/01-cluster-status.log"
stop_server

echo "[2/8] Часть 2: репликация изменений общего ресурса (бронь → оплата → отмена)"
start_server "$LOGS/02-server-replication.log" reset
run_scenario repl "$LOGS/02-replication.log" "Часть 2: репликация изменений"
stop_server

echo "[3/8] Части 3-4: отказ Primary, Failover и восстановление (один сервер, два прогона)"
start_server "$LOGS/03-server-failover.log" reset
run_scenario failover "$LOGS/03-failover.log" "Часть 3: отказ Primary и Failover"

# Recovery продолжает тот же отказ: Primary остаётся помеченный недоступным после Части 3.
echo "[4/8] Часть 4: восстановление Primary и синхронизация копий"
run_scenario recovery "$LOGS/04-recovery.log" "Часть 4: Recovery и синхронизация"
stop_server

echo "[5/8] Полный цикл Primary → Replica → Failover → Recovery (9 шагов)"
start_server "$LOGS/05-server-cycle.log" reset
run_scenario repl-cycle "$LOGS/05-cycle.log" "Полный цикл отказоустойчивости"
stop_server

echo "[6/8] Обработка ошибок репликации, отказа и некорректных запросов (13 ситуаций)"
start_server "$LOGS/06-server-errors.log" reset
run_scenario repl-errors "$LOGS/06-errors.log" "Ошибки репликации и отказа"
stop_server

echo "[7/8] Replica физически недоступна: настоящая ошибка соединения СУБД"
start_server "$LOGS/07-server-replica-down.log" reset --replica-conn "$MISSING_CONN"
dotnet "$CLIENT" --port "$PORT" --name "Client" --scenario repl-status > "$LOGS/07-replica-down.log" 2>&1
dump_copies "живой Primary и отсутствующая база Replica" >> "$LOGS/07-replica-down.log"
stop_server

echo "[8/8] Primary физически недоступен: запросы уходит к живой Replica"
# Replica обязана существовать и содержать данные, поэтому кластер сначала поднимается штатно.
start_server "$LOGS/08a-server-cluster.log" reset
stop_server
start_server "$LOGS/08-server-primary-down.log" keep --primary-conn "$MISSING_CONN"
run_scenario failover "$LOGS/08-primary-down.log" "Primary недоступен на уровне соединения"
stop_server

echo "[доп.] Структура двух копий глазами PostgreSQL"
{
  echo "--- psql \\d+: таблицы обеих схем ---"
  for schema in "$PRIMARY_SCHEMA" "$REPLICA_SCHEMA"; do
    for table in rooms bookings payments; do
      psql_db -c "\\d+ $schema.$table"
    done
  done
  echo "--- pg_constraint: ключи и ограничения целостности в обеих схемах ---"
  psql_db -c "SELECT n.nspname AS schema, t.relname AS table_name, c.conname AS constraint_name,
                     c.contype AS kind, c.confrelid::regclass AS references_table
              FROM pg_constraint c
              JOIN pg_class t ON t.oid = c.conrelid
              JOIN pg_namespace n ON n.oid = t.relnamespace
              WHERE n.nspname IN ('$PRIMARY_SCHEMA', '$REPLICA_SCHEMA')
              ORDER BY n.nspname, t.relname, c.contype, c.conname;"
  echo "--- списки схем кластера ---"
  psql_db -c "SELECT nspname FROM pg_namespace WHERE nspname LIKE 'riot%' ORDER BY nspname;"
} > "$LOGS/09-cluster-structure.log" 2>&1

echo ""
echo "Готово. Логи и дампы: $LOGS"
