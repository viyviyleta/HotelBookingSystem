#!/usr/bin/env bash
# Прогон всех экспериментов ЛР №4: конкурентный доступ к базе данных и транзакции.
#
# База — PostgreSQL. Состояние таблиц после каждого эксперимента дампится утилитой самой
# СУБД (psql), поэтому цифры в отчёте — содержимое базы, а не утверждение приложения.
#
#   createdb hotelbooking                         # один раз, база не создаётся сама
#   ./scripts/run_lr4_experiments.sh
#   caffeinate -i ./scripts/run_lr4_experiments.sh   # на ноутбуке, чтобы macOS не уснул в середине
#
# Переменные окружения: PG_HOST, PG_PORT, PG_DB, PG_USER, PGPASSWORD,
#                       PORT, RESERVE_DELAY, RACE_WINDOW.
# Пароль в файле не хранится: libpq берёт его из PGPASSWORD или ~/.pgpass.
set -u
cd "$(dirname "$0")/.."
ROOT="$(pwd)"

PG_HOST=${PG_HOST:-localhost}
PG_PORT=${PG_PORT:-5432}
PG_DB=${PG_DB:-hotelbooking}
PG_USER=${PG_USER:-$USER}

LOGS="$ROOT/docs/logs/lr4"
mkdir -p "$LOGS"

dotnet build -v q --nologo > /dev/null || exit 1

SERVER="$ROOT/Server/bin/Debug/net10.0/Server.dll"
CLIENT="$ROOT/Client/bin/Debug/net10.0/Client.dll"

PORT=${PORT:-5256}
RESERVE_DELAY=${RESERVE_DELAY:-1200}   # задержка до входа в критическую секцию
RACE_WINDOW=${RACE_WINDOW:-2500}       # пауза между проверкой номера и записью (окно гонки)

SERVER_PID=""

# Порт может быть занят другим процессом на машине — тогда сервер падает с
# SocketException(48) и эксперимент даёт пустые логи. Проверяем до прогона.
if lsof -nP -iTCP:"$PORT" -sTCP:LISTEN > /dev/null 2>&1; then
  echo "Порт $PORT занят. Укажите свободный: PORT=5256 ./scripts/run_lr4_experiments.sh"
  exit 1
fi

stop_server() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null
    wait "$SERVER_PID" 2>/dev/null
  fi
  SERVER_PID=""
}
trap stop_server EXIT

# Одна конфигурация в двух форматах: ADO.NET-строка для сервера и опции для psql,
# который строку вида «Host=…;Port=…» не понимает.
PG_CONN="Host=$PG_HOST;Port=$PG_PORT;Database=$PG_DB;Username=$PG_USER${PGPASSWORD:+;Password=$PGPASSWORD}"
export PGPASSWORD

psql_db() {
  psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" "$@"
}

# Перед каждым экспериментом схема очищается, чтобы прогон начинался с пустой базы.
reset_database() {
  psql_db -q -v ON_ERROR_STOP=1 \
    -c "SET client_min_messages TO warning" \
    -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;" > /dev/null || {
    echo "Не удалось очистить PostgreSQL-базу '$PG_DB' на $PG_HOST:$PG_PORT. Создайте её: createdb $PG_DB"
    exit 1
  }
}

# $1 = файл журнала сервера, остальное = аргументы режима (--sync, --tx)
start_server() {
  local log="$1"
  shift
  reset_database
  dotnet "$SERVER" --port "$PORT" --reserve-delay "$RESERVE_DELAY" --race-window "$RACE_WINDOW" \
    --storage postgres --db-conn "$PG_CONN" "$@" > "$log" 2>&1 &
  SERVER_PID=$!
  sleep 2
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "Сервер не запустился ($log):"
    tail -n 15 "$log"
    exit 1
  fi
}

# Состояние общих таблиц напрямую из базы — независимая проверка результата.
dump_db() {   # $1 = подпись
  echo ""
  echo "--- psql dump: $1 ---"
  psql_db -c "SELECT id, room_number, type, price_per_night, is_available FROM rooms ORDER BY id;" \
          -c "SELECT id, room_id, guest_id, total_amount, status FROM bookings ORDER BY id;" \
          -c "SELECT id, booking_id, amount, is_successful FROM payments ORDER BY id;" \
          -c "SELECT room_id, COUNT(*) AS active_bookings FROM bookings WHERE status <> 'Cancelled'
                GROUP BY room_id HAVING COUNT(*) > 1;"
}

two_clients() {   # $1 = общий файл журнала, $2 = номер комнаты
  local log="$1" room="$2"
  (
    dotnet "$CLIENT" --port "$PORT" --name "Client 1" --offset 7 \
      --scenario reserve --room "$room" > "$LOGS/.c1.tmp" 2>&1 &
    local p1=$!
    dotnet "$CLIENT" --port "$PORT" --name "Client 2" --offset -5 \
      --scenario reserve --room "$room" > "$LOGS/.c2.tmp" 2>&1 &
    local p2=$!
    wait $p1 $p2
  )
  {
    echo "--- Client 1 ---"; cat "$LOGS/.c1.tmp"
    echo "--- Client 2 ---"; cat "$LOGS/.c2.tmp"
  } > "$log"
  rm -f "$LOGS/.c1.tmp" "$LOGS/.c2.tmp"
}

echo "[1/6] Конкурентный доступ к БД без lock и без транзакции (ожидание: продажа одного номера дважды)"
start_server "$LOGS/01-server-no-lock.log" --sync none --tx off
two_clients "$LOGS/01-clients-no-lock.log" 101
dump_db "после гонки без синхронизации" >> "$LOGS/01-clients-no-lock.log"
stop_server

echo "[2/6] Конкурентный доступ к БД с lock и транзакцией (ожидание: один SUCCESS, один FAIL)"
start_server "$LOGS/02-server-lock.log" --sync lock --tx on
two_clients "$LOGS/02-clients-lock.log" 101
dump_db "после гонки с lock" >> "$LOGS/02-clients-lock.log"
stop_server

echo "[3/6] Два клиента из одного процесса поверх БД (barrier-старт)"
start_server "$LOGS/03-server-two-clients.log" --sync lock --tx on
dotnet "$CLIENT" --port "$PORT" --name "Client" --scenario db-race --room 101 --clients 2 \
  > "$LOGS/03-db-race.log" 2>&1
dump_db "после гонки двух клиентов из одного процесса" >> "$LOGS/03-db-race.log"
stop_server

echo "[4/6] Четыре эксперимента с составной операцией (COMMIT / ROLLBACK / частичное изменение)"
start_server "$LOGS/04-server-transactions.log" --sync lock --tx on
dotnet "$CLIENT" --port "$PORT" --name "Client" --scenario tx --room 101 \
  > "$LOGS/04-transactions.log" 2>&1
dump_db "после всех экспериментов с транзакцией" >> "$LOGS/04-transactions.log"
stop_server

echo "[5/6] Обработка ошибок базы данных"
start_server "$LOGS/05-server-errors.log" --sync lock --tx on
dotnet "$CLIENT" --port "$PORT" --name "Client" --scenario db-errors --room 101 \
  > "$LOGS/05-db-errors.log" 2>&1
dump_db "после проверок ошибок" >> "$LOGS/05-db-errors.log"
stop_server

echo "[6/6] База данных недоступна (сервер не должен падать)"
# Та же проверка, что и для живой базы, только подключение ведёт к несуществующей базе.
dotnet "$SERVER" --port "$PORT" --storage postgres --reserve-delay 50 \
  --db-conn "Host=$PG_HOST;Port=$PG_PORT;Database=${PG_DB}-missing;Username=$PG_USER" \
  > "$LOGS/06-server-db-down.log" 2>&1 &
SERVER_PID=$!
sleep 2
dotnet "$CLIENT" --port "$PORT" --name "Client" --scenario db-down --room 101 \
  > "$LOGS/06-db-down.log" 2>&1
stop_server

echo "[доп.] Структура созданной базы данных глазами PostgreSQL"
{
  echo "--- psql \\d+: таблицы схемы public ---"
  for table in hotels rooms guests bookings payments; do
    psql_db -c "\\d+ public.$table"
  done
  echo "--- pg_constraint: ключи и ограничения целостности ---"
  psql_db -c "SELECT t.relname AS table_name, c.conname AS constraint_name, c.contype AS kind,
                     c.confrelid::regclass AS references_table, c.condeferrable AS deferrable
              FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid
              WHERE c.connamespace = 'public'::regnamespace
              ORDER BY t.relname, c.contype, c.conname;"
} > "$LOGS/07-database-structure.log" 2>&1

echo ""
echo "Готово. Логи и дампы: $LOGS"
