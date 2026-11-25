#!/usr/bin/env bash
# Прогон экспериментов ЛР №8: контейнеризация распределённого приложения.
#
# Проект тот же, что в ЛР №4-№7: клиент и сервер на C# (.NET 10) говорят по TCP+JSON,
# данные лежат в PostgreSQL (Primary + Replica), работают Pub/Sub и кластер узлов ЛР №7.
# Новое здесь - Docker: серверный образ, образ клиента, официальный образ PostgreSQL,
# пользовательская сеть riot-net, переменные окружения и совместный запуск через compose.yaml.
#
#   cp .env.example .env && nano .env        # задать DB_PASSWORD
#   ./scripts/run_lr8_experiments.sh
#   caffeinate -i ./scripts/run_lr8_experiments.sh   # чтобы macOS не уснул в середине прогона
#
# Скрипт сам собирает образы, поднимает compose, выполняет операции предметной области,
# нагрузочное тестирование, ошибки, остановку и повторный запуск контейнеров, а затем
# запускает то же приложение без Docker для сравнения. Результаты - в docs/logs/lr8/.
set -u
cd "$(dirname "$0")/.."
ROOT="$(pwd)"
LOGS="$ROOT/docs/logs/lr8"
mkdir -p "$LOGS"
rm -f "$LOGS"/*.log "$LOGS"/*.json "$LOGS"/*.yaml

export COMPOSE_PROJECT_NAME=riot-lr8

compose() { docker compose "$@"; }

log_step() {
  echo ""
  echo "============================================================"
  echo "$1"
  echo "============================================================"
}

require_docker() {
  if ! docker info > /dev/null 2>&1; then
    echo "Docker daemon не запущен. Запустите Docker Desktop и повторите."
    exit 1
  fi
}

if [[ ! -f "$ROOT/.env" ]]; then
  cp "$ROOT/.env.example" "$ROOT/.env"
  echo "Создан .env из .env.example. Проверьте DB_PASSWORD и при необходимости измените порты."
fi

require_docker

wait_for_db() {
  local status=""
  for _ in $(seq 1 60); do
    status=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' riot-db 2>/dev/null)
    [[ "$status" == "healthy" ]] && return 0
    sleep 1
  done
  echo "База riot-db не стала healthy (status=$status)"
  docker logs --tail 40 riot-db 2>&1
  return 1
}

wait_for_server() {
  for _ in $(seq 1 60); do
    if docker inspect -f '{{.State.Running}}' riot-server 2>/dev/null | grep -q true; then
      if docker logs riot-server 2>&1 | grep -q "Running on port"; then
        return 0
      fi
    fi
    sleep 1
  done
  echo "Сервер riot-server не сообщил о готовности"
  docker logs --tail 40 riot-server 2>&1
  return 1
}

run_client() {   # $1 = имя сценария, $2 = файл журнала, остальные аргументы передаются клиенту
  local scenario="$1" log="$2"
  shift 2
  compose run --rm --no-deps client --scenario "$scenario" "$@" > "$log" 2>&1
  local code=$?
  echo "  client --scenario $scenario → код $code, журнал $log"
  return 0
}

save_container_logs() {   # $1 = суффикс
  docker logs riot-server > "$LOGS/90-server-$1.log" 2>&1
  docker logs riot-db > "$LOGS/91-db-$1.log" 2>&1
  compose ps -a > "$LOGS/92-ps-$1.log" 2>&1
}

# ---------------------------------------------------------------- 1. Подготовка

log_step "[1/10] Проверка окружения, сборка образов и проверка compose.yaml"

docker --version > "$LOGS/00-versions.log" 2>&1
docker compose version >> "$LOGS/00-versions.log" 2>&1
dotnet --version >> "$LOGS/00-versions.log" 2>&1

# Синтаксис проверяется с подстановкой .env, но в журнал пишется вариант без значений:
# иначе реальный пароль из .env попал бы в docs/logs/lr8/01-compose-config.yaml.
compose config --quiet > /dev/null 2> "$LOGS/01-compose-config-error.log" || {
  cat "$LOGS/01-compose-config-error.log"
  exit 1
}
compose config --no-interpolate > "$LOGS/01-compose-config.yaml" 2>&1

compose --profile client build > "$LOGS/02-compose-build.log" 2>&1
if [[ $? -ne 0 ]]; then
  echo "Сборка образов не удалась, см. $LOGS/02-compose-build.log"
  tail -n 40 "$LOGS/02-compose-build.log"
  exit 1
fi

docker images --format '{{.Repository}}:{{.Tag}}\t{{.ID}}\t{{.Size}}' \
  | grep -E 'riot-(server|client)|postgres' > "$LOGS/03-images.log" 2>&1

# ---------------------------------------------------------------- 2. Запуск

log_step "[2/10] Остановка старых контейнеров и запуск через docker compose up"

compose down --remove-orphans > "$LOGS/04-compose-down.log" 2>&1
compose up -d --build > "$LOGS/05-compose-up.log" 2>&1

wait_for_db || exit 1
wait_for_server || exit 1

compose ps > "$LOGS/06-compose-ps.log" 2>&1
# Пароль в inspect остаётся в окружении контейнера, но в журнал он не записывается.
docker inspect riot-db riot-server \
  | sed -E 's/(POSTGRES_PASSWORD|DB_PASSWORD)=[^"]*/\1=<redacted>/g' \
  > "$LOGS/07-inspect.json" 2>&1
docker network inspect riot-net > "$LOGS/08-network.json" 2>&1
docker volume inspect riot-db-data > "$LOGS/09-volume.json" 2>&1

# ------------------------------------------------- 3. Части 1-2: контейнеры и операции

log_step "[3/10] Части 1-2: запуск контейнеров, параметры окружения и операции"

run_client docker-operations "$LOGS/10-operations-in-network.log" --room 101
save_container_logs "after-operations"

# ------------------------------------------------------ 4. Часть 4: ошибки

log_step "[4/10] Часть 4: обработка ошибок контейнеризованного приложения"

run_client docker-errors "$LOGS/11-errors.log" --room 101
save_container_logs "after-errors"

# ------------------------------------------------ 5. Часть 4: нагрузка

log_step "[5/10] Часть 4: нагрузочное тестирование"

run_client docker-load "$LOGS/12-load.log" --clients 8 --requests 20
save_container_logs "after-load"

# ------------------------------ 6. Часть 4: остановка и повторный запуск сервера

log_step "[6/10] Часть 4: остановка контейнера сервера, отказ и повторный запуск"

run_client docker-operations "$LOGS/13a-before-stop.log" --room 101
run_client docker-state "$LOGS/13b-state-before-stop.log" --room 101

docker stop riot-server > "$LOGS/13c-stop-server.log" 2>&1
sleep 1
compose ps > "$LOGS/13d-ps-server-stopped.log" 2>&1
run_client docker-state "$LOGS/13e-client-when-server-stopped.log" --room 101
save_container_logs "server-stopped"

docker start riot-server > "$LOGS/13f-start-server.log" 2>&1
wait_for_server || exit 1
run_client docker-state "$LOGS/13g-state-after-restart.log" --room 101
run_client docker-operations "$LOGS/13h-operations-after-restart.log" --room 101
save_container_logs "server-restarted"

# ------------------------------------ 7. Часть 4: остановка базы и восстановление

log_step "[7/10] Часть 4: остановка контейнера базы, ошибки и восстановление"

docker stop riot-db > "$LOGS/14a-stop-db.log" 2>&1
sleep 1
compose ps > "$LOGS/14b-ps-db-stopped.log" 2>&1
run_client time "$LOGS/14c-server-alive-when-db-stopped.log" --delays 0
run_client docker-operations "$LOGS/14d-operations-when-db-stopped.log" --room 101
save_container_logs "db-stopped"

docker start riot-db > "$LOGS/14e-start-db.log" 2>&1
wait_for_db || exit 1
sleep 3
run_client docker-recover "$LOGS/14f-recovery-after-db-restart.log" --room 101
run_client docker-operations "$LOGS/14g-operations-after-db-restart.log" --room 101
run_client docker-state "$LOGS/14h-state-after-db-restart.log" --room 101
save_container_logs "db-restarted"

# ------------------------------------------- 8. Проверка сохранности данных в томе

log_step "[8/10] Часть 4: сохранность данных при пересоздании контейнеров"

run_client docker-operations "$LOGS/15a-data-before-recreate.log" --room 101
compose down > "$LOGS/15b-compose-down-before-recreate.log" 2>&1
compose up -d > "$LOGS/15c-compose-up-after-recreate.log" 2>&1
wait_for_db || exit 1
wait_for_server || exit 1
run_client docker-state "$LOGS/15d-state-after-recreate.log" --room 101
save_container_logs "after-recreate"

# ------------------------------------------------ 9. Журналы контейнеров

log_step "[9/10] Часть 4: журналы контейнеров"

{
  echo "--- заголовок журнала сервера ---"
  head -n 30 "$LOGS/90-server-after-operations.log"
  echo ""
  echo "--- строки о конфигурации и сети ---"
  grep -E "\[SERVER\] (pubsub|cluster|port|nodes|Pub/Sub|PostgreSQL)|INSTANCE|SYNC|SUBSCRIBER" \
    "$LOGS/90-server-after-operations.log" | head -n 30
  echo ""
  echo "--- ошибки, попавшие в журнал за все прогоны ---"
  grep -hE "FAIL|ERROR|WARNING|UNAVAILABLE|unreachable" "$LOGS"/90-server-*.log 2>/dev/null | head -n 30
  echo ""
  echo "--- журнал базы: готовность и остановка ---"
  grep -E "database system is ready|shutting down|shut down|received fast shutdown" "$LOGS/91-db-after-operations.log" | tail -n 10
} > "$LOGS/16-logs-digest.log" 2>&1

# -------------------------------- 10. Сравнение запуска без Docker и в Docker

log_step "[10/10] Часть 4: запуск того же приложения без Docker для сравнения"

HOST_PORT=${HOST_PORT:-5005}
HOST_NODE_BASE=${HOST_NODE_BASE:-5511}
HOST_PRIMARY=${HOST_PRIMARY:-riot_lr8_host_primary}
HOST_REPLICA=${HOST_REPLICA:-riot_lr8_host_replica}
PG_HOST=${PG_HOST:-localhost}
PG_PORT=${PG_PORT:-5432}
PG_DB=${PG_DB:-hotelbooking}
PG_USER=${PG_USER:-$USER}

SERVER_PID=""
stop_host_server() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null
    wait "$SERVER_PID" 2>/dev/null
  fi
  SERVER_PID=""
}
trap stop_host_server EXIT

dotnet build -v q --nologo > "$LOGS/20-host-build.log" 2>&1 || exit 1

PG_CONN="Host=$PG_HOST;Port=$PG_PORT;Database=$PG_DB;Username=$PG_USER"
dotnet "$ROOT/Server/bin/Debug/net10.0/Server.dll" \
  --port "$HOST_PORT" --reserve-delay 200 \
  --storage postgres --replication on --pubsub on --cluster on \
  --primary-schema "$HOST_PRIMARY" --replica-schema "$HOST_REPLICA" \
  --primary-conn "$PG_CONN" --replica-conn "$PG_CONN" \
  --nodes 5 --cluster-port "$HOST_NODE_BASE" --heartbeat-timeout 500 \
  > "$LOGS/21-host-server.log" 2>&1 &
SERVER_PID=$!

sleep 4
if ! kill -0 "$SERVER_PID" 2>/dev/null; then
  echo "Сервер без Docker не запустился, см. $LOGS/21-host-server.log"
  tail -n 30 "$LOGS/21-host-server.log"
  exit 1
fi

dotnet "$ROOT/Client/bin/Debug/net10.0/Client.dll" \
  --host 127.0.0.1 --port "$HOST_PORT" --name "Host Client" \
  --scenario docker-operations --room 101 > "$LOGS/22-host-operations.log" 2>&1

docker compose run --rm --no-deps client --scenario docker-operations --room 101 \
  > "$LOGS/23-docker-operations-for-comparison.log" 2>&1

stop_host_server

{
  echo "--- без Docker: сервер запущен локально, БД localhost:$PG_PORT, схемы $HOST_PRIMARY/$HOST_REPLICA ---"
  grep -E "\[SERVER\] (pubsub|cluster|port|nodes|PostgreSQL)|Running on port" "$LOGS/21-host-server.log" | head -n 10
  echo ""
  echo "--- без Docker: операции предметной области ---"
  grep -E "Target:|SUCCESS|FAIL|DB (before|after)|LR5\]|LR6\]|LR7\]" "$LOGS/22-host-operations.log" | head -n 25
  echo ""
  echo "--- в Docker: сервер запущен через compose, БД db:5432, схемы riot_primary/riot_replica ---"
  grep -E "\[SERVER\] (pubsub|cluster|port|nodes|PostgreSQL)|Running on port" "$LOGS/90-server-after-operations.log" | head -n 10
  echo ""
  echo "--- в Docker: операции предметной области ---"
  grep -E "Target:|SUCCESS|FAIL|DB (before|after)|LR5\]|LR6\]|LR7\]" "$LOGS/23-docker-operations-for-comparison.log" | head -n 25
} > "$LOGS/24-docker-vs-host.log" 2>&1

# ---------------------------------------------------------------- Итог

echo ""
echo "Готово. Журналы и снимки состояния Docker: $LOGS"
echo "Контейнеры оставлены запущенными. Остановить: docker compose down"
echo "Проверить код возврата клиентских сценариев можно в строках 'код N' выше."
