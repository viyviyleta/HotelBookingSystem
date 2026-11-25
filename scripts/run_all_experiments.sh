
set -u
cd "$(dirname "$0")/.."
ROOT="$(pwd)"
LOGS="$ROOT/docs/logs"
mkdir -p "$LOGS"

dotnet build -v q --nologo > /dev/null || exit 1

SERVER="$ROOT/Server/bin/Debug/net10.0/Server.dll"
SEMAPHORE_SERVER="$ROOT/Server-SemaphoreSlim/bin/Debug/net10.0/Server-SemaphoreSlim.dll"
CLIENT="$ROOT/Client/bin/Debug/net10.0/Client.dll"

PORT=${PORT:-5045}
RESERVE_DELAY=${RESERVE_DELAY:-4000}   # окно гонки: проверка → изменение (мс)

stop_server() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null
    wait "$SERVER_PID" 2>/dev/null
  fi
  SERVER_PID=""
}
trap stop_server EXIT

start_server() {   # $1 = сборка сервера, $2 = файл журнала, остальное = аргументы
  local bin="$1" log="$2"
  shift 2
  dotnet "$bin" "$@" > "$log" 2>&1 &
  SERVER_PID=$!
  sleep 1
}

two_clients() {    # $1 = файл журнала клиентов
  local log="$1"
  (
    dotnet "$CLIENT" --port "$PORT" --name "Client 1" --offset 7 \
      --scenario reserve --room 101 > "$LOGS/client1.tmp" 2>&1 &
    local p1=$!
    dotnet "$CLIENT" --port "$PORT" --name "Client 2" --offset -5 \
      --scenario reserve --room 101 > "$LOGS/client2.tmp" 2>&1 &
    local p2=$!
    wait $p1 $p2
  )
  {
    echo "--- Client 1 ---"; cat "$LOGS/client1.tmp"
    echo "--- Client 2 ---"; cat "$LOGS/client2.tmp"
  } > "$log"
  rm -f "$LOGS/client1.tmp" "$LOGS/client2.tmp"
}

echo "[1/5] Race Condition без синхронизации"
start_server "$SERVER" "$LOGS/01-server-no-sync.log" --port "$PORT" --sync none --reserve-delay "$RESERVE_DELAY"
two_clients "$LOGS/01-clients-no-sync.log"
stop_server

echo "[2/5] Два клиента с lock"
start_server "$SERVER" "$LOGS/02-server-lock.log" --port "$PORT" --sync lock --reserve-delay "$RESERVE_DELAY"
two_clients "$LOGS/02-clients-lock.log"
stop_server

echo "[3/5] Два клиента с SemaphoreSlim"
start_server "$SEMAPHORE_SERVER" "$LOGS/03-server-semaphore.log" --port "$PORT" --sync semaphore --reserve-delay "$RESERVE_DELAY"
two_clients "$LOGS/03-clients-semaphore.log"
stop_server

echo "[4/5] Синхронизация времени (смещение часов клиента +7 с, задержки 0/1000/3000 мс)"
start_server "$SERVER" "$LOGS/04-server-time.log" --port "$PORT" --sync lock --reserve-delay 200
dotnet "$CLIENT" --port "$PORT" --name "Client 1" --offset 7 --scenario time --delays 0,1000,3000 \
  > "$LOGS/04-client-time.log" 2>&1

echo "[5/5] Обработка ошибок"
dotnet "$CLIENT" --port "$PORT" --name "Client 1" --offset 7 --scenario errors \
  > "$LOGS/05-client-errors.log" 2>&1
stop_server

rm -f "$LOGS"/*.tmp
echo "Логи: $LOGS"
