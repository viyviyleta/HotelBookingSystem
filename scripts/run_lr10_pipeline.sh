
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"
LOGS="$ROOT/docs/logs/lr10"
mkdir -p "$LOGS"

BUILD_NUMBER="${BUILD_NUMBER:-10}"
IMAGE_NAME="${IMAGE_NAME:-riot-server}"
IMAGE_TAG="${BUILD_NUMBER}"
DOCKERFILE="${DOCKERFILE:-Server/Dockerfile}"
DEPLOYMENT="${DEPLOYMENT:-riot-server}"
CONTAINER="${CONTAINER:-server}"
NAMESPACE="${NAMESPACE:-default}"
SOLUTION="${SOLUTION:-HotelBookingSystem.slnx}"
SERVER_PROJECT="${SERVER_PROJECT:-Server/Server.csproj}"
TESTS_PROJECT="${TESTS_PROJECT:-Tests/Tests.csproj}"

if [[ "${SIMULATE_DEPLOY_FAILURE:-0}" == "1" ]]; then
  DEPLOYMENT="riot-server-missing"
fi

log_step() {
  echo ""
  echo "============================================================"
  echo "$1"
  echo "============================================================"
}

run_stage() { # $1 = имя этапа, $2 = файл журнала, далее команда
  local name="$1" log="$2"
  shift 2
  log_step "STAGE: ${name}"
  set +e
  "$@" 2>&1 | tee "$log"
  local code=${PIPESTATUS[0]}
  set -e
  if [[ $code -ne 0 ]]; then
    echo ""
    echo "PIPELINE FAILED на этапе ${name} (код ${code}). Последующие этапы не выполняются."
    echo "Журнал: ${log}"
    exit $code
  fi
}

# ------------------------------------------------------------------ 0. Окружение
{
  echo "date: $(date)"
  echo "dotnet: $(dotnet --version)"
  echo "git: $(git --version)"
  echo "docker: $(docker --version)"
  echo "kubectl: $(kubectl version --client 2>&1 | head -n 1)"
  echo "build number: ${BUILD_NUMBER}"
  echo "image: ${IMAGE_NAME}:${IMAGE_TAG}"
  echo "deployment: ${DEPLOYMENT}/${CONTAINER} in namespace ${NAMESPACE}"
} > "$LOGS/00-versions.log" 2>&1

# ---------------------------------------------------------------- 1. Checkout
checkout_stage() {
  echo "=== Checkout ==="
  if git rev-parse --is-inside-work-tree > /dev/null 2>&1; then
    echo "branch: $(git rev-parse --abbrev-ref HEAD)"
    echo "commit: $(git rev-parse HEAD)"
    git log -1 --oneline
    echo "--- git status ---"
    git status --short
  else
    echo "Каталог не является Git-репозиторием; используется текущий рабочий каталог."
  fi
  echo "--- исходники ---"
  ls -1 HotelBookingSystem.slnx Server/Server.csproj Tests/Tests.csproj Server/Dockerfile
}
run_stage "Checkout" "$LOGS/01-checkout.log" checkout_stage

# ----------------------------------------------------------------- 2. Restore
restore_stage() {
  echo "=== Restore ==="
  dotnet --version
  dotnet restore "$SOLUTION"
}
run_stage "Restore" "$LOGS/02-restore.log" restore_stage

# ------------------------------------------------------------------- 3. Build
build_stage() {
  echo "=== Build (Release) ==="
  dotnet build "$SOLUTION" --configuration Release --no-restore
}
run_stage "Build" "$LOGS/03-build.log" build_stage

# -------------------------------------------------------------------- 4. Test
test_stage() {
  echo "=== Test ==="
  rm -rf TestResults
  dotnet test "$TESTS_PROJECT" \
    --configuration Release \
    --no-build \
    --results-directory TestResults \
    --logger "trx;LogFileName=lr10.trx"
}
run_stage "Test" "$LOGS/04-test.log" test_stage

# ----------------------------------------------------------------- 5. Package
package_stage() {
  echo "=== Package ==="
  docker build -f "$DOCKERFILE" -t "${IMAGE_NAME}:${IMAGE_TAG}" .
  docker image inspect "${IMAGE_NAME}:${IMAGE_TAG}" \
    --format 'IMAGE={{.RepoTags}} ID={{.Id}} SIZE={{.Size}}'
}
run_stage "Package" "$LOGS/05-package.log" package_stage

# ------------------------------------------------------------------ 6. Deploy
deploy_stage() {
  echo "=== Deploy ==="
  kubectl config current-context
  kubectl config set-context --current --namespace="$NAMESPACE" > /dev/null 2>&1 || true
  kubectl set image "deployment/${DEPLOYMENT}" "${CONTAINER}=${IMAGE_NAME}:${IMAGE_TAG}" --namespace="$NAMESPACE"
  kubectl rollout status "deployment/${DEPLOYMENT}" --namespace="$NAMESPACE" --timeout=180s
  kubectl get deployment "$DEPLOYMENT" --namespace="$NAMESPACE"
  kubectl get pods -l "app=${DEPLOYMENT}" --namespace="$NAMESPACE" -o wide
}
run_stage "Deploy" "$LOGS/06-deploy.log" deploy_stage

# ----------------------------------------------- 7. Проверка через Service
{
  echo "=== Итоговое состояние ==="
  kubectl get deployment,service,pods -n "$NAMESPACE" -o wide
  echo ""
  echo "=== Ожидаемый образ ==="
  kubectl get deployment "$DEPLOYMENT" -n "$NAMESPACE" \
    -o jsonpath='{.spec.template.spec.containers[?(@.name=="'"$CONTAINER"'")].image}{"\n"}'
  echo ""
  echo "=== rollout history ==="
  kubectl rollout history "deployment/${DEPLOYMENT}" -n "$NAMESPACE" | tail -n 8
} > "$LOGS/07-final-state.log" 2>&1

if [[ "${VERIFY:-0}" == "1" ]]; then
  log_step "Проверка приложения через Service (port-forward 15377 -> 5377)"
  kubectl port-forward svc/riot-server 15377:5377 -n "$NAMESPACE" > "$LOGS/08-port-forward.log" 2>&1 &
  PF_PID=$!
  trap 'kill ${PF_PID:-} 2>/dev/null || true' EXIT
  for _ in $(seq 1 30); do
    if grep -q "Forwarding from" "$LOGS/08-port-forward.log" 2>/dev/null; then break; fi
    sleep 1
  done
  dotnet "$ROOT/Client/bin/Release/net10.0/Client.dll" \
    --host 127.0.0.1 --port 15377 --name "LR10 Client" \
    --scenario docker-operations --room 101 \
    > "$LOGS/09-client-operations.log" 2>&1 || true
  kill "$PF_PID" 2>/dev/null || true
  wait "$PF_PID" 2>/dev/null || true
  trap - EXIT

  {
    echo "=== Клиентские операции через Service ==="
    grep -E "SUCCESS|FAIL|Target:|LR5\]|DB (before|after)" "$LOGS/09-client-operations.log" | head -n 30
    echo ""
    echo "=== Журнал нового Pod ==="
    POD="$(kubectl get pods -l "app=riot-server" -n "$NAMESPACE" \
      --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1:].metadata.name}')"
    kubectl logs "$POD" -n "$NAMESPACE" | grep -E "\[SERVER\]|Running on port" | head -n 12
  } > "$LOGS/10-service-verification.log" 2>&1
fi

log_step "PIPELINE SUCCESS: ${IMAGE_NAME}:${IMAGE_TAG} развёрнут"
echo "Журналы: $LOGS"
