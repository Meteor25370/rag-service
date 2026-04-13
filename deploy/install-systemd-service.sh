#!/usr/bin/env bash
set -euo pipefail

# Install and enable rag-service as a systemd service from a stable local deploy dir.
# Usage:
#   bash deploy/install-systemd-service.sh

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
UNIT_DST="${HOME}/.config/systemd/user/rag-service.service"
RUN_USER="${USER}"
RUN_HOME="${HOME}"
DEPLOY_BASE="${RUN_HOME}/.rag-service"
DEPLOY_DIR="${DEPLOY_BASE}/app"
RUNTIME_DATA_DIR="${DEPLOY_BASE}/data"
SOURCE_DB="${ROOT_DIR}/rag-service.db"
RUNTIME_DB="${RUNTIME_DATA_DIR}/rag-service.db"

echo "[0/6] Prepare deploy directories"
mkdir -p "${DEPLOY_BASE}" "${DEPLOY_DIR}" "${RUNTIME_DATA_DIR}" "${HOME}/.config/systemd/user"

echo "[1/6] Sync code to ${DEPLOY_DIR}"
if command -v rsync >/dev/null 2>&1; then
  rsync -a --delete \
    --exclude ".git/" \
    --exclude ".venv/" \
    --exclude "__pycache__/" \
    --exclude "deploy/" \
    "${ROOT_DIR}/" "${DEPLOY_DIR}/"
else
  rm -rf "${DEPLOY_DIR}"
  mkdir -p "${DEPLOY_DIR}"
  cp -a "${ROOT_DIR}/." "${DEPLOY_DIR}/"
  rm -rf "${DEPLOY_DIR}/.git" "${DEPLOY_DIR}/.venv" "${DEPLOY_DIR}/deploy"
fi

if [[ ! -f "${RUNTIME_DB}" && -f "${SOURCE_DB}" ]]; then
  echo "[1b/6] Seed runtime DB from ${SOURCE_DB}"
  cp -f "${SOURCE_DB}" "${RUNTIME_DB}"
fi

echo "[2/6] Generate unit file -> ${UNIT_DST}"
cat > "${UNIT_DST}" <<EOF
[Unit]
Description=RAG Service Web UI + Background Ingestion
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=${DEPLOY_DIR}
Environment=PYTHONUNBUFFERED=1
Environment=RAG_SERVICE_HOME=${RUNTIME_DATA_DIR}
Environment=RAG_SERVICE_HOST=0.0.0.0
Environment=RAG_SERVICE_PORT=6334
ExecStart=/usr/bin/python3 ${DEPLOY_DIR}/server.py
Restart=always
RestartSec=3
KillMode=control-group
KillSignal=SIGINT
FinalKillSignal=SIGKILL
SendSIGKILL=yes
TimeoutStopSec=10

[Install]
WantedBy=default.target
EOF

echo "[3/6] Reload systemd"
systemctl --user daemon-reload

echo "[4/6] Enable service"
systemctl --user enable rag-service.service

echo "[5/6] Restart service"
systemctl --user reset-failed rag-service.service || true
systemctl --user restart rag-service.service

echo "[6/6] Deployed from ${ROOT_DIR} to ${DEPLOY_DIR}"
echo "Runtime data dir: ${RUNTIME_DATA_DIR}"

echo "Service status:"
systemctl --user --no-pager --full status rag-service.service
