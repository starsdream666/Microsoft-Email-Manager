#!/bin/sh
set -e

HOST=${HOST:-"0.0.0.0"}
PORT=${PORT:-8000}
WORKERS=${WORKERS:-1}
DATA_DIR=${DATA_DIR:-/app/data}
ACCOUNTS_FILE=${ACCOUNTS_FILE:-$DATA_DIR/accounts.json}

mkdir -p "$DATA_DIR"

if [ -d "$ACCOUNTS_FILE" ]; then
    echo "Accounts path is a directory, expected a file: $ACCOUNTS_FILE" >&2
    exit 1
fi

if [ ! -f "$ACCOUNTS_FILE" ]; then
    echo "{}" > "$ACCOUNTS_FILE"
fi

chown appuser:appuser "$ACCOUNTS_FILE" 2>/dev/null || true
chown -R appuser:appuser "$DATA_DIR" 2>/dev/null || true
# 数据目录含明文刷新令牌/密码哈希/会话令牌，收紧权限为仅属主可访问。
chmod 700 "$DATA_DIR" 2>/dev/null || true
chmod 600 "$ACCOUNTS_FILE" 2>/dev/null || true

echo "Starting Outlook Email API service..."
echo "Configuration:"
echo "  - Host: $HOST"
echo "  - Port: $PORT"
echo "  - Workers: $WORKERS"
echo "  - Data dir: $DATA_DIR"
echo "  - Accounts file: $ACCOUNTS_FILE"

# 以非 root 用户 appuser 启动应用（容器以 root 启动仅用于修正挂载卷属主，随后立即降权）。
if command -v su-exec >/dev/null 2>&1; then
    exec su-exec appuser:appuser python main.py
fi
exec python main.py
