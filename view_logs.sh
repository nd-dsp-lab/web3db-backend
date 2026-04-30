#!/bin/bash
# Stream the Web3DB server log (Ctrl+C to stop, server keeps running)

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
BRANCH="$(git -C "$REPO_DIR" branch --show-current)"
SESSION_NAME="web3db-${BRANCH}"
LOG_FILE="$REPO_DIR/logs/${SESSION_NAME}.log"

if ! tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
    echo "✗ No running session '$SESSION_NAME' found."
    echo "  Start the server first: ./start_server.sh"
    exit 1
fi

if [ ! -f "$LOG_FILE" ]; then
    echo "✗ Log file not found: $LOG_FILE"
    echo "  Restart the server to enable file logging: ./start_server.sh"
    exit 1
fi

echo "Streaming logs for '$SESSION_NAME' (Ctrl+C to stop)..."
tail -f "$LOG_FILE"
