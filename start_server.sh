#!/bin/bash
# Start the Web3DB backend server in a detached tmux session
# Each branch gets its own tmux session so multiple servers can run simultaneously

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPT_DIR="$REPO_DIR/app/scripts"
BRANCH="$(git -C "$REPO_DIR" branch --show-current)"
SESSION_NAME="web3db-${BRANCH}"

# Kill existing session for THIS branch only
tmux kill-session -t "$SESSION_NAME" 2>/dev/null

# Determine the port (mirrors app.py logic) and free it if occupied
PORT="${PORT:-8000}"
fuser -k "${PORT}/tcp" 2>/dev/null && sleep 1

LOG_FILE="$REPO_DIR/logs/${SESSION_NAME}.log"
mkdir -p "$REPO_DIR/logs"

# Start detached, tee output to log file
tmux new-session -d -s "$SESSION_NAME" "cd $SCRIPT_DIR && python3 -u app.py 2>&1 | tee '$LOG_FILE'"

echo "✓ Server started in detached tmux session '$SESSION_NAME'"
echo "  → Branch:        $BRANCH"
echo "  → View logs:     ./view_logs.sh  (Ctrl+C to stop)"
echo "  → Log file:      $LOG_FILE"