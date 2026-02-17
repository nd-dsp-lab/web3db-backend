#!/bin/bash
# Start the Web3DB backend server in a detached tmux session

SESSION_NAME="web3db"
SCRIPT_DIR="$(cd "$(dirname "$0")/app/scripts" && pwd)"

# Kill existing session if running
tmux kill-session -t "$SESSION_NAME" 2>/dev/null

# Start detached with venv activated
VENV_DIR="$(cd "$(dirname "$0")" && pwd)/venv"
tmux new-session -d -s "$SESSION_NAME" "source $VENV_DIR/bin/activate && cd $SCRIPT_DIR && python3 app.py"

echo "✓ Server started in detached tmux session '$SESSION_NAME'"
echo "  → View logs:    ./view_logs.sh"
echo "  → Detach again:  Ctrl+B, then D"