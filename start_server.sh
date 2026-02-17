#!/bin/bash
# Start the Web3DB backend server in a detached tmux session
# Each branch gets its own tmux session so multiple servers can run simultaneously

SCRIPT_DIR="$(cd "$(dirname "$0")/app/scripts" && pwd)"
BRANCH="$(git -C "$(dirname "$0")" branch --show-current)"
SESSION_NAME="web3db-${BRANCH}"

# Kill existing session for THIS branch only
tmux kill-session -t "$SESSION_NAME" 2>/dev/null

# Start detached
tmux new-session -d -s "$SESSION_NAME" "cd $SCRIPT_DIR && python3 app.py"

echo "✓ Server started in detached tmux session '$SESSION_NAME'"
echo "  → Branch:        $BRANCH"
echo "  → View logs:     tmux attach -t $SESSION_NAME"
echo "  → Detach again:  Ctrl+B, then D"