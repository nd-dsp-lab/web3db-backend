#!/bin/bash
# Reattach to the Web3DB server tmux session to view logs

SESSION_NAME="web3db"

if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
    echo "Reattaching to '$SESSION_NAME'... (Ctrl+B, then D to detach again)"
    tmux attach -t "$SESSION_NAME"
else
    echo "✗ No running session '$SESSION_NAME' found."
    echo "  Start the server first: ./start_server.sh"
fi
