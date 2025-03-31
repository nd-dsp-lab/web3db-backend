#!/bin/sh
# Initialize IPFS if not already initialized
if [ ! -f /data/ipfs/config ]; then
    echo "Initializing IPFS..."
    ipfs init
    
    # Add default bootstrap nodes to connect to public network
    ipfs bootstrap add --default
fi

# Configure IPFS to listen on all interfaces
ipfs config Addresses.API /ip4/0.0.0.0/tcp/5001
ipfs config Addresses.Gateway /ip4/0.0.0.0/tcp/8080
ipfs config Addresses.Swarm '["/ip4/0.0.0.0/tcp/4001", "/ip6/::/tcp/4001", "/ip4/0.0.0.0/udp/4001/quic", "/ip6/::/udp/4001/quic"]'

# Enable pubsub (useful for many distributed applications)
ipfs config --json Pubsub.Enabled true

# Start the daemon with public network connection enabled
exec ipfs daemon --migrate=true --enable-pubsub-experiment --enable-namesys-pubsub