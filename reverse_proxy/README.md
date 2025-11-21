# Reverse SSH Tunnel Docker Container

This Docker container runs an autossh reverse SSH tunnel that forwards traffic from a remote server to a local service.

## Features

- **Multiple tunnels**: Configure multiple tunnels in a single YAML file
- **Automatic nginx configuration**: Generates and deploys nginx config on remote server
- **Automatic reconnection**: Uses `autossh` to automatically reconnect if the SSH connection drops
- **Persistent**: Container runs continuously and restarts automatically
- **Configurable**: YAML-based configuration for easy tunnel management
- **Individual logging**: Each tunnel has its own log file for easier debugging

## Prerequisites

- Docker and Docker Compose installed
- SSH private key for accessing the remote server
- Remote server must allow SSH connections and port forwarding

## Server Setup

Before running the reverse SSH tunnel container, you need to set up the remote server. Use the `set-server.sh` script to configure the server automatically.

The script configures SSH GatewayPorts and optionally installs/configures nginx. Use `--no-nginx` flag to skip nginx setup.

## Quick Start

1. **Set up the remote server** (run this on your remote server):
   ```bash
   # Copy the setup script to your server
   scp set-server.sh user@your-server:/tmp/
   
   # SSH into your server and run the setup
   ssh user@your-server
   sudo bash /tmp/set-server.sh
   ```

2. **Copy your SSH key to the build context:**
   ```bash
   cp ~/.ssh/ec2.pem ./ec2.pem
   chmod 600 ./ec2.pem
   ```

3. **Configure your tunnels in `tunnel.yml`:**
   ```yaml
   server:
     REMOTE_HOST: your-host.com
     REMOTE_USER: your-username
   
   nginx:
     site_config_file_name: reverse_proxy
     server_name: your-host.com
   
   tunnels:
     app1:
       local_port: 5000
       remote_port: 8000
     app2:
       local_port: 4999
       remote_port: 8090
   ```

4. **Build and start the container:**
   ```bash
   docker compose build
   docker compose up -d
   ```

5. **View logs:**
   ```bash
   # View all container logs
   docker compose logs -f
   
   # View individual tunnel logs
   tail -f logs/tunnel_app1.log
   tail -f logs/tunnel_app2.log
   ```

6. **Stop the container:**
   ```bash
   docker compose down
   ```

## Configuration

### Tunnel Configuration File (`tunnel.yml`)

The tunnel configuration is defined in `tunnel.yml`:

```yaml
server:
  REMOTE_HOST: your-host.com
  REMOTE_USER: your-username

nginx:
  site_config_file_name: reverse_proxy
  server_name: your-host.com

tunnels:
  app1:
    local_port: 5000
    remote_port: 8000
  
  app2:
    local_port: 4999
    remote_port: 8090
```

- **server**: Server configuration
  - `REMOTE_HOST`: Remote server hostname or IP (required)
  - `REMOTE_USER`: SSH username (default: `ubuntu`)
  
- **nginx**: Nginx configuration (optional)
  - `site_config_file_name`: Name of the nginx site configuration file (will be created in `/etc/nginx/sites-available/` with a symlink to `/etc/nginx/sites-enabled`)
  - `server_name`: Server name for nginx config (defaults to `REMOTE_HOST` if not specified)
  - If configured, the container will automatically generate and deploy nginx config
  - Each tunnel becomes a location block (e.g., `/app1/` → `http://127.0.0.1:8000/`)
  
- **tunnels**: Define multiple tunnels
  - Each tunnel has a unique name (e.g., `app1`, `app2`)
  - `local_port`: Port on local machine to forward from
  - `remote_port`: Port on remote server to forward to

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TUNNEL_CONFIG` | `/app/tunnel.yml` | Path to tunnel configuration file |
| `SSH_KEY_PATH` | `/app/ssh/ec2.pem` | Path to SSH private key in container |
| `TARGET_HOST` | `localhost` | Target host for port forwarding |
| `LOG_DIR` | `/app/logs` | Directory for tunnel logs |

### Build Arguments

- **SSH_KEY_FILE**: Filename of the SSH key in the build context (default: `ec2.pem`)
  - The key file must be copied to the build context directory before building

### Volume Mounts

- **Logs**: Mount a logs directory for persistent log storage
  - Each tunnel has its own log file: `tunnel_<name>.log`

## How It Works

The container:
1. Reads the `tunnel.yml` configuration file
2. **If nginx configuration is specified:**
   - Generates nginx config dynamically with location blocks for each tunnel
   - Creates location blocks for each tunnel (e.g., `/tunnel_name/` → `http://127.0.0.1:remote_port/`)
   - Copies the config file to `/etc/nginx/sites-available/` and creates a symlink in `/etc/nginx/sites-enabled/`
   - Tests and reloads nginx service
3. For each tunnel defined, starts a separate `autossh` process that:
   - Establishes an SSH connection to the remote server
   - Sets up reverse port forwarding (`-R`) from `remote_port` on the remote server to `local_port` on the local machine
   - Automatically reconnects if the connection drops
   - Logs all activity to `tunnel_<name>.log`
4. Monitors all tunnels and exits if all tunnels fail

## Network Configuration

The container uses `host` network mode to access services running on the host machine. If you need to access a service in another Docker container, you can:

1. Use `host.docker.internal` (already configured)
2. Or change `network_mode` to `bridge` and use the service's container name

## Troubleshooting

### Port forwarding fails

- Check that the remote port is not already in use
- Verify AWS security group allows inbound traffic on `REMOTE_PORT`
- Check SSH key permissions: `chmod 600 ~/.ssh/ec2.pem`
- Ensure `set-server.sh` has been run on the remote server to configure SSH GatewayPorts

### Container exits immediately

- Check logs: `docker compose logs`
- Verify `REMOTE_HOST` is set correctly
- Ensure SSH key is mounted correctly

### Connection drops frequently

- Check network connectivity
- Verify remote server SSH configuration allows port forwarding
- Review logs for specific error messages

### Nginx configuration update fails

- Ensure `set-server.sh` has been run on the remote server to install and configure nginx
- Ensure the SSH user has sudo privileges on the remote server
- Verify nginx is installed on the remote server
- Check that the nginx config file path is writable (may need sudo)
- Review container logs for specific SSH/SCP errors
- The container will continue with tunnel setup even if nginx update fails
- If you see errors about `server_names_hash_bucket_size`, ensure the script was run to configure it

## Example: Forwarding Multiple Services

If you have multiple services running locally:

```yaml
server:
  REMOTE_HOST: your-host.com
  REMOTE_USER: your-username

nginx:
  site_config_file_name: reverse_proxy
  server_name: your-host.com

tunnels:
  app1:
    local_port: 5000
    remote_port: 8000
  
  app2:
    local_port: 4999
    remote_port: 8090
```

With nginx configured, access your services via:
- App1: `http://your-host.com/app1/`
- App2: `http://your-host.com/app2/`

Without nginx (direct port access):
- App1: `http://your-host.com:8000`
- App2: `http://your-host.com:8090`

## Security Notes

- **SSH keys are copied into the Docker image** - be aware that keys are embedded in the image
- Consider using Docker secrets or mounted volumes for production deployments
- The container disables host key checking for automation (use with trusted hosts only)
- **Never commit SSH keys to version control** - they are excluded via `.gitignore`
- For production, consider using Docker secrets or environment-based key management

