#!/bin/bash
# EdgeRunner VPS Setup Script
# Run this on a fresh Ubuntu 22.04+ DigitalOcean droplet
#
# Usage:
#   1. Create a $6/mo droplet (1 vCPU, 1GB RAM, Ubuntu 22.04)
#   2. SSH in: ssh root@YOUR_DROPLET_IP
#   3. Upload this repo (or git clone)
#   4. Run: bash deploy/setup.sh
#   5. Copy your .env and keys/ to the server
#   6. Start: sudo systemctl start edgerunner

set -e

echo "=== EdgeRunner VPS Setup ==="
echo ""

# System updates
echo "[1/6] Updating system..."
apt-get update -qq && apt-get upgrade -y -qq

# Install Python 3.11+
echo "[2/6] Installing Python..."
apt-get install -y -qq python3 python3-pip python3-venv git

# Create edgerunner user (don't run as root)
echo "[3/6] Creating edgerunner user..."
if ! id "edgerunner" &>/dev/null; then
    useradd -m -s /bin/bash edgerunner
fi

# Set up the project
PROJ_DIR="/home/edgerunner/app"
echo "[4/6] Setting up project at $PROJ_DIR..."

if [ -d "$PROJ_DIR" ]; then
    echo "  Project directory exists, updating..."
else
    mkdir -p "$PROJ_DIR"
fi

# Copy current directory to project (if running from repo)
cp -r . "$PROJ_DIR/" 2>/dev/null || true
chown -R edgerunner:edgerunner /home/edgerunner

# Install Python dependencies
echo "[5/6] Installing Python dependencies..."
sudo -u edgerunner bash -c "
    cd $PROJ_DIR
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -q --upgrade pip
    pip install -q -r requirements.txt
"

# Create systemd service
echo "[6/6] Creating systemd service..."
cat > /etc/systemd/system/edgerunner.service << 'EOF'
[Unit]
Description=EdgeRunner Trading Agent
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=edgerunner
Group=edgerunner
WorkingDirectory=/home/edgerunner/app
Environment=PYTHONIOENCODING=utf-8
ExecStart=/home/edgerunner/app/.venv/bin/python runner.py
Restart=always
RestartSec=30

# Safety limits
MemoryMax=512M
CPUQuota=80%

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=edgerunner

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable edgerunner

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Next steps:"
echo "  1. Copy your .env file:     scp .env root@YOUR_IP:/home/edgerunner/app/.env"
echo "  2. Copy your keys:          scp -r keys/ root@YOUR_IP:/home/edgerunner/app/keys/"
echo "  3. Fix permissions:          chown -R edgerunner:edgerunner /home/edgerunner/app"
echo "  4. Start the agent:          sudo systemctl start edgerunner"
echo "  5. Check status:             sudo systemctl status edgerunner"
echo "  6. View live logs:           sudo journalctl -u edgerunner -f"
echo ""
echo "Useful commands:"
echo "  Stop:                        sudo systemctl stop edgerunner"
echo "  Restart:                     sudo systemctl restart edgerunner"
echo "  View last 100 lines:         sudo journalctl -u edgerunner -n 100"
echo "  Check if running:            sudo systemctl is-active edgerunner"
echo ""
