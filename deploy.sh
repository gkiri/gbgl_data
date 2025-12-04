#!/bin/bash
# deploy.sh - Deployment script for AWS EC2 instance

set -e

echo "=========================================="
echo "Gabagool Bot - AWS Deployment Script"
echo "=========================================="

# Check if we're on the server
if [ ! -d "/home/ubuntu" ]; then
    echo "❌ This script should be run on the AWS EC2 instance"
    exit 1
fi

BOT_DIR="/home/ubuntu/polymarket-gabagool-bot"

echo ""
echo "[1/5] Creating bot directory..."
mkdir -p $BOT_DIR
cd $BOT_DIR

echo ""
echo "[2/5] Installing system dependencies..."
sudo apt-get update
sudo apt-get install -y python3 python3-pip python3-venv git

echo ""
echo "[3/5] Setting up Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

echo ""
echo "[4/5] Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo ""
echo "[5/5] Setting up systemd services..."
sudo tee /etc/systemd/system/gabagool-bot.service > /dev/null <<EOF
[Unit]
Description=Polymarket Gabagool Bot
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=$BOT_DIR
Environment="PATH=$BOT_DIR/venv/bin"
ExecStart=$BOT_DIR/venv/bin/python $BOT_DIR/main.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

sudo tee /etc/systemd/system/gabagool-btc.service > /dev/null <<EOF
[Unit]
Description=Polymarket Gabagool Bot (BTC)
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=$BOT_DIR
Environment="PATH=$BOT_DIR/venv/bin"
ExecStart=$BOT_DIR/venv/bin/python $BOT_DIR/main_btc.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload

echo ""
echo "=========================================="
echo "✅ Deployment complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Copy your .env file to: $BOT_DIR/.env"
echo "2. Start the ETH bot: sudo systemctl start gabagool-bot"
echo "3. (Optional) Start the BTC bot: sudo systemctl start gabagool-btc"
echo "4. Check status: sudo systemctl status gabagool-bot"
echo "5. View logs: sudo journalctl -u gabagool-bot -f"
echo "6. Enable auto-start: sudo systemctl enable gabagool-bot (and gabagool-btc if needed)"
echo ""

