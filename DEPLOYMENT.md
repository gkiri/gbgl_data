# AWS Deployment Guide for Gabagool Bot

## Prerequisites

1. **AWS EC2 Instance** (Ubuntu 22.04 LTS recommended)
   - Instance Type: `c6i.large` or better (as per Technical Design Doc)
   - Region: `us-east-1` (N. Virginia) for lowest latency
   - Security Group: Allow SSH (port 22) from your IP

2. **SSH Access**
   - Your `.pem` key file: `poly-bot.pem`
   - Instance IP: `98.86.148.148` (from your earlier SSH session)

## Deployment Steps

### Step 1: Copy Files to AWS

From your **local machine**, run:

```bash
# Copy all bot files to AWS
scp -i "poly-bot.pem" -r \
    *.py \
    requirements.txt \
    README.md \
    .gitignore \
    ubuntu@98.86.148.148:~/polymarket-gabagool-bot/

# Copy deployment script
scp -i "poly-bot.pem" deploy.sh ubuntu@98.86.148.148:~/
```

### Step 2: SSH into AWS Instance

```bash
ssh -i "poly-bot.pem" ubuntu@98.86.148.148
```

### Step 3: Run Deployment Script

On the AWS instance:

```bash
cd ~
chmod +x deploy.sh
./deploy.sh
```

### Step 4: Configure Environment Variables

**CRITICAL**: Copy your `.env` file to the server:

```bash
# On your LOCAL machine:
scp -i "poly-bot.pem" .env ubuntu@98.86.148.148:~/polymarket-gabagool-bot/.env

# On AWS instance, verify:
cat ~/polymarket-gabagool-bot/.env
# Should show your PRIVATE_KEY, API_KEY, etc. (don't share this output!)
```

### Step 5: Start the Bot (ETH)

```bash
# Start the bot service
sudo systemctl start gabagool-bot

# Check if it's running
sudo systemctl status gabagool-bot

# View live logs
sudo journalctl -u gabagool-bot -f

# Enable auto-start on boot
sudo systemctl enable gabagool-bot
```

### Optional: Start the BTC Bot

```bash
# Start the BTC bot
sudo systemctl start gabagool-btc

# Check status
sudo systemctl status gabagool-btc

# View BTC logs
sudo journalctl -u gabagool-btc -f

# Enable auto-start
sudo systemctl enable gabagool-btc
```

### Optional: Run the Shadow Listener

Capture live fills for tuning:

```bash
cd ~/polymarket-gabagool-bot
source venv/bin/activate
python shadow_listener.py --market btc-updown-15m-1764871200 --output data/btc-trades.log &
```

Review `data/btc-trades.log` and update the adaptive knobs in `.env`.
### Step 6: Verify It's Working

Watch the logs for:
- `[Hot] Connecting to Websocket...`
- `[Cold] Fetching details for...`
- `[Hot] Subscribing to: [...]`
- No error messages

## Useful Commands

```bash
# Stop the ETH bot
sudo systemctl stop gabagool-bot

# Stop the BTC bot
sudo systemctl stop gabagool-btc

# Restart (ETH/BTC)
sudo systemctl restart gabagool-bot
sudo systemctl restart gabagool-btc

# View last 100 log lines
sudo journalctl -u gabagool-bot -n 100
sudo journalctl -u gabagool-btc -n 100

# View logs since boot
sudo journalctl -u gabagool-bot -b
sudo journalctl -u gabagool-btc -b

# Check if services are running
sudo systemctl is-active gabagool-bot
sudo systemctl is-active gabagool-btc
```

## Troubleshooting

### Bot won't start
- Check logs: `sudo journalctl -u gabagool-bot -n 50`
- Verify `.env` exists: `ls -la ~/polymarket-gabagool-bot/.env`
- Check Python: `python3 --version` (should be 3.10+)

### WebSocket connection fails
- Check network: `curl -I https://clob.polymarket.com`
- Verify security group allows outbound HTTPS/WSS
- Check if instance has internet access: `ping 8.8.8.8`

### No market data
- This is normal if markets are inactive
- Bot will continuously scan for active markets
- Check logs for `[Cold] Scanning for markets...`

## Security Notes

- ✅ `.env` file is in `.gitignore` (won't be committed)
- ✅ `.env` permissions should be `600` (only owner can read)
- ✅ Bot runs as `ubuntu` user (not root)
- ⚠️ Keep your `.pem` key secure and never commit it

## Next Steps After Deployment

1. Monitor logs for the first hour
2. Verify it finds and subscribes to markets
3. Check that it receives price updates
4. Monitor for any errors or connection issues

