#!/bin/bash

exec > /var/log/bootstrap.log 2>&1

echo "=== EMR Bootstrap Starting ==="
date

# Kill any existing ollama processes
sudo pkill -9 ollama 2>/dev/null || true

# Install Ollama
echo "Installing Ollama..."
curl -fsSL https://ollama.com/install.sh | sh

# Configure Ollama service
echo "Configuring Ollama service..."
sudo tee /etc/systemd/system/ollama.service > /dev/null <<'EOF'
[Unit]
Description=Ollama Service
After=network.target

[Service]
Type=simple
User=root
ExecStart=/usr/bin/env OLLAMA_HOST=0.0.0.0:11434 /usr/local/bin/ollama serve
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

# Start Ollama
sudo systemctl daemon-reload
sudo systemctl enable ollama
sudo systemctl start ollama

sleep 10

# Pull model
echo "Pulling mxbai-embed-large model..."
/usr/local/bin/ollama pull mxbai-embed-large

# Verify
/usr/local/bin/ollama list
sudo netstat -tlnp | grep 11434

echo "=== Bootstrap Complete ==="
date
exit 0