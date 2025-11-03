#!/bin/bash

# Install Ollama
curl -fsSL https://ollama.com/install.sh | sh

# Configure systemd to listen on all interfaces
sudo mkdir -p /etc/systemd/system/ollama.service.d
sudo tee /etc/systemd/system/ollama.service.d/override.conf << 'EOF'
[Service]
Environment="OLLAMA_HOST=0.0.0.0:11434"
EOF

# Start Ollama service
sudo systemctl daemon-reload
sudo systemctl enable ollama
sudo systemctl start ollama

# Wait for service to be ready
sleep 10

# Pull model
ollama pull mxbai-embed-large

# Verify
netstat -tlnp | grep 11434
ollama list