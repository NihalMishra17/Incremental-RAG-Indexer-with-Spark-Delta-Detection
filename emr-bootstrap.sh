#!/bin/bash
set -e

echo "=========================================="
echo "EMR Bootstrap: Installing Ollama"
echo "=========================================="

# Update system
sudo yum update -y

# Install Ollama
echo "Installing Ollama..."
curl -fsSL https://ollama.com/install.sh | sh

# Start Ollama service
echo "Starting Ollama..."
sudo systemctl start ollama
sudo systemctl enable ollama

# Wait for Ollama to start
sleep 10

# Pull embedding model
echo "Pulling mxbai-embed-large model..."
ollama pull mxbai-embed-large

# Configure Ollama to accept all connections
echo "Configuring Ollama..."
sudo mkdir -p /etc/systemd/system/ollama.service.d
sudo tee /etc/systemd/system/ollama.service.d/override.conf > /dev/null <<EOC
[Service]
Environment="OLLAMA_HOST=0.0.0.0:11434"
EOC

# Restart Ollama
sudo systemctl daemon-reload
sudo systemctl restart ollama
sleep 5

# Verify
if curl -s http://localhost:11434/api/tags > /dev/null; then
    echo "✓ Ollama installed successfully!"
else
    echo "✗ Ollama installation failed!"
    exit 1
fi

echo "=========================================="
echo "Bootstrap completed!"
echo "=========================================="