#!/bin/bash
set -euo pipefail
set -x

# 1) Install Ollama
curl -fsSL https://ollama.com/install.sh | sudo sh

# 2) Start Ollama (detached) and log to /var/log
nohup /usr/local/bin/ollama serve > /var/log/ollama.log 2>&1 &

# 3) Wait for API
echo "Waiting for Ollama to start..."
for i in {1..20}; do
  if curl -s http://127.0.0.1:11434/api/tags >/dev/null 2>&1; then
    echo "✅ Ollama is up!"
    break
  fi
  echo "⏳ Not ready... retry $i/20"
  sleep 5
done

# 4) Pull required model with retries
MODEL="mxbai-embed-large"
for i in {1..5}; do
  if /usr/local/bin/ollama pull "$MODEL"; then
    echo "✅ Pulled $MODEL"
    break
  fi
  echo "⏳ Pull failed, retry $i/5 ..."
  sleep 10
done

# 5) Verify
curl -s http://127.0.0.1:11434/api/tags || echo "⚠️ Ollama API not responding after install."