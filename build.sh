#!/usr/bin/env bash
# Exit on error
set -o errexit

# 1. Install dependencies
pip install -r requirements.txt

# 2. Download the LITE model (L3-v2) to 'model_cache'
# This is the smallest official model (60MB) that fits in Free Tier RAM.
echo "Downloading Lite AI model..."
python -c "from sentence_transformers import SentenceTransformer; model = SentenceTransformer('sentence-transformers/paraphrase-MiniLM-L3-v2'); model.save('./model_cache')"