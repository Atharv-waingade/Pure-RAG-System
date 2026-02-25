#!/usr/bin/env bash
# Exit on error
set -o errexit

# 1. Install dependencies
pip install -r requirements.txt

# 2. Download and FORCE SAVE the model to 'model_cache'
# This flattens the folder structure so the app can find config.json immediately.
echo "Downloading and saving AI model..."
python -c "from sentence_transformers import SentenceTransformer; model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2'); model.save('./model_cache')"