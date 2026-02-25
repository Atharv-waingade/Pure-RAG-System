#!/usr/bin/env bash
# Exit on error
set -o errexit

# 1. Install dependencies
pip install -r requirements.txt

# 2. Download model to a SPECIFIC folder named 'model_cache'
# This ensures Render keeps the files!
echo "Downloading AI model to local project folder..."
python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2', cache_folder='./model_cache')"