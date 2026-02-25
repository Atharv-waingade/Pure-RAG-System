#!/usr/bin/env bash
# Exit on error
set -o errexit

pip install -r requirements.txt

# Pre-download the model to the local cache
python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')"