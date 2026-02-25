#!/usr/bin/env bash
# Exit on error
set -o errexit

# 1. Install dependencies
pip install -r requirements.txt

# 2. Download the NANO model (17 MB!)
echo "Downloading Nano AI model..."
python -c "from sentence_transformers import SentenceTransformer; model = SentenceTransformer('prajjwal1/bert-tiny'); model.save('./model_cache')"