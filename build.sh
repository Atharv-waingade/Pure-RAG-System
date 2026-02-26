#!/usr/bin/env bash
# Exit on error
set -o errexit

# Upgrade pip and install the lightweight requirements
pip install --upgrade pip
pip install -r requirements.txt

echo "✅ Build Completed Successfully. Ready for launch."