#!/usr/bin/env bash
# exit on error
set -o errexit

pip install -r requirements.txt

# Download NLTK data needed for the chatbot model
python -c "import nltk; nltk.download('punkt'); nltk.download('wordnet'); nltk.download('omw-1.4')"