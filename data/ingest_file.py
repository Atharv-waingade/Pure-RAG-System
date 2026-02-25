import json
import requests
import os

# Configuration
JSON_FILE = "products.json"
API_URL = "http://localhost:8000/sync"
CLIENT_ID = "beauty_store"  # The ID you want to save this data under

def ingest_data():
    # 1. Check if file exists
    if not os.path.exists(JSON_FILE):
        print(f"Error: {JSON_FILE} not found. Please create it first.")
        return

    # 2. Load the JSON file
    print(f"Reading {JSON_FILE}...")
    try:
        with open(JSON_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format. {e}")
        return

    # Handle different JSON structures (List vs Object with "products" key)
    if isinstance(data, dict) and "products" in data:
        products_list = data["products"]
    elif isinstance(data, list):
        products_list = data
    else:
        print("Error: JSON must be a list of products or have a 'products' key.")
        return

    print(f"Found {len(products_list)} products. Formatting...")

    # 3. Transform Data to RAG Format
    formatted_pages = []
    
    for p in products_list:
        # construct a unique ID
        pid = p.get('id', 'unknown')
        sku = p.get('sku', 'unknown')
        title = p.get('title', 'No Title')
        
        # Build the searchable text block
        # We combine all useful fields into one text blob
        content_text = f"""
Product: {title}
Brand: {p.get('brand', 'Generic')}
Price: ${p.get('price', '0.00')}
Category: {p.get('category', 'General')}
Status: {p.get('availabilityStatus', 'Unknown')}
Description: {p.get('description', '')}
Reviews:
"""
        # Add reviews if they exist
        if 'reviews' in p:
            for r in p['reviews']:
                content_text += f"- {r.get('comment', '')} ({r.get('rating', 0)}/5)\n"

        formatted_pages.append({
            "url": f"product://{pid}/{sku}",  # Internal ID for the system
            "content": content_text.strip()
        })

    # 4. Send to API
    payload = {
        "client_id": CLIENT_ID,
        "pages": formatted_pages
    }

    print(f"Sending {len(formatted_pages)} items to RAG engine...")
    
    try:
        response = requests.post(API_URL, json=payload)
        if response.status_code == 200:
            print("✅ Success!")
            print(response.json())
        else:
            print(f"❌ Failed (Status {response.status_code})")
            print(response.text)
            
    except requests.exceptions.ConnectionError:
        print("❌ Error: Could not connect to server. Is 'uvicorn' running?")

if __name__ == "__main__":
    ingest_data()