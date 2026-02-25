from fastapi import APIRouter
from pydantic import BaseModel
from app.core.embedding_model import get_embedding_model
import json
import os
import numpy as np

router = APIRouter()

# --- 1. DATA LOADER ---
def load_products():
    # Calculate the path to 'data/products.json'
    current_file = os.path.abspath(__file__)           # .../app/routes/chat.py
    app_dir = os.path.dirname(os.path.dirname(current_file)) # .../app
    root_dir = os.path.dirname(app_dir)                # .../ (Project Root)
    
    # EXACT PATH from your screenshot
    json_path = os.path.join(root_dir, "data", "products.json")
    
    print(f"🔍 Looking for database at: {json_path}")
    
    if os.path.exists(json_path):
        try:
            with open(json_path, "r") as f:
                data = json.load(f)
                # Handle standard JSON structure
                items = data.get("products", []) if isinstance(data, dict) else data
                print(f"✅ SUCCESS: Loaded {len(items)} products.")
                return items
        except Exception as e:
            print(f"❌ Error reading file: {e}")
    else:
        print("⚠️ File not found. Did you run 'git add -f data/products.json'?")
    
    return []

# Load DB on startup
PRODUCTS_DB = load_products()

class QueryRequest(BaseModel):
    query: str

# --- 2. TEMPLATE ENGINE ---
def frame_response(product):
    title = product.get('title', 'This piece')
    price = product.get('price', 'N/A')
    desc = product.get('description', 'No details available.')
    return (
        f"I found a piece that matches your inquiry. The **{title}** is currently available "
        f"for **${price}**. Here are the details: {desc}"
    )

@router.post("/chat")
async def chat_endpoint(request: QueryRequest):
    global PRODUCTS_DB
    # Retry loading if empty (in case of server delay)
    if not PRODUCTS_DB:
        PRODUCTS_DB = load_products()
        
    if not PRODUCTS_DB:
        return {"response": "My archives are empty. Please run 'git add -f data/products.json' and push again."}

    # Search Logic (Dot Product Similarity)
    model = get_embedding_model()
    user_query = request.query
    
    product_texts = [f"{p.get('title')} {p.get('description')}" for p in PRODUCTS_DB]
    product_embeddings = model.encode(product_texts)
    query_embedding = model.encode([user_query])
    
    scores = np.dot(query_embedding, product_embeddings.T)[0]
    best_idx = np.argmax(scores)
    
    if scores[best_idx] < 0.2: # Low confidence threshold
        return {"response": "I searched the collection, but I couldn't find a piece matching that description."}
    
    return {"response": frame_response(PRODUCTS_DB[best_idx])}