from fastapi import APIRouter
from pydantic import BaseModel
import json
import os
import random

router = APIRouter()

# --- 1. ROBUST DATA LOADER ---
def load_products():
    # Find the products.json file no matter where it is
    current_file = os.path.abspath(__file__)
    app_dir = os.path.dirname(os.path.dirname(current_file))
    root_dir = os.path.dirname(app_dir)
    
    # Check data/products.json
    json_path = os.path.join(root_dir, "data", "products.json")
    
    if os.path.exists(json_path):
        try:
            with open(json_path, "r") as f:
                data = json.load(f)
                return data.get("products", []) if isinstance(data, dict) else data
        except:
            pass
    return []

# Load Data Once
PRODUCTS_DB = load_products()

class QueryRequest(BaseModel):
    query: str

# --- 2. ZERO-RAM SEARCH ENGINE ---
def find_best_match(query, products):
    """
    Finds the best product based on keyword overlap.
    Ultra-fast and uses 0 RAM.
    """
    query_words = query.lower().split()
    best_product = None
    max_score = 0
    
    for product in products:
        score = 0
        # Create a text blob of the product
        product_text = (product.get('title', '') + " " + product.get('description', '') + " " + product.get('category', '')).lower()
        
        # Simple scoring: +1 for every matching word
        for word in query_words:
            if word in product_text:
                score += 1
        
        # Boost score if the word is in the Title
        if query.lower() in product.get('title', '').lower():
            score += 5
            
        if score > max_score:
            max_score = score
            best_product = product
            
    return best_product if max_score > 0 else None

# --- 3. TEMPLATE ENGINE ---
def frame_response(product):
    title = product.get('title', 'This piece')
    price = product.get('price', 'N/A')
    desc = product.get('description', 'No details available.')
    
    openers = [
        "I found exactly what you are looking for.",
        "An excellent choice. I have located this item in our vault.",
        "Here is the piece that matches your inquiry."
    ]
    
    return (
        f"{random.choice(openers)} The **{title}** is currently available. "
        f"It is priced at **${price}**. <br><br>Here are the details: {desc}"
    )

@router.post("/chat")
async def chat_endpoint(request: QueryRequest):
    global PRODUCTS_DB
    if not PRODUCTS_DB:
        PRODUCTS_DB = load_products()
    
    if not PRODUCTS_DB:
        return {"response": "My archives are empty. Please check the 'data' folder."}

    # Use the Zero-RAM Search
    best_product = find_best_match(request.query, PRODUCTS_DB)
    
    if best_product:
        return {"response": frame_response(best_product)}
    else:
        return {"response": "I searched our collection, but I couldn't find a piece matching that specific description. Could you try a broader term like 'lipstick' or 'mascara'?"}