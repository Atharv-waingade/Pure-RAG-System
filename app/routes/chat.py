from fastapi import APIRouter
from pydantic import BaseModel
from app.core.embedding_model import get_embedding_model
import json
import os
import numpy as np

router = APIRouter()

# --- 1. LOAD DATA LOCALLY (Private) ---
# We load your clients/products.json into memory once
PRODUCTS_DB = []
try:
    with open("clients/products.json", "r") as f:
        data = json.load(f)
        # Flatten the list if it's nested
        if isinstance(data, dict):
            PRODUCTS_DB = data.get("products", [])
        elif isinstance(data, list):
            PRODUCTS_DB = data
except Exception as e:
    print(f"Error loading database: {e}")

class QueryRequest(BaseModel):
    query: str

# --- 2. THE "FRAMER" (Smart Template Engine) ---
def frame_response(product):
    """
    This acts like a Generative Model but uses 0 RAM.
    It takes raw data and turns it into a natural sentence.
    """
    name = product.get('name', 'This item')
    price = product.get('price', 'N/A')
    desc = product.get('description', '')
    
    # Randomize these templates to make it feel "alive" if you want
    return (
        f"I found exactly what you're looking for! The **{name}** is a great choice. "
        f"It is currently priced at **${price}**. "
        f"Here is what you should know: {desc}"
    )

@router.post("/chat")
async def chat_endpoint(request: QueryRequest):
    user_query = request.query
    model = get_embedding_model()

    # A. Search (The "Brain")
    # -----------------------
    if not PRODUCTS_DB:
        return {"response": "My archives are currently empty. Please add products to the database."}
        
    # Create embeddings for all products (In a real app, cache this!)
    # For this demo, we do it on the fly (Fast with bert-tiny)
    product_texts = [f"{p.get('name')} {p.get('description')}" for p in PRODUCTS_DB]
    product_embeddings = model.encode(product_texts)
    query_embedding = model.encode([user_query])
    
    # Calculate similarity (Dot Product)
    scores = np.dot(query_embedding, product_embeddings.T)[0]
    best_idx = np.argmax(scores)
    best_score = scores[best_idx]
    
    # B. Frame the Answer
    # -------------------
    if best_score < 0.3: # Threshold for "I don't know"
        return {"response": "I looked through the archives, but I couldn't find a product matching that description. Could you be more specific?"}
    
    best_product = PRODUCTS_DB[best_idx]
    
    # Use our "Framer" function to generate the private response
    natural_response = frame_response(best_product)
    
    return {"response": natural_response}