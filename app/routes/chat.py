from fastapi import APIRouter
from pydantic import BaseModel
import json
import os
import random
import difflib
import re

router = APIRouter()

# --- 1. ROBUST DATA LOADER ---
def load_products():
    current_file = os.path.abspath(__file__)
    app_dir = os.path.dirname(os.path.dirname(current_file))
    root_dir = os.path.dirname(app_dir)
    json_path = os.path.join(root_dir, "data", "products.json")
    
    if os.path.exists(json_path):
        try:
            with open(json_path, "r") as f:
                data = json.load(f)
                return data.get("products", []) if isinstance(data, dict) else data
        except:
            pass
    return []

PRODUCTS_DB = load_products()

class QueryRequest(BaseModel):
    query: str

# --- 2. INTELLIGENT SEARCH ENGINE ---
def find_best_match(query, products):
    # A. INTELLIGENCE: STOP WORDS
    # We remove these "noise" words so the AI focuses on what matters
    stop_words = {"show", "me", "the", "a", "an", "i", "want", "to", "buy", "find", "looking", "for", "is", "are", "some", "suggest", "something"}
    
    # Clean and tokenize the query
    # We use regex to split by any non-letter character
    raw_words = re.findall(r'\w+', query.lower())
    important_words = [w for w in raw_words if w not in stop_words]
    
    # If the user ONLY typed stop words (e.g., "Show me"), we return nothing
    if not important_words:
        return None

    # B. INTELLIGENCE: SYNONYMS
    synonyms = {
        "parfum": ["perfume", "fragrance", "scent", "cologne", "spray"],
        "scent": ["perfume", "fragrance", "parfum"],
        "smell": ["perfume", "fragrance", "scent"],
        "lipstick": ["lip", "color", "matte", "gloss", "stick"],
        "rouge": ["blush", "cheek", "powder"],
        "juice": ["drink", "beverage", "refreshing", "liquid"],
        "creamy": ["smooth", "soft", "lotion", "moisturizer"],
        "eye": ["shadow", "palette", "liner", "mascara", "lid"]
    }

    # Expand query with synonyms
    search_terms = list(important_words)
    for word in important_words:
        if word in synonyms:
            search_terms.extend(synonyms[word])

    best_product = None
    max_score = 0
    
    for product in products:
        score = 0
        
        # Prepare Product Text
        # We combine title, description, and category into one search field
        prod_title = product.get('title', '').lower()
        prod_desc = product.get('description', '').lower()
        prod_cat = product.get('category', '').lower()
        
        # Tokenize product text (split into list of words)
        prod_words = re.findall(r'\w+', prod_title + " " + prod_desc + " " + prod_cat)
        
        for term in search_terms:
            # C. INTELLIGENCE: EXACT MATCH (High Points)
            if term in prod_words:
                score += 3
            
            # D. INTELLIGENCE: FUZZY MATCH (Medium Points)
            # This handles typos like "lipstik" -> "lipstick"
            # get_close_matches looks for words that are 80% similar
            elif difflib.get_close_matches(term, prod_words, n=1, cutoff=0.8):
                score += 1

        # Bonus: Title Match gets extra priority
        for term in important_words:
             if term in prod_title:
                 score += 5

        if score > max_score:
            max_score = score
            best_product = product
            
    # E. INTELLIGENCE: CONFIDENCE THRESHOLD
    # If the score is too low (meaning we barely found anything), 
    # we prefer to say "I don't know" rather than guessing wrong.
    return best_product if max_score >= 2 else None

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

    best_product = find_best_match(request.query, PRODUCTS_DB)
    
    if best_product:
        return {"response": frame_response(best_product)}
    else:
        return {"response": "I searched our collection, but I couldn't find a piece matching that specific description. Could you try a broader term like 'lipstick' or 'fragrance'?"}