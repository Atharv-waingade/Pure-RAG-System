from fastapi import APIRouter
from pydantic import BaseModel
import json
import os
import random
import difflib
import re

router = APIRouter()

# --- 1. ROBUST DATA LOADER (The "Bloodhound") ---
def load_products():
    """
    Locates and loads the products.json file from any standard directory.
    Prevents 'File Not Found' errors on Render.
    """
    current_file = os.path.abspath(__file__)
    app_dir = os.path.dirname(os.path.dirname(current_file))
    root_dir = os.path.dirname(app_dir)
    
    # List of possible hiding spots for the data
    possible_paths = [
        os.path.join(root_dir, "data", "products.json"),
        os.path.join(root_dir, "clients", "beauty_store", "data", "products.json"),
        os.path.join(root_dir, "products.json")
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    data = json.load(f)
                    items = data.get("products", []) if isinstance(data, dict) else data
                    print(f"✅ SUCCESS: Loaded {len(items)} products from {path}")
                    return items
            except Exception as e:
                print(f"⚠️ Found file at {path} but failed to load: {e}")
                
    print("❌ CRITICAL: Could not find products.json in any expected folder.")
    return []

# Load Data Immediately on Startup
PRODUCTS_DB = load_products()

class QueryRequest(BaseModel):
    query: str

# --- 2. HIGH IQ SEARCH ENGINE (The "Brain") ---
def find_best_match(query, products):
    """
    Intelligent search logic that handles:
    - Plurals (Lipsticks -> Lipstick)
    - Synonyms (Parfum -> Fragrance)
    - Typos (Lipstik -> Lipstick)
    - Compound Words (Nailpolish -> Nail Polish)
    """
    # A. PRE-PROCESSING
    clean_query = query.lower()
    
    # Fix common compound words manually
    replacements = {
        "nailpolish": "nail polish",
        "eyeshadow": "eye shadow",
        "sunblock": "sun block",
        "lipsticks": "lipstick",
        "perfumes": "perfume",
        "scents": "scent"
    }
    for bad, good in replacements.items():
        clean_query = clean_query.replace(bad, good)

    # B. STOP WORDS REMOVAL
    # Remove noise words to focus on the actual intent
    stop_words = {"show", "me", "the", "a", "an", "i", "want", "to", "buy", "find", "looking", "for", "is", "are", "some", "suggest", "something", "in", "my", "of", "do", "you", "have"}
    
    raw_words = re.findall(r'\w+', clean_query)
    important_words = [w for w in raw_words if w not in stop_words]
    
    # Auto-Singularize: If word ends in 's', add the singular version too
    final_search_terms = list(important_words)
    for w in important_words:
        if w.endswith('s'):
            final_search_terms.append(w[:-1])

    # C. SYNONYM EXPANSION
    # Maps user slang to database terms
    synonyms = {
        "parfum": ["perfume", "fragrance", "scent", "cologne", "spray"],
        "scent": ["perfume", "fragrance", "parfum"],
        "smell": ["perfume", "fragrance"],
        "polish": ["nail", "lacquer", "enamel", "manicure"],
        "nail": ["polish", "lacquer"],
        "lipstick": ["lip", "color", "matte", "gloss", "stick", "tint", "balm"],
        "juice": ["drink", "beverage", "refreshing", "liquid"],
        "shadow": ["eye", "palette", "lid"],
        "rouge": ["blush", "cheek", "powder"],
        "creamy": ["lotion", "moisturizer", "soft"]
    }

    # Expand query with synonyms
    expanded_terms = list(final_search_terms)
    for word in final_search_terms:
        if word in synonyms:
            expanded_terms.extend(synonyms[word])

    best_product = None
    max_score = 0
    
    for product in products:
        score = 0
        
        # Create Search Blob (Title + Description + Category)
        prod_title = product.get('title', '').lower()
        prod_desc = product.get('description', '').lower()
        prod_cat = product.get('category', '').lower()
        
        # Tokenize product text
        prod_words = re.findall(r'\w+', prod_title + " " + prod_desc + " " + prod_cat)
        
        # D. SCORING LOGIC
        matches_found = 0
        
        for term in expanded_terms:
            term_matched = False
            
            # 1. Exact Word Match (3 points)
            if term in prod_words:
                score += 3
                term_matched = True
            
            # 2. Substring Match (1 point) - e.g. "lip" inside "lipstick"
            elif any(term in pw for pw in prod_words):
                score += 1
                term_matched = True
            
            # 3. Fuzzy Match (2 points) - Handles typos like "fountation"
            elif difflib.get_close_matches(term, prod_words, n=1, cutoff=0.85):
                score += 2
                term_matched = True
            
            if term_matched:
                matches_found += 1

        # 4. Title Bonus (Critical!)
        # If the word is in the TITLE, it's almost certainly what they want.
        for term in final_search_terms:
             if term in prod_title:
                 score += 5

        # E. PENALTY FOR PARTIAL MATCHES
        # If user searched "Red Nailpolish" (2 concepts) and we only found "Red",
        # we penalize the score to avoid showing Red Lipstick.
        if len(final_search_terms) > 1 and matches_found < len(final_search_terms) / 2:
             score = score / 2 

        if score > max_score:
            max_score = score
            best_product = product
            
    # Threshold: Score must be >= 3 to be considered a "Real Match"
    return best_product if max_score >= 3 else None

# --- 3. TEMPLATE ENGINE (The "Personality") ---
def frame_response(product):
    title = product.get('title', 'This piece')
    price = product.get('price', 'N/A')
    desc = product.get('description', 'No details available.')
    
    openers = [
        "I found exactly what you are looking for.",
        "An excellent choice. I have located this item in our vault.",
        "Here is the piece that matches your inquiry.",
        "I believe this is the perfect match for your request."
    ]
    
    return (
        f"{random.choice(openers)} The **{title}** is currently available. "
        f"It is priced at **${price}**. <br><br>Here are the details: {desc}"
    )

@router.post("/chat")
async def chat_endpoint(request: QueryRequest):
    global PRODUCTS_DB
    
    # Reload safety check (in case file was added after server start)
    if not PRODUCTS_DB:
        PRODUCTS_DB = load_products()
    
    if not PRODUCTS_DB:
        return {"response": "My archives are currently empty. Please ensure 'data/products.json' is committed to the repository."}

    best_product = find_best_match(request.query, PRODUCTS_DB)
    
    if best_product:
        return {"response": frame_response(best_product)}
    else:
        return {"response": "I searched our collection, but I couldn't find a piece matching that specific description. Could you try a broader term like 'lipstick' or 'fragrance'?"}