from fastapi import APIRouter
from pydantic import BaseModel
import httpx
import json
import os
import re
import random
import difflib
import asyncio

router = APIRouter()

# ==============================================================================
# CONFIGURATION
# ==============================================================================
API_URL = "https://dummyjson.com/products?limit=100" 

# ==============================================================================
# 1. STATIC INVENTORY MANAGER (Fetch Once, Keep Forever)
# ==============================================================================
class InventoryManager:
    def __init__(self):
        self.products = []
        self._lock = asyncio.Lock() # Prevents race conditions on startup

    async def get_products(self):
        # 1. CHECK RAM: If we have data, return it immediately.
        # We do NOT check for time expiration. This data stays until restart.
        if self.products:
            return self.products

        # 2. FIRST RUN ONLY: Fetch data safely
        async with self._lock:
            # Double check inside lock (in case two users hit at once)
            if not self.products:
                await self._load_data()
            return self.products

    async def _load_data(self):
        print("🔄 SYSTEM: Booting up... Fetching Inventory...")
        
        # A. Try Live API
        live_data = await self._fetch_from_api()
        
        # B. Fallback to Local JSON (Disaster Recovery)
        if not live_data:
            print("⚠️ API Failed. Loading Backup File.")
            live_data = self._load_local_fallback()

        if live_data:
            self.products = live_data
            print(f"✅ SYSTEM: Inventory Loaded. {len(self.products)} items locked in RAM.")
        else:
            print("❌ CRITICAL: No data available.")

    async def _fetch_from_api(self):
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(API_URL)
                if resp.status_code == 200:
                    data = resp.json()
                    return data.get("products", [])
        except Exception as e: 
            print(f"API Error: {e}")
            return None
        return None

    def _load_local_fallback(self):
        current_file = os.path.abspath(__file__)
        root = os.path.dirname(os.path.dirname(os.path.dirname(current_file)))
        paths = [
            os.path.join(root, "data", "products.json"),
            os.path.join(root, "products.json")
        ]
        for path in paths:
            if os.path.exists(path):
                try:
                    with open(path, "r") as f:
                        data = json.load(f)
                        return data.get("products", []) if isinstance(data, dict) else data
                except: pass
        return []

# Singleton Instance
inventory = InventoryManager()

# ==============================================================================
# 2. UNIVERSAL SEARCH ENGINE (The Brain)
# ==============================================================================
class UniversalSearchEngine:
    def __init__(self):
        self.stop_words = {
            "i", "me", "my", "we", "you", "your", "what", "which", "this", "that", 
            "is", "are", "was", "be", "have", "has", "do", "does", "a", "an", "the", 
            "and", "or", "but", "if", "of", "at", "by", "for", "with", "to", "from", 
            "in", "on", "can", "will", "show", "want", "buy", "need", "find", "looking", 
            "suggest", "recommend", "best", "top", "get"
        }

    def clean_text(self, text):
        text = re.sub(r'[^a-zA-Z0-9\s]', '', str(text).lower())
        words = text.split()
        cleaned = []
        for w in words:
            if w not in self.stop_words:
                if w.endswith('s') and len(w) > 3: # Singularize
                    cleaned.append(w[:-1])
                else:
                    cleaned.append(w)
        return cleaned

    def score_product(self, query_tokens, product):
        score = 0
        
        title = self.clean_text(product.get('title', ''))
        category = self.clean_text(product.get('category', ''))
        brand = self.clean_text(product.get('brand', ''))
        desc = self.clean_text(product.get('description', ''))
        
        all_text = title + category + brand + desc
        
        for token in query_tokens:
            token_score = 0
            if token in title: token_score += 10
            elif token in brand or token in category: token_score += 6
            elif token in desc: token_score += 2
            else:
                closest = difflib.get_close_matches(token, all_text, n=1, cutoff=0.85)
                if closest: token_score += 1.5
            
            if token_score > 0: score += token_score

        return score

    def search(self, query, products):
        tokens = self.clean_text(query)
        if not tokens: return []
        
        scored_results = []
        for p in products:
            s = self.score_product(tokens, p)
            if s > 3: scored_results.append((s, p))
        
        scored_results.sort(key=lambda x: x[0], reverse=True)
        return [p for s, p in scored_results[:3]]

brain = UniversalSearchEngine()

# ==============================================================================
# 3. CONVERSATION HANDLER (The Persona)
# ==============================================================================
class ConversationHandler:
    def handle_greeting(self, text):
        greetings = ["hi", "hello", "hey", "good morning", "start", "greetings"]
        if any(w in text.lower().split() for w in greetings):
            return "Welcome! I am your intelligent shopping assistant. How can I help you explore our collection today?"
        return None

    def generate_response(self, query, results):
        if not results:
            return "I searched our inventory but couldn't find a match. Could you try a broader category like 'smartphones', 'laptops', or 'skincare'?"

        if len(results) == 1:
            p = results[0]
            title = p.get('title')
            price = p.get('price', 'N/A')
            desc = p.get('description', '')
            rating = p.get('rating', '4.5')
            return f"I found the perfect match: <b>{title}</b> is available for ${price}.<br><br><i>{desc}</i><br><br>⭐ Rating: {rating}/5"

        response = "I found a few top-rated options for you:<br><br>"
        for p in results:
            response += f"🔹 <b>{p['title']}</b> - ${p['price']}<br>"
            short_desc = (p.get('description', '')[:70] + '...') if p.get('description') else ''
            response += f"<i>{short_desc}</i><br><br>"
            
        return response + "Would you like more details on any of these?"

speaker = ConversationHandler()

# ==============================================================================
# MAIN ROUTE
# ==============================================================================
class QueryRequest(BaseModel):
    query: str

@router.post("/chat")
async def chat_endpoint(request: QueryRequest):
    # 1. Fetch Data (RAM Only - No Refresh)
    products = await inventory.get_products()
    
    if not products:
        return {"response": "System Notice: Unable to connect to product database."}

    # 2. Greeting
    greeting = speaker.handle_greeting(request.query)
    if greeting:
        return {"response": greeting}

    # 3. Search
    results = brain.search(request.query, products)

    # 4. Respond
    final_response = speaker.generate_response(request.query, results)
    
    return {"response": final_response}