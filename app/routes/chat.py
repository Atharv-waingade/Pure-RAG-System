from fastapi import APIRouter
from pydantic import BaseModel
import httpx
import json
import os
import re
import math
import asyncio
from collections import Counter, defaultdict
import difflib

router = APIRouter()
API_URL = "https://dummyjson.com/products?limit=100"

# ==============================================================================
# 1. INVENTORY MANAGER (Static RAM Cache)
# ==============================================================================
class InventoryManager:
    def __init__(self):
        self.products = []
        self._lock = asyncio.Lock()

    async def get_products(self):
        if self.products: return self.products
        async with self._lock:
            if not self.products: await self._load_data()
            return self.products

    async def _load_data(self):
        print("🔄 SYSTEM: Booting Micro-NLP Engine...")
        live_data = await self._fetch_from_api()
        if not live_data: live_data = self._load_local_fallback()
        
        if live_data:
            self.products = live_data
            nlp_brain.build_knowledge_base(self.products) 
            print(f"✅ SYSTEM: Indexed {len(self.products)} items. NLP Pipeline Ready.")
        else:
            print("❌ CRITICAL: No data.")

    async def _fetch_from_api(self):
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(API_URL)
                return resp.json().get("products", []) if resp.status_code == 200 else None
        except: return None

    def _load_local_fallback(self):
        try:
            with open("data/products.json", "r") as f: return json.load(f).get("products", [])
        except: return []

inventory = InventoryManager()

# ==============================================================================
# 2. MICRO-NLP & BM25 SEARCH ENGINE (The 150MB Genius)
# ==============================================================================
class NLP_SearchEngine:
    def __init__(self):
        self.stop_words = {"i", "me", "my", "we", "you", "is", "are", "was", "a", "an", "the", "and", "or", "of", "to", "in", "on", "show", "want", "buy", "find", "looking", "get"}
        self.vocab = set() # Custom dictionary of your products
        
        # BM25 Constants
        self.k1, self.b = 1.5, 0.75
        self.doc_len, self.idf, self.product_map = {}, {}, {}
        self.avg_doc_len = 0
        self.corpus_size = 0

    # --- UPGRADE 1: CATALOG-AWARE SPELL CHECK ---
    def build_knowledge_base(self, products):
        self.corpus_size = len(products)
        self.product_map = {p['id']: p for p in products}
        
        df = defaultdict(int)
        total_length = 0
        
        for p in products:
            content = f"{p.get('title','')} {p.get('category','')} {p.get('brand','')} {p.get('description','')}"
            tokens = self._tokenize(content)
            
            self.doc_len[p['id']] = len(tokens)
            total_length += len(tokens)
            
            for token in set(tokens):
                df[token] += 1
                self.vocab.add(token) # Add to recognized vocabulary
                
        self.avg_doc_len = total_length / self.corpus_size if self.corpus_size > 0 else 1

        for token, freq in df.items():
            self.idf[token] = math.log((self.corpus_size - freq + 0.5) / (freq + 0.5) + 1)

    def _tokenize(self, text):
        text = re.sub(r'[^a-z0-9\s]', '', str(text).lower())
        tokens = [t for t in text.split() if t not in self.stop_words]
        return [t[:-1] if (t.endswith('s') and not t.endswith('ss') and len(t)>3) else t for t in tokens]

    def _auto_correct(self, tokens):
        corrected = []
        for t in tokens:
            if t in self.vocab or t.isdigit(): 
                corrected.append(t)
            else:
                # Find the closest matching word in YOUR specific catalog
                closest = difflib.get_close_matches(t, self.vocab, n=1, cutoff=0.75)
                corrected.append(closest[0] if closest else t)
        return corrected

    # --- UPGRADE 2: PRICE & INTENT EXTRACTION ---
    def extract_intent(self, query):
        q_lower = query.lower()
        intent = {
            "max_price": float('inf'),
            "min_price": 0,
            "sort_by": "relevance" # relevance, price_asc, rating_desc
        }
        
        # 1. Detect Numbers (e.g., "under 50", "< 100", "500 dollars")
        price_match = re.search(r'(under|less than|<|below|max)\s*\$?(\d+)', q_lower)
        if price_match: intent["max_price"] = float(price_match.group(2))
        
        price_match_above = re.search(r'(over|more than|>|above|min)\s*\$?(\d+)', q_lower)
        if price_match_above: intent["min_price"] = float(price_match_above.group(2))

        # 2. Detect Adjectives
        if "cheap" in q_lower or "affordable" in q_lower:
            intent["sort_by"] = "price_asc"
        elif "best" in q_lower or "top rated" in q_lower:
            intent["sort_by"] = "rating_desc"
            
        return intent

    # --- UPGRADE 3: HYBRID SEARCH ---
    def search(self, query, products):
        if not self.corpus_size: return []
        
        # 1. Parse Intent & Correct Spelling
        intent = self.extract_intent(query)
        raw_tokens = self._tokenize(query)
        query_tokens = self._auto_correct(raw_tokens)
        
        # Remove intent words from search query so they don't mess up BM25
        search_tokens = [t for t in query_tokens if t not in ["under", "over", "cheap", "best", "dollar", "price"]]
        
        scores = defaultdict(float)
        
        # 2. BM25 Scoring + Price Filtering
        for p in products:
            price = float(p.get('price', 0))
            
            # HARD FILTER: Discard items outside price range
            if price > intent["max_price"] or price < intent["min_price"]:
                continue

            pid = p['id']
            content = f"{p.get('title','')} {p.get('category','')} {p.get('brand','')} {p.get('description','')}"
            doc_counts = Counter(self._tokenize(content))
            
            score = 0
            for token in search_tokens:
                if token not in doc_counts: continue
                freq = doc_counts[token]
                idf = self.idf.get(token, 0)
                num = freq * (self.k1 + 1)
                den = freq + self.k1 * (1 - self.b + self.b * (self.doc_len[pid] / self.avg_doc_len))
                score += idf * (num / den)
                
                if token in self._tokenize(p.get('title','')): score *= 1.5 # Title Bonus

            if score > 0 or not search_tokens: # If only asked for "cheap", show all cheap items
                scores[pid] = score

        # 3. Apply Smart Sorting
        valid_products = [self.product_map[pid] for pid in scores]
        
        if intent["sort_by"] == "price_asc":
            valid_products.sort(key=lambda x: x.get('price', 0))
        elif intent["sort_by"] == "rating_desc":
            valid_products.sort(key=lambda x: x.get('rating', 0), reverse=True)
        else:
            # Default: Sort by BM25 Relevance
            valid_products.sort(key=lambda x: scores[x['id']], reverse=True)

        return valid_products[:3]

nlp_brain = NLP_SearchEngine()

# ==============================================================================
# 3. CONVERSATION HANDLER
# ==============================================================================
class ConversationHandler:
    def handle_greeting(self, text):
        if any(w in text.lower().split() for w in ["hi", "hello", "hey"]):
            return "Welcome! I am your intelligent shopping assistant. I can find specific items, top-rated products, or items within your budget. What do you need?"
        return None

    def generate_response(self, query, results):
        if not results:
            return "I searched our inventory but couldn't find a match for those specific requirements. Could you adjust your criteria?"

        if len(results) == 1:
            p = results[0]
            urgency = "🔥 Almost Gone!" if p.get('stock', 10) < 15 else "✅ In Stock"
            return (f"I found the perfect match: <b>{p['title']}</b> (${p['price']}).<br>"
                    f"<i>{p['description']}</i><br><br>"
                    f"⭐ {p.get('rating', 'N/A')}/5  |  {urgency}")

        response = "Here are the top matches I found based on your criteria:<br><br>"
        for p in results:
            response += f"🔹 <b>{p['title']}</b> - ${p['price']} (⭐ {p.get('rating', 'N/A')})<br>"
            short_desc = (p.get('description', '')[:60] + '...') 
            response += f"<i>{short_desc}</i><br><br>"
            
        return response

speaker = ConversationHandler()

# ==============================================================================
# MAIN ROUTE
# ==============================================================================
class QueryRequest(BaseModel):
    query: str

@router.post("/chat")
async def chat_endpoint(request: QueryRequest):
    products = await inventory.get_products()
    if not products: return {"response": "System Notice: Database unavailable."}

    greeting = speaker.handle_greeting(request.query)
    if greeting: return {"response": greeting}

    results = nlp_brain.search(request.query, products)
    final_response = speaker.generate_response(request.query, results)
    
    return {"response": final_response}