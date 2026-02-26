from fastapi import APIRouter
from pydantic import BaseModel
import httpx
import json
import os
import re
import math
import asyncio
from collections import Counter, defaultdict

router = APIRouter()

# ==============================================================================
# HELPER: SAFE CASTING
# ==============================================================================
def safe_float(val, default=0.0):
    try: return float(val)
    except (ValueError, TypeError): return default

# ==============================================================================
# 1. DATA MANAGER (Products, Recipes, Carts)
# ==============================================================================
class DataManager:
    def __init__(self):
        self.products, self.recipes, self.carts = [], [], []
        self._lock = asyncio.Lock()
        self.ready = False

    async def get_data(self):
        if self.ready: return self.products, self.recipes, self.carts
        async with self._lock:
            if not self.ready: await self._load_all_data()
            return self.products, self.recipes, self.carts

    async def _load_all_data(self):
        print("🔄 SYSTEM: Booting Tri-Domain Federated Engine...")
        
        async with httpx.AsyncClient(timeout=20.0) as client:
            try:
                p_res, r_res, c_res = await asyncio.gather(
                    client.get("https://dummyjson.com/products?limit=100"),
                    client.get("https://dummyjson.com/recipes?limit=100"),
                    client.get("https://dummyjson.com/carts?limit=50"),
                    return_exceptions=True
                )
                
                self.products = p_res.json().get("products", []) if getattr(p_res, 'status_code', 0) == 200 else []
                self.recipes = r_res.json().get("recipes", []) if getattr(r_res, 'status_code', 0) == 200 else []
                self.carts = c_res.json().get("carts", []) if getattr(c_res, 'status_code', 0) == 200 else []
            except Exception as e:
                print(f"⚠️ API Error: {e}.")

        if self.products or self.recipes or self.carts:
            multi_brain.build_product_index(self.products)
            multi_brain.build_recipe_index(self.recipes)
            multi_brain.build_cart_index(self.carts)
            self.ready = True
            print(f"✅ SYSTEM: Indexed {len(self.products)} Products, {len(self.recipes)} Recipes, {len(self.carts)} Carts.")
        else:
            print("❌ CRITICAL: No data could be loaded.")

inventory = DataManager()

# ==============================================================================
# 2. MULTI-DOMAIN NLP ENGINE (The Federated Brain)
# ==============================================================================
class MultiDomainEngine:
    def __init__(self):
        self.stop_words = {"i", "me", "my", "we", "you", "is", "are", "was", "a", "an", "the", "and", "or", "of", "to", "in", "on", "show", "want", "buy", "find", "looking", "get", "any", "some", "available"}
        
        self.p_idf, self.p_doc_len, self.p_map = {}, {}, {}
        self.r_idf, self.r_doc_len, self.r_map = {}, {}, {}
        self.c_map = {} 
        self.p_avg_len, self.r_avg_len = 1, 1

    def _stem(self, word):
        if len(word) <= 3: return word
        if word.endswith('ies'): return word[:-3] + 'y'
        if word.endswith('es') and not word.endswith('shoes'): return word[:-2]
        if word.endswith('s') and not word.endswith('ss'): return word[:-1]
        return word

    def _tokenize(self, text):
        text = re.sub(r'[^a-z0-9\s]', '', str(text).lower())
        return [self._stem(t) for t in text.split() if t not in self.stop_words]

    def build_product_index(self, products):
        self.p_map = {p['id']: p for p in products}
        df, total_len = defaultdict(int), 0
        for p in products:
            content = f"{p.get('title','')} {p.get('brand','')} {p.get('category','')} {p.get('description','')}"
            tokens = self._tokenize(content)
            self.p_doc_len[p['id']] = len(tokens)
            total_len += len(tokens)
            for t in set(tokens): df[t] += 1
        self.p_avg_len = total_len / len(products) if products else 1
        for t, freq in df.items():
            self.p_idf[t] = math.log((len(products) - freq + 0.5) / (freq + 0.5) + 1)

    def build_recipe_index(self, recipes):
        self.r_map = {r['id']: r for r in recipes}
        df, total_len = defaultdict(int), 0
        for r in recipes:
            ingredients = " ".join(r.get('ingredients', []))
            content = f"{r.get('name','')} {ingredients} {r.get('mealType',[''])[0]} {r.get('cuisine','')}"
            tokens = self._tokenize(content)
            self.r_doc_len[r['id']] = len(tokens)
            total_len += len(tokens)
            for t in set(tokens): df[t] += 1
        self.r_avg_len = total_len / len(recipes) if recipes else 1
        for t, freq in df.items():
            self.r_idf[t] = math.log((len(recipes) - freq + 0.5) / (freq + 0.5) + 1)

    def build_cart_index(self, carts):
        self.c_map = {str(c['id']): c for c in carts}

    def route_query(self, query):
        q = query.lower()
        
        # 1. Cart Intent
        if any(w in q for w in ["cart", "basket", "checkout", "my order"]):
            return "carts", "search"

        # 2. Recipe Intent (Fixed: Removed broad words like "food" and "cook")
        if any(w in q for w in ["recipe", "how to make", "ingredients for", "dish", "cuisine"]):
            return "recipes", "search"
            
        # 3. Product Metadata Intents
        if any(w in q for w in ["return", "policy", "warranty", "refund", "shipping", "ship"]):
            return "products", "policy"
        if any(w in q for w in ["minimum", "moq", "just one", "buy one", "how many"]):
            return "products", "moq"
        if "review" in q or "rating" in q:
            return "products", "reviews"
        if "discount" in q or "final price" in q:
            return "products", "discount"

        # Default to standard product search
        return "products", "search"

    def search_carts(self, query):
        numbers = re.findall(r'\d+', query)
        if numbers:
            target_id = numbers[0]
            if target_id in self.c_map:
                return [self.c_map[target_id]]
        return []

    def search_recipes(self, query):
        search_tokens = self._tokenize(query)
        scores = defaultdict(float)
        
        for rid, r in self.r_map.items():
            ingredients = " ".join(r.get('ingredients', []))
            content = f"{r.get('name','')} {ingredients} {r.get('cuisine','')}"
            doc_counts = Counter(self._tokenize(content))
            
            score = 0
            for token in search_tokens:
                if token not in doc_counts: continue
                freq = doc_counts[token]
                idf = self.r_idf.get(token, 0)
                num = freq * 2.5 
                den = freq + 1.5 * (0.25 + 0.75 * (self.r_doc_len[rid] / self.r_avg_len))
                score += idf * (num / den)
                if token in self._tokenize(r.get('name', '')): score *= 2.0 

            if score > 0: scores[rid] = score

        if not scores: return []
        max_score = max(scores.values())
        results = [self.r_map[rid] for rid, s in scores.items() if s >= (max_score * 0.25)]
        results.sort(key=lambda x: scores[x['id']], reverse=True)
        return results[:3]

    def search_products(self, query):
        search_tokens = self._tokenize(query)
        search_tokens = [t for t in search_tokens if t not in ["under", "cheap", "price", "return", "policy", "cart"]]
        scores = defaultdict(float)
        
        for pid, p in self.p_map.items():
            content = f"{p.get('title','')} {p.get('brand','')} {p.get('category','')}"
            doc_counts = Counter(self._tokenize(content))
            
            score = 0
            for token in search_tokens:
                if token not in doc_counts: continue
                freq = doc_counts[token]
                idf = self.p_idf.get(token, 0)
                num = freq * 2.5
                den = freq + 1.5 * (0.25 + 0.75 * (self.p_doc_len[pid] / self.p_avg_len))
                score += idf * (num / den)
                if token in self._tokenize(p.get('title', '')): score *= 2.5
                if token in self._tokenize(p.get('brand', '')): score *= 2.0

            if score > 0: scores[pid] = score

        if not scores: return []
        max_score = max(scores.values())
        results = [self.p_map[pid] for pid, s in scores.items() if s >= (max_score * 0.25)]
        results.sort(key=lambda x: scores[x['id']], reverse=True)
        return results[:3]

multi_brain = MultiDomainEngine()

# ==============================================================================
# 3. CONVERSATION HANDLER (Personality & Formatting)
# ==============================================================================
class ConversationHandler:
    
    # RESTORED: The Personality Block
    def handle_chit_chat(self, text):
        q = text.lower().strip()
        greetings = ["hi", "hello", "hey", "greetings"]
        if any(w == q for w in greetings) or any(q.startswith(w + " ") for w in greetings):
            return "Welcome to Maison Luxe. I am your digital concierge. I can find products, provide recipes, or check your cart. How may I assist you?"
        if "thank" in q or "appreciate" in q:
            return "You are very welcome! Let me know if you need anything else."
        return None

    def handle_product_logic(self, sub_intent, results):
        if not results: return "Please specify which product you are asking about."
        
        target = results[0]
        title = target.get('title', 'This item')

        if sub_intent == "policy":
            ret = target.get('returnPolicy', 'Standard 30-day return.')
            war = target.get('warrantyInformation', 'No warranty listed.')
            return f"📦 **Policies for {title}:**<br>• **Returns:** {ret}<br>• **Warranty:** {war}"

        if sub_intent == "moq":
            moq = int(safe_float(target.get('minimumOrderQuantity', 1)))
            if moq > 1: return f"The **{title}** requires a minimum order of **{moq} units**."
            return f"Yes, you can purchase a single unit of the **{title}**."

        if sub_intent == "discount":
            price = safe_float(target.get('price', 0))
            disc = safe_float(target.get('discountPercentage', 0))
            if disc > 0:
                final = price - (price * (disc / 100))
                return f"Original: ${price:.2f}. With {disc}% discount, the final price is **${final:.2f}**."
            return f"The **{title}** is **${price:.2f}** with no active discounts."

        if sub_intent == "reviews":
            reviews = target.get('reviews', [])
            if not reviews: return f"There are no reviews for the **{title}**."
            rev_text = f"**Top Reviews for {title}:**<br>"
            for r in reviews[:2]:
                rev_text += f"• ⭐{r.get('rating', 5)} - <i>\"{r.get('comment', '')}\"</i><br>"
            return rev_text

    def format_products(self, results):
        if not results: return "I scanned our products but couldn't find a match."
        if len(results) == 1:
            p = results[0]
            urgency = "🔥 Low Stock!" if safe_float(p.get('stock', 10)) < 5 else "✅ In Stock"
            return f"I found exactly what you need: <b>{p.get('title')}</b> (${p.get('price')}).<br><i>{p.get('description')}</i><br>⭐ {p.get('rating')}/5 | {urgency}"
        
        res = "Here are the top products I found:<br><br>"
        for p in results:
            res += f"🔹 <b>{p.get('title')}</b> - ${p.get('price')}<br><i>{p.get('description','')[:60]}...</i><br><br>"
        return res

    def format_recipes(self, results):
        if not results: return "I couldn't find a recipe matching those ingredients."
        if len(results) == 1:
            r = results[0]
            ings = ", ".join(r.get('ingredients', [])[:5])
            return f"🍳 **{r.get('name')}** ({r.get('cuisine')})<br><b>Prep:</b> {r.get('prepTimeMinutes')}m | <b>Cook:</b> {r.get('cookTimeMinutes')}m<br><b>Ingredients:</b> {ings}..."
            
        res = "Here are some delicious recipes I found:<br><br>"
        for r in results:
            res += f"🍲 <b>{r.get('name')}</b> ({r.get('difficulty')}) - {r.get('caloriesPerServing')} calories/serving<br>"
        return res

    def format_carts(self, results):
        if not results: 
            return "I couldn't find that cart. Please specify the cart number (e.g., 'What is in cart 5?')."
        
        c = results[0] 
        cart_id = c.get('id')
        total = safe_float(c.get('total', 0))
        discounted = safe_float(c.get('discountedTotal', 0))
        items = c.get('products', [])
        
        res = f"🛒 **Cart #{cart_id} Overview:**<br>"
        res += f"• **Total Items:** {c.get('totalQuantity', 0)}<br>"
        res += f"• **Subtotal:** ${total:.2f}<br>"
        res += f"• **Discounted Total:** ${discounted:.2f}<br><br>**Items in this cart:**<br>"
        
        for item in items[:3]: 
            res += f"  - {item.get('quantity')}x {item.get('title')} (${item.get('price')})<br>"
            
        if len(items) > 3:
            res += f"  - ...and {len(items) - 3} more items.<br>"
            
        return res

speaker = ConversationHandler()

# ==============================================================================
# MAIN ROUTE
# ==============================================================================
class QueryRequest(BaseModel):
    query: str

@router.post("/chat")
async def chat_endpoint(request: QueryRequest):
    products, recipes, carts = await inventory.get_data()
    if not products: return {"response": "System Notice: Database unavailable."}

    query = request.query
    
    # RESTORED: Check for greetings before doing math
    chit_chat = speaker.handle_chit_chat(query)
    if chit_chat: return {"response": chit_chat}
    
    # 1. Traffic Controller
    domain, sub_intent = multi_brain.route_query(query)

    # 2. Execute & Format
    if domain == "carts":
        results = multi_brain.search_carts(query)
        return {"response": speaker.format_carts(results)}

    elif domain == "recipes":
        results = multi_brain.search_recipes(query)
        return {"response": speaker.format_recipes(results)}

    elif domain == "products":
        results = multi_brain.search_products(query)
        if sub_intent != "search":
            return {"response": speaker.handle_product_logic(sub_intent, results)}
        return {"response": speaker.format_products(results)}

    return {"response": "I am unable to process that request."}