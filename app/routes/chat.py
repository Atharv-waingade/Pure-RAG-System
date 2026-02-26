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
API_URL = "https://dummyjson.com/products?limit=100"

# ==============================================================================
# HELPER: PRODUCTION DATA ARMOR
# Prevents server crashes if the JSON contains dirty data (e.g., price = "N/A")
# ==============================================================================
def safe_float(val, default=0.0):
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

# ==============================================================================
# 1. INVENTORY MANAGER (Auto-Schema Discovery & RAM Cache)
# ==============================================================================
class InventoryManager:
    def __init__(self):
        self.products = []
        self.schema = {}
        self._lock = asyncio.Lock()

    async def get_products(self):
        if self.products: return self.products, self.schema
        async with self._lock:
            if not self.products: await self._load_data()
            return self.products, self.schema

    async def _load_data(self):
        print("🔄 SYSTEM: Booting Apex Engine...")
        live_data = await self._fetch_from_api()
        if not live_data: live_data = self._load_local_fallback()
        
        if live_data:
            self.products = live_data
            self._discover_schema(live_data)
            apex_brain.build_knowledge_base(self.products, self.schema) 
            print(f"✅ SYSTEM: Indexed {len(self.products)} items. Closed-Loop Pipeline Ready.")
        else:
            print("❌ CRITICAL: No data could be loaded.")

    def _discover_schema(self, products):
        sample = products[0] if products else {}
        self.schema = {
            'title': next((k for k in ['title', 'name', 'product_name'] if k in sample), 'title'),
            'price': next((k for k in ['price', 'cost', 'amount'] if k in sample), 'price'),
            'desc': next((k for k in ['description', 'details', 'about'] if k in sample), 'description'),
            'brand': next((k for k in ['brand', 'manufacturer', 'maker'] if k in sample), 'brand'),
            'stock': next((k for k in ['stock', 'quantity', 'inventory'] if k in sample), 'stock'),
            'rating': next((k for k in ['rating', 'stars', 'score'] if k in sample), 'rating')
        }

    async def _fetch_from_api(self):
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(API_URL)
                return resp.json().get("products", []) if resp.status_code == 200 else None
        except: return None

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
                    with open(path, "r") as f: return json.load(f).get("products", [])
                except: pass
        return []

inventory = InventoryManager()

# ==============================================================================
# 2. APEX NLP & LOGIC ENGINE (Max Potential under 150MB)
# ==============================================================================
class ApexEngine:
    def __init__(self):
        self.stop_words = {"i", "me", "my", "we", "you", "is", "are", "was", "a", "an", "the", "and", "or", "of", "to", "in", "on", "show", "want", "buy", "find", "looking", "get", "any", "some", "available"}
        self.vocab = set()
        
        self.k1, self.b = 1.2, 0.75
        self.doc_len, self.idf, self.product_map = {}, {}, {}
        self.avg_doc_len, self.corpus_size = 0, 0
        self.schema = {}

    def _porter_lite_stem(self, word):
        if len(word) <= 3: return word
        if word.endswith('ies'): return word[:-3] + 'y'
        if word.endswith('es') and not word.endswith('shoes'): return word[:-2]
        if word.endswith('s') and not word.endswith('ss'): return word[:-1]
        if word.endswith('ing'): return word[:-3]
        if word.endswith('ed'): return word[:-2]
        return word

    def _tokenize(self, text):
        text = re.sub(r'[^a-z0-9\s]', '', str(text).lower())
        tokens = [t for t in text.split() if t not in self.stop_words]
        return [self._porter_lite_stem(t) for t in tokens]

    def build_knowledge_base(self, products, schema):
        self.schema = schema
        self.corpus_size = len(products)
        self.product_map = {p['id']: p for p in products}
        df = defaultdict(int)
        total_length = 0
        
        for p in products:
            title = p.get(schema['title'], '')
            brand = p.get(schema['brand'], '')
            category = p.get('category', '')
            desc = p.get(schema['desc'], '')
            
            content = f"{title} {brand} {category} {desc}"
            tokens = self._tokenize(content)
            
            self.doc_len[p['id']] = len(tokens)
            total_length += len(tokens)
            
            for token in set(tokens):
                df[token] += 1
                self.vocab.add(token)
                
        self.avg_doc_len = total_length / self.corpus_size if self.corpus_size > 0 else 1
        for token, freq in df.items():
            self.idf[token] = math.log((self.corpus_size - freq + 0.5) / (freq + 0.5) + 1)

    def _handle_logic_triggers(self, query, products):
        q_lower = query.lower()
        
        # 1. Discount Math Logic
        if "final price" in q_lower or "discount" in q_lower:
            for p in products:
                title = p.get(self.schema['title'], '').lower()
                if title in q_lower or str(p['id']) in q_lower:
                    price = safe_float(p.get(self.schema['price'], 0))
                    discount = safe_float(p.get('discountPercentage', 0))
                    if discount > 0:
                        final = price - (price * (discount / 100))
                        return f"The original price is ${price:.2f}. With a {discount}% discount, the final price is **${final:.2f}**.", "logic"
                    return f"The **{p.get(self.schema['title'])}** is currently priced at **${price:.2f}** with no active discounts.", "logic"

        # 2. Minimum Order Logic
        if "buy one" in q_lower or "just one" in q_lower:
            for p in products:
                title = p.get(self.schema['title'], '').lower()
                if any(word in title for word in q_lower.replace("buy one", "").split() if len(word) > 3):
                    moq = int(safe_float(p.get('minimumOrderQuantity', 1)))
                    if moq > 1: return f"Actually, the **{p.get(self.schema['title'])}** requires a minimum order of **{moq} units**.", "logic"
                    return f"Yes, you can buy a single unit of the **{p.get(self.schema['title'])}**.", "logic"

        # 3. Reviews Array Traversal
        if "review" in q_lower and ("by" in q_lower or "has" in q_lower):
            names = re.findall(r'([A-Z][a-z]+ [A-Z][a-z]+)', query)
            if names:
                target_name = names[0]
                reviewed_items = []
                for p in products:
                    for review in p.get('reviews', []):
                        if review.get('reviewerName') == target_name:
                            reviewed_items.append(f"• {p.get(self.schema['title'])} ({review.get('rating', 0)}⭐)")
                if reviewed_items:
                    return f"**{target_name}** has reviewed the following:<br>" + "<br>".join(reviewed_items), "logic"

        # 4. Low Stock Aggregation
        if "almost out" in q_lower or "low stock" in q_lower:
            low_stock = [p for p in products if safe_float(p.get(self.schema['stock'], 100)) < 5 or p.get('availabilityStatus') == 'Low Stock']
            if low_stock:
                items = [f"• {p.get(self.schema['title'])} (Only {p.get(self.schema['stock'])} left!)" for p in low_stock[:5]]
                return "⚠️ **Low Stock Alert:**<br>" + "<br>".join(items), "logic"

        return None, "search"

    def search(self, query, products):
        if not self.corpus_size: return [], "search"
        
        logic_answer, intent_type = self._handle_logic_triggers(query, products)
        if logic_answer: return logic_answer, intent_type

        q_lower = query.lower()
        max_price, min_price = float('inf'), 0
        sort_by = "relevance"
        must_have_shipping = "overnight" in q_lower or "tomorrow" in q_lower

        if any(w in q_lower for w in ["return", "policy", "warranty", "refund"]): return products, "policy"
        
        price_match = re.search(r'(under|less|<|max)\s*\$?(\d+)', q_lower)
        if price_match: max_price = safe_float(price_match.group(2), float('inf'))
        
        if "cheap" in q_lower: sort_by = "price_asc"
        elif "best" in q_lower or "top" in q_lower: sort_by = "rating_desc"

        search_tokens = self._tokenize(query)
        search_tokens = [t for t in search_tokens if t not in ["under", "cheap", "best", "dollar", "price"]]
        
        scores = defaultdict(float)
        for p in products:
            price = safe_float(p.get(self.schema['price'], 0))
            if price > max_price or price < min_price: continue
            if must_have_shipping and "overnight" not in p.get('shippingInformation', '').lower(): continue

            pid = p['id']
            title = p.get(self.schema['title'], '')
            brand = p.get(self.schema['brand'], '')
            category = p.get('category', '')
            
            content = f"{title} {category} {brand}"
            doc_counts = Counter(self._tokenize(content))
            
            score = 0
            for token in search_tokens:
                if token not in doc_counts: continue
                freq = doc_counts[token]
                idf = self.idf.get(token, 0)
                num = freq * (self.k1 + 1)
                den = freq + self.k1 * (1 - self.b + self.b * (self.doc_len[pid] / self.avg_doc_len))
                score += idf * (num / den)
                
                # Math Multipliers for Exact Matches
                if token in self._tokenize(title): score *= 2.5 
                if token in self._tokenize(brand): score *= 2.0
                if token in self._tokenize(category): score *= 1.5

            if score > 0: scores[pid] = score

        if not scores: return [], "search"

        max_score = max(scores.values())
        valid_products = []
        for pid, s in scores.items():
            if s >= (max_score * 0.25) and s > 1.0: 
                p = self.product_map[pid]
                p['_score'] = s 
                valid_products.append(p)
        
        if sort_by == "price_asc": valid_products.sort(key=lambda x: safe_float(x.get(self.schema['price'], 0)))
        elif sort_by == "rating_desc": valid_products.sort(key=lambda x: safe_float(x.get(self.schema['rating'], 0)), reverse=True)
        else: valid_products.sort(key=lambda x: x.get('_score', 0), reverse=True)

        return valid_products[:3], "search"

apex_brain = ApexEngine()

# ==============================================================================
# 3. CONVERSATION HANDLER (UI Formatting)
# ==============================================================================
class ConversationHandler:
    def handle_chit_chat(self, text):
        q = text.lower().strip()
        if any(w in q.split() for w in ["hi", "hello", "hey", "greetings"]):
            return "Welcome! I am your advanced shopping assistant. I can calculate discounts, check stock levels, verify policies, and find specific items. How can I help?"
        if any(w in q for w in ["thank you", "thanks"]):
            return "It is my absolute pleasure. Do you need anything else?"
        return None

    def generate_policy_response(self, products, schema):
        policies = set(str(p.get('returnPolicy', 'Standard 30-day return policy')) for p in products if p.get('returnPolicy'))
        shipping = set(str(p.get('shippingInformation', 'Standard shipping')) for p in products if p.get('shippingInformation'))
        p_text = list(policies)[0] if policies else "Standard 30-day return policy."
        s_text = list(shipping)[0] if shipping else "Standard shipping applies."
        
        return f"📦 **Store Policies:**<br><br>• **Returns:** {p_text}<br>• **Shipping:** {s_text}<br><br>Is there a specific product you'd like to check?"

    def generate_response(self, results, schema):
        if not results: return "I scanned the database but couldn't find an exact match for your criteria. Try adjusting your search."

        if len(results) == 1:
            p = results[0]
            stock = safe_float(p.get(schema['stock'], 10))
            urgency = "🔥 Low Stock!" if stock < 5 else "✅ In Stock"
            return (f"I found the perfect match: <b>{p.get(schema['title'])}</b> (${p.get(schema['price'])}).<br>"
                    f"<i>{p.get(schema['desc'])}</i><br><br>"
                    f"⭐ {p.get(schema['rating'], 'N/A')}/5  |  {urgency}")

        response = "Here are the top matches I found:<br><br>"
        for p in results:
            response += f"🔹 <b>{p.get(schema['title'])}</b> - ${p.get(schema['price'])} (⭐ {p.get(schema['rating'], 'N/A')})<br>"
            desc = str(p.get(schema['desc'], ''))
            short_desc = (desc[:65] + '...') if len(desc) > 65 else desc
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
    products, schema = await inventory.get_products()
    if not products: return {"response": "System Notice: Database unavailable. Please check the API connection."}

    chit_chat = speaker.handle_chit_chat(request.query)
    if chit_chat: return {"response": chit_chat}

    results, intent_type = apex_brain.search(request.query, products)
    
    if intent_type == "logic":
        return {"response": results} 
    elif intent_type == "policy":
        return {"response": speaker.generate_policy_response(products, schema)}
    
    return {"response": speaker.generate_response(results, schema)}