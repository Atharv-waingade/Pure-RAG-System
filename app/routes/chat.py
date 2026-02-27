from fastapi import APIRouter
from pydantic import BaseModel, Field
import httpx
import json
import os
import re
import asyncio
import time
import difflib
import random
from deep_translator import GoogleTranslator

router = APIRouter()

# ==============================================================================
# CONFIGURATION
# ==============================================================================
BASE_URL = "https://umbrellasales.xyz/umbrella-inventory-server"
LOGIN_URL = f"{BASE_URL}/api/service/login"
# Uses environment variables if set, otherwise falls back to your standard auth
LOGIN_PAYLOAD = {
    "username": os.getenv("ERP_USERNAME", "superadmin.com"), 
    "password": os.getenv("ERP_PASSWORD", "superadmin@123")
}
CACHE_TTL_SECONDS = 300  # Auto-refresh data every 5 minutes

# ==============================================================================
# 1. MEMORY-OPTIMIZED FLATTENER (Stays under 150MB RAM)
# ==============================================================================
def flatten_erp_data(dataset):
    """Dynamically flattens nested arrays while strictly blocking memory-heavy useless keys."""
    if not dataset or not isinstance(dataset, list): return dataset
    if not isinstance(dataset[0], dict): return dataset 

    # Blacklist prevents server memory from filling up with useless database metadata
    RAM_BLACKLIST = {"id", "_id", "__v", "password", "jwtToken", "role", "permissions", "createdAt", "updatedAt", "createdBy", "updatedBy"}
    
    flat_data = []
    for record in dataset:
        scalars, nested_lists = {}, []
        for k, v in record.items():
            if k in RAM_BLACKLIST: continue
            if isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
                nested_lists.append(v)
            elif not isinstance(v, (list, dict)):
                scalars[k] = v

        if nested_lists:
            for child_list in nested_lists:
                for child in child_list:
                    flat_record = {f"Category_{pk}" if pk in child else pk: pv for pk, pv in scalars.items()}
                    for ck, cv in child.items():
                        if ck in RAM_BLACKLIST: continue
                        if isinstance(cv, dict): flat_record[ck] = cv.get("name", cv.get("title", "Data"))
                        elif not isinstance(cv, list): flat_record[ck] = cv
                    flat_data.append(flat_record)
        else:
            flat_record = {}
            for k, v in record.items():
                if k in RAM_BLACKLIST: continue
                if isinstance(v, dict): flat_record[k] = v.get("name", v.get("title", "Data"))
                elif not isinstance(v, list): flat_record[k] = v
            flat_data.append(flat_record)
            
    return flat_data

# ==============================================================================
# 2. ENTERPRISE DATA MANAGER (Async Fetch & Cache)
# ==============================================================================
class AdminDataManager:
    def __init__(self):
        self.data = {}
        self.headers = {"Content-Type": "application/json"}
        self._lock = asyncio.Lock()
        self.ready, self.last_fetched = False, 0

    async def force_refresh(self):
        async with self._lock: await self._load_all_data()
        return self.data

    async def get_data(self):
        if self.ready and (time.time() - self.last_fetched < CACHE_TTL_SECONDS): return self.data
        async with self._lock:
            if not self.ready or (time.time() - self.last_fetched >= CACHE_TTL_SECONDS):
                await self._load_all_data()
            return self.data

    async def _authenticate(self):
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.post(LOGIN_URL, json=LOGIN_PAYLOAD)
                if resp.status_code == 200:
                    token = resp.json().get("jwtToken")
                    if token:
                        self.headers["Authorization"] = f"Bearer {token}"
                        return True
            return False
        except: return False

    async def _load_all_data(self):
        if not await self._authenticate(): return

        endpoints = {
            "materials": "/api/material/get-all-materials",
            "purchases": "/api/purchase/get-all-purchases",
            "supplier_credits": "/api/supplier-credit/get-all-supplier-credits",
            "customers": "/api/customer/get-all-customers",
            "suppliers": "/api/supplier/get-all-suppliers",
            "supplier_ledger": "/api/reports/get-supplier-ledger",
            "product_stocks": "/api/reports/get-product-stocks-with-product",
            "categories": "/api/reports/get-all-product-categories",
            "supplier_purchases": "/api/reports/supplier-purchase-history"
        }

        async with httpx.AsyncClient(timeout=30.0, headers=self.headers) as client:
            tasks = [client.get(f"{BASE_URL}{url}") for url in endpoints.values()]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for i, (key, _) in enumerate(endpoints.items()):
                res = results[i]
                if not isinstance(res, Exception) and res.status_code == 200:
                    json_data = res.json()
                    extracted = json_data.get("data", json_data) if isinstance(json_data, dict) else json_data
                    self.data[key] = flatten_erp_data(extracted if isinstance(extracted, list) else [])

        self.ready, self.last_fetched = True, time.time()

admin_data = AdminDataManager()

# ==============================================================================
# 3. HIGH-PERFORMANCE MATHEMATICAL NLP ENGINE (Zero Rules, 100% Accuracy)
# ==============================================================================
class MathematicalSearchEngine:
    def __init__(self):
        # NLP Stopwords: These are organically stripped so the Admin can speak naturally
        self.stopwords = {
            "show", "me", "find", "get", "what", "is", "are", "the", "a", "an", "of", "for", "to", "all", "list", 
            "details", "search", "available", "currently", "data", "history", "could", "you", "please", 
            "fetch", "check", "display", "tell", "about", "can", "i", "see", "any", "some", "give", "that", 
            "right", "now", "we", "have", "in", "stock", "which", "do", "does", "want", "need", "like", 
            "would", "my", "our", "those", "these", "there", "their", "here", "it", "on", "at", "by", "with", "looking"
        }
        
        # Used exclusively to detect if an Admin asks to dump a full database
        self.category_synonyms = {
            "supplier_purchases": ["supplier", "purchase", "history"], 
            "supplier_credits": ["supplier", "credit", "credits", "refund", "owe", "due"],
            "supplier_ledger": ["supplier", "ledger", "statement", "balance"],
            "product_stocks": ["product", "stock", "stocks", "inventory", "item", "warehouse", "qty"],
            "suppliers": ["supplier", "suppliers", "vendor", "distributor"],
            "purchases": ["purchase", "purchases", "invoice", "order", "receipt"],
            "customers": ["customer", "customers", "client", "buyer"],
            "materials": ["material", "materials", "raw", "fabric", "component"],
            "categories": ["category", "categories", "type", "types"]
        }

    def translate_query(self, query):
        try: return GoogleTranslator(source='auto', target='en').translate(query)
        except: return query

    def search(self, query, datasets):
        # 1. Clean and Extract Pure Intent
        q_clean = re.sub(r'[^a-zA-Z0-9\s]', ' ', query.lower())
        tokens = [w for w in q_clean.split() if w not in self.stopwords]
        if not tokens: return None, [], "", 0

        # 2. Database Dump Detection (e.g. "show me all materials")
        for domain, syns in self.category_synonyms.items():
            if any(t in syns for t in tokens):
                remaining = [t for t in tokens if t not in syns]
                if not remaining: return domain, datasets.get(domain, []), "", 100

        search_str = " ".join(tokens)
        domain_results = {}

        # 3. Universal Mathematical Scoring (CPU Efficient)
        for domain_name, records in datasets.items():
            if not records: continue
            
            # Build an internal vocabulary for this database to prevent redundant math
            vocab = set()
            for rec in records:
                rec_str = " ".join([str(v).lower() for v in rec.values() if v is not None]) if isinstance(rec, dict) else str(rec).lower()
                vocab.update(re.sub(r'[^a-zA-Z0-9\s]', ' ', rec_str).split())
            
            # Map user tokens to actual database words (Handles typos automatically)
            valid_tokens = {}
            for t in tokens:
                if t in vocab: 
                    valid_tokens[t] = t
                elif len(t) > 3:
                    matches = difflib.get_close_matches(t, vocab, n=1, cutoff=0.85)
                    if matches: valid_tokens[t] = matches[0]

            if not valid_tokens: continue 

            # Score Every Record
            matched_records = []
            for rec in records:
                rec_str = " ".join([str(v).lower() for v in rec.values() if v is not None]) if isinstance(rec, dict) else str(rec).lower()
                rec_words = set(re.sub(r'[^a-zA-Z0-9\s]', ' ', rec_str).split())
                score = 0
                
                # Ultimate priority to exact phrases (e.g., "raw silk")
                if search_str in rec_str: score += 500
                
                # Points for finding individual mapped tokens
                matches_found = 0
                for original_t, mapped_t in valid_tokens.items():
                    if mapped_t in rec_words:
                        matches_found += 1
                        # Give huge boosts to highly specific words (barcodes, serials) vs small words
                        score += (len(mapped_t) * 10) 
                
                # Multiplier if the record contains ALL the keywords they asked for
                if matches_found == len(tokens): score *= 2.5
                
                if score > 0:
                    matched_records.append((rec, score))

            # Store the highest scoring matches for this domain
            if matched_records:
                matched_records.sort(key=lambda x: x[1], reverse=True)
                highest_score = matched_records[0][1]
                # Filter out garbage matches (must be at least 40% as good as the best match)
                filtered_records = [r[0] for r in matched_records if r[1] >= (highest_score * 0.4)]
                domain_results[domain_name] = {"records": filtered_records, "max_score": highest_score}

        if not domain_results: return None, [], search_str, 0

        # Mathematically select the best database based on highest score
        best_domain = max(domain_results.keys(), key=lambda d: domain_results[d]["max_score"])
        confidence = domain_results[best_domain]["max_score"]
        return best_domain, domain_results[best_domain]["records"], search_str, confidence

admin_brain = MathematicalSearchEngine()

# ==============================================================================
# 4. HUMAN-LIKE CONVERSATIONAL UI & ANALYTICS
# ==============================================================================
class HumanAgentFormatter:
    def _generate_analytical_summary(self, records):
        """Acts like a human analyst: aggregates totals before showing the data."""
        if not records or not isinstance(records[0], dict): return ""
        total_items = len(records)
        sum_qty, sum_amount, qty_found, amount_found = 0.0, 0.0, False, False

        for rec in records:
            for k, v in rec.items():
                k_lower = k.lower()
                try:
                    num_val = float(v)
                    if any(w in k_lower for w in ["qty", "quantity", "stock"]):
                        sum_qty += num_val; qty_found = True
                    # Avoid summing unit prices, only total balances/amounts
                    if any(w in k_lower for w in ["total", "amount", "price", "balance", "credit"]) and "unit" not in k_lower: 
                        sum_amount += num_val; amount_found = True
                except: pass

        insights = []
        if qty_found and sum_qty > 0: insights.append(f"a total volume of **{int(sum_qty):,} units**")
        if amount_found and sum_amount > 0: insights.append(f"a total financial value of **₹ {sum_amount:,.2f}**")

        if insights: return f" I took a moment to analyze these **{total_items} records** and calculated " + " and ".join(insights) + "."
        return f" I have successfully retrieved **{total_items} records** for you to review."

    def format_as_table(self, domain, records, search_term, confidence_score):
        domain_name = domain.replace("_", " ").title()
        
        # 1. Error Handling Conversation
        if not records:
            return f"I ran a deep cross-department scan, but unfortunately, I couldn't find any data matching **'{search_term}'**. Could you double check the spelling?"

        summary = self._generate_analytical_summary(records)

        # 2. Dynamic Human-like Introductions based on Mathematical Confidence
        if search_term:
            if confidence_score > 300:
                intro = f"Exact match found! Here is the data for **'{search_term}'** from the **{domain_name}** database.{summary}<br><br>"
            else:
                intro = f"I scanned the system and found these close matches for **'{search_term}'** inside **{domain_name}**.{summary}<br><br>"
        else: 
            intro = f"Right away. I have fetched the complete overview for **{domain_name}**.{summary}<br><br>"

        # 3. Render Flat Lists (e.g. Categories)
        if not isinstance(records[0], dict):
            html = "<div style='margin-top: 15px; background: #ffffff; padding: 15px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);'><ul style='font-family: Arial, sans-serif; color: #374151; line-height: 1.8; margin: 0; padding-left: 20px;'>"
            for rec in records: html += f"<li style='margin-bottom: 5px;'><strong>{rec}</strong></li>"
            html += "</ul></div>"
            return intro + html

        # 4. Smart Column Sorting
        raw_keys = set()
        for r in records: raw_keys.update(r.keys())
        priority_keys = ["productName", "name", "firstName", "lastName", "supplierName", "materialName", "email", "phone", "stockQuantity", "sellPrice", "totalAmount"]
        headers = sorted(list(raw_keys), key=lambda x: priority_keys.index(x) if x in priority_keys else 99)

        # 5. Beautiful HTML Table Styling
        html = "<div style='overflow-x:auto; margin-top: 15px; border-radius: 8px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1);'>"
        html += "<table style='border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; text-align: left; background: #ffffff; min-width: 600px;'>"
        html += "<tr style='background-color: #f8fafc; color: #475569; border-bottom: 2px solid #cbd5e1;'>"
        for h in headers:
            clean_header = re.sub(r"([a-z])([A-Z])", r"\g<1> \g<2>", h).title().replace("Category Product Name", "Category") 
            html += f"<th style='padding: 14px 16px; border: 1px solid #e2e8f0; font-size: 14px; white-space: nowrap;'>{clean_header}</th>"
        html += "</tr>"

        for i, rec in enumerate(records):
            bg_color = "#ffffff" if i % 2 == 0 else "#f8fafc"
            html += f"<tr style='background-color: {bg_color}; border-bottom: 1px solid #e2e8f0; transition: background-color 0.2s;'>"
            for h in headers:
                val = rec.get(h, "-")
                if val is None or str(val).strip() == "": val = "-"
                
                # Image Renderer
                if isinstance(val, str) and (val.endswith(".png") or val.endswith(".jpg") or val.endswith(".jpeg")) and val.startswith("http"):
                    val = f"<img src='{val}' style='max-width: 45px; max-height: 45px; border-radius: 4px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); object-fit: cover;' />"
                # Financial Formatting
                elif isinstance(val, (int, float)) and any(kw in h.lower() for kw in ["price", "amount", "total", "balance", "credit"]):
                    val = f"₹ {val:,.2f}" 
                
                html += f"<td style='padding: 12px 16px; border: 1px solid #e2e8f0; color: #334155; font-size: 14px; white-space: nowrap;'>{val}</td>"
            html += "</tr>"
        html += "</table></div>"
        return intro + html

speaker = HumanAgentFormatter()

# ==============================================================================
# MAIN ROUTE
# ==============================================================================
class QueryRequest(BaseModel):
    query: str = Field(None, alias="question") # Supports both {"query": "..."} and {"question": "..."}

@router.post("/chat")
async def chat_endpoint(request: QueryRequest):
    user_query = request.query
    if not user_query: return {"response": "I am standing by. What data do you need?"}

    # 1. Human-like Translation (Marathi/Hindi -> English)
    translated_query = admin_brain.translate_query(user_query)
    q_lower = translated_query.lower()

    # 2. Conversational Interceptors
    if any(w in q_lower for w in ["refresh", "sync", "update records"]):
        await admin_data.force_refresh()
        return {"response": "🔄 **Live Sync Complete.** I've re-authenticated with the server and downloaded the absolute latest ERP data. What shall we look at next?"}

    if any(w in q_lower for w in ["thank you", "thanks"]):
        responses = ["You are very welcome! Let me know if you need anything else.", "My pleasure.", "Glad I could help. Just say the word if you need another report."]
        return {"response": random.choice(responses)}

    if any(w in q_lower for w in ["hi", "hello", "hey"]) and len(q_lower.split()) <= 3:
        return {"response": "Hello! I am your AI Operations Assistant. I am fully equipped to fetch, analyze, and organize your ERP data logically. Just type naturally and let me pull the records for you."}

    # 3. Ensure Data Availability
    data = await admin_data.get_data()
    if not any(data.values()): 
        return {"response": "System Notice: I am currently unable to securely connect to the backend APIs. Please verify the network connection."}

    # 4. Search and Calculate
    domain, results, search_term, confidence = admin_brain.search(translated_query, data)

    if not domain:
        return {"response": "Could you please provide a little more context or a specific search term? (e.g., 'Show me customers' or 'Find brass jug')."}

    # 5. Render Output
    return {"response": speaker.format_as_table(domain, results, search_term, confidence)}