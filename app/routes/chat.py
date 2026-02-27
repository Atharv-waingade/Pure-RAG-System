from fastapi import APIRouter
from pydantic import BaseModel
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
LOGIN_PAYLOAD = {"username": "superadmin.com", "password": "superadmin@123"}
CACHE_TTL_SECONDS = 300  

# ==============================================================================
# HELPER: UNIVERSAL JSON FLATTENER
# ==============================================================================
def flatten_erp_data(dataset):
    if not dataset or not isinstance(dataset, list): return dataset
    if not isinstance(dataset[0], dict): return dataset 

    flat_data = []
    for record in dataset:
        scalars, nested_lists = {}, []
        for k, v in record.items():
            if isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
                nested_lists.append(v)
            elif not isinstance(v, (list, dict)):
                scalars[k] = v

        if nested_lists:
            for child_list in nested_lists:
                for child in child_list:
                    flat_record = {f"Category_{pk}" if pk in child else pk: pv for pk, pv in scalars.items()}
                    for ck, cv in child.items():
                        if isinstance(cv, dict): flat_record[ck] = cv.get("name", cv.get("title", "Data"))
                        elif not isinstance(cv, list): flat_record[ck] = cv
                    flat_data.append(flat_record)
        else:
            flat_record = {}
            for k, v in record.items():
                if isinstance(v, dict): flat_record[k] = v.get("name", v.get("title", "Data"))
                elif not isinstance(v, list): flat_record[k] = v
            flat_data.append(flat_record)
    return flat_data

# ==============================================================================
# 1. ENTERPRISE DATA MANAGER 
# ==============================================================================
class AdminDataManager:
    def __init__(self):
        self.data = {
            "materials": [], "purchases": [], "supplier_credits": [], 
            "customers": [], "suppliers": [], "supplier_ledger": [], 
            "product_stocks": [], "categories": [], "supplier_purchases": []
        }
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
                        print("🔑 SYSTEM: Authentication successful.")
                        return True
            return False
        except: return False

    async def _load_all_data(self):
        print("🔄 SYSTEM: Pulling fresh live data from ERP...")
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
        print("✅ SYSTEM: Data Cached & Flattened successfully.")

admin_data = AdminDataManager()

# ==============================================================================
# 2. UNIVERSAL NLP SEARCH ENGINE (Zero Hardcoded Routing Rules)
# ==============================================================================
class UniversalNLPEngine:
    def __init__(self):
        # NLP Stopwords: These are universally ignored to extract pure intent.
        self.stopwords = {
            "show", "me", "find", "get", "what", "is", "are", "the", "a", "an", "of", "for", "to", "all", "list", 
            "details", "search", "available", "currently", "data", "history", "could", "you", "please", 
            "fetch", "check", "display", "tell", "about", "can", "i", "see", "any", "some", "give", "that", 
            "right", "now", "we", "have", "in", "stock", "which", "do", "does", "want", "need", "like", 
            "would", "my", "our", "those", "these", "there", "their", "here", "it", "on", "at", "by", "with", "looking"
        }
        
        # Used ONLY to detect if the user asked for a whole category (e.g. "show all materials")
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
        """
        No guessing. Scans all datasets universally. Scores tokens mathematically. 
        """
        # 1. Clean and Tokenize
        q_clean = re.sub(r'[^a-zA-Z0-9\s]', ' ', query.lower())
        tokens = [w for w in q_clean.split() if w not in self.stopwords]
        
        if not tokens:
            return None, [], ""

        # 2. Check Intent: Did they just ask for an entire database? (e.g. "get me all materials")
        for domain, syns in self.category_synonyms.items():
            if any(t in syns for t in tokens):
                # Check if EVERY meaningful word they typed belongs to this category's name
                remaining = [t for t in tokens if t not in syns]
                if not remaining:
                    return domain, datasets.get(domain, []), "" # Return whole database

        # 3. Universal Deep Search: Score every record across ALL databases based on tokens
        search_str = " ".join(tokens)
        domain_results = {}

        for domain_name, records in datasets.items():
            matched_records = []
            for rec in records:
                rec_str = " ".join([str(v).lower() for v in rec.values() if v is not None]) if isinstance(rec, dict) else str(rec).lower()
                # Split record into distinct words to prevent "owe" matching "flower"
                rec_tokens = set(re.sub(r'[^a-zA-Z0-9\s]', ' ', rec_str).split())
                
                score = 0
                
                # Bonus for exact phrase
                if search_str in rec_str: score += 100
                
                # Individual word matching
                for t in tokens:
                    if t in rec_tokens: 
                        score += 10 # Exact distinct word match
                    else:
                        # Typo tolerance for longer words
                        if len(t) > 3:
                            for rt in rec_tokens:
                                if difflib.SequenceMatcher(None, t, rt).ratio() > 0.85:
                                    score += 5
                                    break
                                    
                if score > 0:
                    matched_records.append((rec, score))
            
            if matched_records:
                # Sort by highest score
                matched_records.sort(key=lambda x: x[1], reverse=True)
                highest_score = matched_records[0][1]
                # Filter out garbage matches (must be at least 50% as good as the best match)
                filtered_records = [r[0] for r in matched_records if r[1] >= (highest_score * 0.5)]
                domain_results[domain_name] = {"records": filtered_records, "max_score": highest_score}

        # 4. Resolve the Best Database
        if not domain_results:
            return None, [], search_str

        # Pick the domain that had the absolute highest scoring matches
        best_domain = max(domain_results.keys(), key=lambda d: domain_results[d]["max_score"])
        return best_domain, domain_results[best_domain]["records"], search_str

admin_brain = UniversalNLPEngine()

# ==============================================================================
# 3. HUMAN INSIGHTS & FORMATTER
# ==============================================================================
class HumanAgentFormatter:
    def _generate_analytical_summary(self, records):
        if not records or not isinstance(records[0], dict): return ""
        total_items = len(records)
        sum_qty, sum_amount, qty_found, amount_found = 0.0, 0.0, False, False

        for rec in records:
            for k, v in rec.items():
                k_lower = k.lower()
                try:
                    num_val = float(v)
                    if any(w in k_lower for w in ["qty", "quantity", "stock"]):
                        sum_qty += num_val
                        qty_found = True
                    if any(w in k_lower for w in ["total", "amount", "price", "balance", "credit"]) and "unit" not in k_lower: 
                        sum_amount += num_val
                        amount_found = True
                except: pass

        insights = []
        if qty_found and sum_qty > 0: insights.append(f"a total volume of **{int(sum_qty):,} units**")
        if amount_found and sum_amount > 0: insights.append(f"a total financial value of **₹ {sum_amount:,.2f}**")

        if insights: return f" I've analyzed these **{total_items} records** and calculated " + " and ".join(insights) + "."
        return f" I have retrieved **{total_items} detailed records** for you."

    def format_as_table(self, domain, records, search_term):
        domain_name = domain.replace("_", " ").title()
        
        if not records:
            return f"I ran a deep scan across the ERP, but I couldn't find any data matching **'{search_term}'**. Please verify the spelling or try a broader term."

        summary = self._generate_analytical_summary(records)

        if search_term: intro = f"I searched the system and found the best matches for **'{search_term}'** in **{domain_name}**.{summary}<br><br>"
        else: intro = f"Absolutely. I have fetched the complete database for **{domain_name}**.{summary}<br><br>"

        # FLAT STRINGS (Categories)
        if not isinstance(records[0], dict):
            html = "<div style='margin-top: 15px; background: #ffffff; padding: 15px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);'><ul style='font-family: Arial, sans-serif; color: #374151; line-height: 1.8; margin: 0; padding-left: 20px;'>"
            for rec in records: html += f"<li style='margin-bottom: 5px;'><strong>{rec}</strong></li>"
            html += "</ul></div>"
            return intro + html

        # DICTIONARIES (Tables)
        raw_keys = set()
        for r in records: raw_keys.update(r.keys())
        
        # Kept extremely minimal to ensure full detail visibility
        hidden_keys = {"id", "_id", "__v", "password", "jwtToken", "role", "permissions"}
        visible_keys = [k for k in raw_keys if k not in hidden_keys]

        priority_keys = ["productName", "name", "firstName", "lastName", "supplierName", "materialName", "email", "phone", "stockQuantity", "sellPrice", "totalAmount"]
        headers = sorted(visible_keys, key=lambda x: priority_keys.index(x) if x in priority_keys else 99)

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
                
                if isinstance(val, str) and (val.endswith(".png") or val.endswith(".jpg") or val.endswith(".jpeg")) and val.startswith("http"):
                    val = f"<img src='{val}' style='max-width: 45px; max-height: 45px; border-radius: 4px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); object-fit: cover;' />"
                elif isinstance(val, str) and "T" in val and len(val) >= 19 and val.count("-") >= 2:
                    try: val = val.split("T")[0] + " " + val.split("T")[1][:5]
                    except: pass
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
    query: str

@router.post("/chat")
async def chat_endpoint(request: QueryRequest):
    translated_query = admin_brain.translate_query(request.query)
    q_lower = translated_query.lower()

    if any(w in q_lower for w in ["refresh", "sync", "update records"]):
        await admin_data.force_refresh()
        return {"response": "🔄 **Live Sync Complete.** I've re-authenticated with the server and downloaded the latest data. What shall we look at next?"}

    if any(w in q_lower for w in ["thank you", "thanks"]):
        return {"response": random.choice(["You are very welcome!", "My pleasure.", "Glad I could help."])}

    if any(w in q_lower for w in ["hi", "hello", "hey"]) and len(q_lower.split()) <= 3:
        return {"response": "Hello! I am your AI Administrative Assistant. I am fully equipped to fetch, analyze, and organize your ERP data without relying on rigid commands. Just type naturally and let me find the data for you."}

    data = await admin_data.get_data()
    if not any(data.values()): 
        return {"response": "System Notice: I am unable to securely connect to the backend APIs. Please verify the network."}

    # Universal Deep Search (No more brittle routing)
    domain, results, search_term = admin_brain.search(translated_query, data)

    if not domain:
        return {"response": "Could you please provide a little more context or a specific search term? (e.g., 'Show me customers' or 'Find brass jug')."}

    return {"response": speaker.format_as_table(domain, results, search_term)}