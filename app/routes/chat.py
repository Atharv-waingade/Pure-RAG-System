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
        self.ready = False
        self.last_fetched = 0

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
# 2. HYBRID FUZZY-LOGIC ENGINE 
# ==============================================================================
class HybridSearchEngine:
    def __init__(self):
        self.ignore_words = {"show", "me", "find", "get", "what", "is", "are", "the", "a", "of", "for", "to", "all", "list", "details", "search", "available", "currently", "data", "history", "could", "you", "please", "fetch", "check", "display", "tell", "about", "can", "i", "see", "any", "some", "give"}
        
        self.domain_map = {
            "product_stocks": ["stock", "inventory", "product", "item", "warehouse"],
            "supplier_purchases": ["purchase history", "supplier history"], 
            "suppliers": ["supplier", "vendor", "distributor"],
            "purchases": ["purchase", "invoice", "order", "bought", "receipt"],
            "customers": ["customer", "client", "buyer"],
            "materials": ["material", "raw", "fabric", "component"],
            "supplier_credits": ["credit", "refund", "owe", "due", "money owed"],
            "supplier_ledger": ["ledger", "statement", "balance", "account"],
            "categories": ["category", "categories", "types"]
        }

    def translate_query(self, query):
        try: return GoogleTranslator(source='auto', target='en').translate(query)
        except: return query

    def _extract_search_term(self, query):
        q_clean = re.sub(r'[^a-zA-Z0-9\s-]', '', query.lower())
        tokens = q_clean.split()
        target_tokens = [t for t in tokens if t not in self.ignore_words and not any(t in words for words in self.domain_map.values())]
        return " ".join(target_tokens).strip()

    def route_query(self, query):
        q = query.lower()
        for domain, keywords in self.domain_map.items():
            if any(w in q for w in keywords): return domain
        return None 

    def search_dataset(self, dataset, search_term):
        if not search_term: return dataset 
        search_words = search_term.lower().split()
        scored_matches = []

        for record in dataset:
            record_str = " ".join([str(v).lower() for v in record.values() if v is not None]) if isinstance(record, dict) else str(record).lower()

            if search_term.lower() in record_str: scored_matches.append((record, 1.0))
            elif all(w in record_str for w in search_words): scored_matches.append((record, 0.9))
            else:
                for word in search_words:
                    if len(word) > 4:
                        for rec_word in record_str.split():
                            if difflib.SequenceMatcher(None, word, rec_word).quick_ratio() > 0.8:
                                scored_matches.append((record, 0.7))
                                break

        scored_matches.sort(key=lambda x: x[1], reverse=True)
        unique_results, seen = [], set()
        for rec, score in scored_matches:
            if str(rec) not in seen:
                unique_results.append(rec)
                seen.add(str(rec))
            if len(unique_results) >= 25: break # Cap at 25 for Admin deep dives
            
        return unique_results

admin_brain = HybridSearchEngine()

# ==============================================================================
# 3. HUMAN INSIGHTS & FORMATTER (The "Brain")
# ==============================================================================
class HumanAgentFormatter:
    def _generate_analytical_summary(self, records):
        """Acts like a human analyst: calculates totals and quantities before showing the table."""
        if not records or not isinstance(records[0], dict): return ""
        
        total_items = len(records)
        sum_qty, sum_amount = 0.0, 0.0
        qty_found, amount_found = False, False

        for rec in records:
            for k, v in rec.items():
                k_lower = k.lower()
                # Safely try to cast values to floats if they look like numbers
                try:
                    num_val = float(v)
                    if any(w in k_lower for w in ["qty", "quantity", "stock"]):
                        sum_qty += num_val
                        qty_found = True
                    if any(w in k_lower for w in ["total", "amount", "price", "balance", "credit"]):
                        # Avoid summing "pricePerUnit", only total amounts
                        if "unit" not in k_lower: 
                            sum_amount += num_val
                            amount_found = True
                except: pass

        insights = []
        if qty_found and sum_qty > 0:
            insights.append(f"a total volume of **{int(sum_qty):,} units**")
        if amount_found and sum_amount > 0:
            insights.append(f"a total financial value of **₹ {sum_amount:,.2f}**")

        if insights:
            return f" I've analyzed these **{total_items} records** and calculated " + " and ".join(insights) + "."
        return f" I found **{total_items} records** for you."

    def format_as_table(self, domain, records, search_term, is_global=False):
        domain_name = domain.replace("_", " ").title()
        
        # 1. Dynamic Human Variations
        success_intros = [
            "Absolutely. ", "Right away. ", "Here is what you requested. ", 
            "I've pulled that up for you. ", "I have the data ready. "
        ]
        fail_intros = [
            "I've run a deep scan, but ", "I checked our current database, and ",
            "It seems "
        ]
        
        if not records:
            if search_term: return f"{random.choice(fail_intros)}I couldn't find any **{domain_name}** matching **'{search_term}'**. Would you like me to search for something else?"
            return f"{random.choice(fail_intros)}the **{domain_name}** database appears to be empty right now."

        # 2. Analytical Insight Generation
        summary = self._generate_analytical_summary(records)

        if is_global:
            intro = f"Since you didn't specify a category, I scanned the entire ERP system. I found this inside **{domain_name}** matching **'{search_term}'**.{summary}<br><br>"
        elif search_term:
            intro = f"{random.choice(success_intros)}Here is the filtered data for **'{search_term}'** in **{domain_name}**.{summary}<br><br>"
        else:
            intro = f"{random.choice(success_intros)}Here is the complete overview of all **{domain_name}**.{summary}<br><br>"

        # HANDLE FLAT STRINGS (Categories)
        if not isinstance(records[0], dict):
            html = "<div style='margin-top: 15px; background: #ffffff; padding: 15px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);'>"
            html += "<ul style='font-family: Arial, sans-serif; color: #374151; line-height: 1.8; margin: 0; padding-left: 20px;'>"
            for rec in records: html += f"<li style='margin-bottom: 5px;'><strong>{rec}</strong></li>"
            html += "</ul></div>"
            return intro + html

        # HANDLE DICTIONARIES (Tables)
        raw_keys = list(records[0].keys())
        hidden_keys = {"id", "_id", "createdAt", "updatedAt", "createdBy", "updatedBy", "__v", "role", "permissions", "password"}
        visible_keys = [k for k in raw_keys if k not in hidden_keys]

        # SMART SORTING
        priority_keys = ["productName", "name", "firstName", "lastName", "supplierName", "materialName", "email", "phone", "stockQuantity", "sellPrice", "totalAmount"]
        headers = sorted(visible_keys, key=lambda x: priority_keys.index(x) if x in priority_keys else 99)

        html = "<div style='overflow-x:auto; margin-top: 15px; border-radius: 8px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1);'>"
        html += "<table style='border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; text-align: left; background: #ffffff; min-width: 600px;'>"
        
        html += "<tr style='background-color: #f8fafc; color: #475569; border-bottom: 2px solid #cbd5e1;'>"
        for h in headers:
            clean_header = re.sub(r"([a-z])([A-Z])","\g<1> \g<2>", h).title().replace("Category Product Name", "Category") 
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
                
                html += f"<td style='padding: 12px 16px; border: 1px solid #e2e8f0; color: #334155; font-size: 14px;'>{val}</td>"
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

    # Dynamic Personality Routing
    if any(w in q_lower for w in ["refresh", "sync", "update records"]):
        await admin_data.force_refresh()
        return {"response": "🔄 **Live Sync Complete.** I've re-authenticated with the Umbrella Inventory Server and downloaded the latest data for you. What shall we look at next?"}

    if any(w in q_lower for w in ["thank you", "thanks"]):
        responses = ["You are very welcome!", "My pleasure.", "Glad I could help. Let me know if you need any other reports."]
        return {"response": random.choice(responses)}

    if any(w in q_lower for w in ["hi", "hello", "hey"]) and len(q_lower.split()) <= 3:
        return {"response": "Hello! I am your AI Administrative Assistant. Though I'm an AI and don't have feelings, I am fully equipped to fetch, analyze, and organize your ERP data. Just ask me to search for an invoice, check stock levels, or view a supplier."}

    data = await admin_data.get_data()
    if not any(data.values()): 
        return {"response": "System Notice: I am currently unable to securely connect to the backend APIs. Please verify the network status or credentials."}

    domain = admin_brain.route_query(translated_query)
    search_term = admin_brain._extract_search_term(translated_query)

    # Global Search Fallback
    if not domain:
        if not search_term:
            return {"response": "I'm ready when you are. Could you please specify which data you'd like to pull? (e.g., 'Show me customers' or 'Find purchase INV-123')."}
        
        best_results, best_domain = [], None
        for d_name, d_set in data.items():
            res = admin_brain.search_dataset(d_set, search_term)
            if len(res) > len(best_results):
                best_results, best_domain = res, d_name
        
        if best_results:
            return {"response": speaker.format_as_table(best_domain, best_results, search_term, is_global=True)}
        else:
            return {"response": f"I've searched across every department, but I couldn't find any records containing **'{search_term}'**. "}

    # Standard Filter
    dataset = data.get(domain, [])
    results = admin_brain.search_dataset(dataset, search_term)
    
    return {"response": speaker.format_as_table(domain, results, search_term)}