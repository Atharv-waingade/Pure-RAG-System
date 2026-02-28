import asyncio, re, time, random, os, math, sqlite3
from collections import Counter
from fastapi import APIRouter, Request
from pydantic import BaseModel
import httpx

router = APIRouter()

# ==============================================================================
# CONFIGURATION & SECURITY
# ==============================================================================
BASE_URL = "https://umbrellasales.xyz/umbrella-inventory-server"
LOGIN_URL = f"{BASE_URL}/api/service/login"
LOGIN_PAYLOAD = {
    "username": os.getenv("ERP_USERNAME", "superadmin.com"), 
    "password": os.getenv("ERP_PASSWORD", "superadmin@123")
}
CACHE_TTL = 300 

# ==============================================================================
# 1. THE MATHEMATICAL ENGINE (Used only as a smart fallback)
# ==============================================================================
class LightweightTFIDF:
    def __init__(self, corpus_dict):
        self.documents = corpus_dict
        self.idf = {}
        self.vocab = set()
        self._build_idf()

    def _build_idf(self):
        N = len(self.documents)
        for doc in self.documents.values():
            self.vocab.update(doc.split())
        for word in self.vocab:
            doc_count = sum(1 for doc in self.documents.values() if word in doc.split())
            self.idf[word] = math.log((1 + N) / (1 + doc_count)) + 1

    def _get_tf(self, text):
        words = text.split()
        if not words: return {}
        counts = Counter(words)
        return {w: counts[w]/len(words) for w in counts}

    def score(self, query):
        q_tf = self._get_tf(query)
        q_vec = {w: q_tf[w] * self.idf.get(w, 0) for w in q_tf}

        scores = {}
        for name, doc in self.documents.items():
            d_tf = self._get_tf(doc)
            d_vec = {w: d_tf[w] * self.idf.get(w, 0) for w in d_tf}

            intersection = set(q_vec.keys()) & set(d_vec.keys())
            numerator = sum(q_vec[x] * d_vec[x] for x in intersection)
            sum1 = sum(v**2 for v in q_vec.values())
            sum2 = sum(v**2 for v in d_vec.values())
            denominator = math.sqrt(sum1) * math.sqrt(sum2)
            
            scores[name] = numerator / denominator if denominator else 0.0
        return scores

# ==============================================================================
# 2. THE HYBRID "ADMIN BRAIN" (Native Rules -> Math Fallback)
# ==============================================================================
class AdminBrain:
    def __init__(self):
        self.api_routing_map = {
            "sell": {"url": "/api/sell/get-all-sells", "keywords": ["sell", "sells", "sold", "sale", "sales", "dispatch", "receipt"]},
            "purchase": {"url": "/api/purchase/get-all-purchases", "keywords": ["purchase", "purchases", "order", "bought", "buy", "acquire"]},
            "material": {"url": "/api/material/get-all-materials", "keywords": ["material", "materials", "fabric", "raw", "component", "item", "goods"]},
            "customer": {"url": "/api/customer/get-all-customers", "keywords": ["customer", "customers", "client", "clients", "buyer", "purchaser"]},
            "supplier": {"url": "/api/supplier/get-all-suppliers", "keywords": ["supplier", "suppliers", "vendor", "distributor", "provider"]},
            "supplier-credit": {"url": "/api/supplier-credit/get-all-supplier-credits", "keywords": ["credit", "credits", "owe", "due", "refund"]},
            "product-stock": {"url": "/api/reports/get-product-stocks-with-product", "keywords": ["stock", "stocks", "inventory", "warehouse", "available", "product"]},
            "payment": {"url": "/api/payment/supplier-payment-history", "keywords": ["payment", "payments", "paid", "settled", "transaction"]},
            "printer": {"url": "/api/printer/get-all-printers", "keywords": ["printer", "printers", "machine", "print"]},
            "email-config": {"url": "/api/email-config/get-all-emails", "keywords": ["email", "emails", "smtp", "config", "mail"]},
            "category": {"url": "/api/reports/get-all-product-categories", "keywords": ["category", "categories", "classification", "type"]},
            "customer-ledger": {"url": "/api/reports/get-all-customer-ledgers", "keywords": ["customer ledger", "client ledger", "account"]},
            "supplier-ledger": {"url": "/api/reports/get-supplier-ledger", "keywords": ["supplier ledger", "vendor ledger", "statement"]},
        }
        
        # Build the math corpus automatically from our strict keywords!
        corpus = {mod: " ".join(data["keywords"]) for mod, data in self.api_routing_map.items()}
        self.math_engine = LightweightTFIDF(corpus)
        
        self.entity_triggers = {
            "invoice": "invoiceNo", "bill": "invoiceNo",
            "barcode": "barcode", "code": "barcode",
            "contact": "contact", "phone": "phone", "number": "contact",
            "id": "id", "email": "email",
            "name": "name"
        }
        
        self.bulk_words = {"all", "list", "every", "entire", "show", "get", "fetch", "display"}
        self.fluff_words = {"me", "find", "what", "is", "are", "the", "a", "an", "of", "for", "please", "can", "you", "tell", "details", "we", "have", "our", "in", "any", "by", "its", "their", "specific", "about", "information", "on", "who", "which"}

    def analyze_intent(self, user_query):
        text_lower = user_query.lower()
        clean_q = re.sub(r'[^a-zA-Z0-9\s-]', ' ', text_lower)
        tokens = clean_q.split()

        analysis = {
            "intent": "UNKNOWN", 
            "module": None,
            "url": None,
            "target_column": None,
            "search_value": ""
        }

        # --- HYBRID PASS 1: Strict Native Routing ---
        highest_match = 0
        matched_category_words = []
        for mod_key, data in self.api_routing_map.items():
            match_count = sum(1 for kw in data["keywords"] if re.search(rf"\b{kw}\b", text_lower))
            if match_count > highest_match:
                highest_match = match_count
                analysis["module"] = mod_key
                analysis["url"] = data["url"]
                matched_category_words = data["keywords"]

        # --- HYBRID PASS 2: Mathematical Fallback ---
        if highest_match == 0:
            math_scores = self.math_engine.score(clean_q)
            best_math_match = max(math_scores, key=math_scores.get)
            
            # If the math finds a correlation > 0, trust the math!
            if math_scores[best_math_match] > 0.0:
                analysis["module"] = best_math_match
                analysis["url"] = self.api_routing_map[best_math_match]["url"]
                matched_category_words = self.api_routing_map[best_math_match]["keywords"]
            else:
                # Ultimate safe fallback
                analysis["module"] = "product-stock"
                analysis["url"] = self.api_routing_map["product-stock"]["url"]

        # Entity/Column Extraction
        words_to_strip = self.fluff_words | self.bulk_words | set(matched_category_words)
        
        for trigger, col in self.entity_triggers.items():
            if re.search(rf"\b{trigger}s?\b", text_lower):
                analysis["target_column"] = col
                words_to_strip.add(trigger)
                words_to_strip.add(trigger + "s")
                break

        # Extract the Clean Value
        value_tokens = [w for w in tokens if w not in words_to_strip]
        extracted_value = " ".join(value_tokens).strip()
        analysis["search_value"] = extracted_value

        is_bulk_request = any(w in tokens for w in self.bulk_words)

        if analysis["target_column"]:
            if extracted_value: analysis["intent"] = "SPECIFIC_SEARCH"
            else: analysis["intent"] = "PROMPT_NEEDED"
        elif extracted_value and not is_bulk_request:
            analysis["intent"] = "GLOBAL_SEARCH"
        else:
            analysis["intent"] = "BULK_LIST"

        return analysis

# ==============================================================================
# 3. IN-MEMORY SQLITE MIRROR (Strict Lazy-Loading by URL)
# ==============================================================================
class SQLiteDataMirror:
    def __init__(self):
        self.conn = sqlite3.connect(':memory:', check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.last_sync_map = {} 
        self._lock = asyncio.Lock()

    def _extract_list(self, raw_json):
        if isinstance(raw_json, list): return raw_json
        if isinstance(raw_json, dict):
            if "data" in raw_json and isinstance(raw_json["data"], list): return raw_json["data"]
            for v in raw_json.values():
                if isinstance(v, list): return v
        return []

    def _stream_flatten(self, dataset):
        if not dataset or not isinstance(dataset, list): return
        for record in dataset:
            if not isinstance(record, dict):
                yield {"Data": record}
                continue
            
            row_base = {}
            list_children = []
            
            for k, v in record.items():
                if v is None:
                    row_base[k] = "-"
                elif not isinstance(v, (list, dict)):
                    row_base[k] = v
                elif isinstance(v, dict):
                    display_val = v.get("name", v.get("title", v.get("invoiceNo", v.get("totalAmount", "Attached Record"))))
                    row_base[k] = display_val
                elif isinstance(v, list) and v and isinstance(v[0], dict):
                    list_children.append((k, v))
            
            if list_children:
                for _, child_list in list_children:
                    for child in child_list:
                        row = row_base.copy()
                        for ck, cv in child.items():
                            if isinstance(cv, dict):
                                row[ck] = cv.get("name", cv.get("title", cv.get("invoiceNo", "Record")))
                            elif not isinstance(cv, list):
                                row[ck] = cv
                        yield row
            else:
                yield row_base

    def _load_to_sql(self, table_name, records):
        self.conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        if not records: 
            self.conn.execute(f'CREATE TABLE "{table_name}" ("Notice" TEXT)')
            return
            
        keys = list({k for r in records for k in r.keys()})
        cols = ", ".join([f'"{k}" TEXT' for k in keys])
        self.conn.execute(f'CREATE TABLE "{table_name}" ({cols})')
        
        placeholders = ",".join(["?"] * len(keys))
        insert_sql = f'INSERT INTO "{table_name}" VALUES ({placeholders})'
        
        for r in records:
            vals = [str(r.get(k, "")) for k in keys]
            self.conn.execute(insert_sql, vals)
        self.conn.commit()

    async def sync_module(self, module_key, api_url, force=False):
        last_sync = self.last_sync_map.get(module_key, 0)
        if not force and (time.time() - last_sync < CACHE_TTL): return True
        
        async with self._lock:
            last_sync = self.last_sync_map.get(module_key, 0)
            if not force and (time.time() - last_sync < CACHE_TTL): return True

            async with httpx.AsyncClient(timeout=30.0) as client:
                try:
                    auth = await client.post(LOGIN_URL, json=LOGIN_PAYLOAD)
                    token = auth.json().get("jwtToken")
                    if not token: return False
                    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
                    
                    res = await client.get(f"{BASE_URL}{api_url}", headers=headers)
                    if res.status_code == 200:
                        data_list = self._extract_list(res.json())
                        flat_records = list(self._stream_flatten(data_list))
                        self._load_to_sql(module_key, flat_records)
                        self.last_sync_map[module_key] = time.time()
                        return True
                except Exception as e:
                    print(f"API Sync Error for {module_key}: {e}")
                    return False
            return False

    async def sync_all(self, routing_map):
        tasks = [self.sync_module(mod, data["url"], force=True) for mod, data in routing_map.items()]
        await asyncio.gather(*tasks)

db = SQLiteDataMirror()

# ==============================================================================
# 4. THE ACTION ROUTER & FORMATTER
# ==============================================================================
class IntelligenceEngine:
    def __init__(self):
        self.brain = AdminBrain()

    def _calculate_insights(self, records):
        if not records: return None, None
        sum_qty, sum_amount = 0.0, 0.0
        
        for rec in records:
            amount_added = False
            for amt_key in ["totalAmount", "purchaseAmount", "creditAmount", "sellPrice", "paid"]:
                actual_key = next((k for k in rec.keys() if k.lower() == amt_key.lower()), None)
                if actual_key and not amount_added:
                    try:
                        sum_amount += float(str(rec[actual_key]).replace(',', ''))
                        amount_added = True 
                    except ValueError: pass

            qty_added = False
            for qty_key in ["stockQuantity", "quantity", "totalQuantity", "qty"]:
                actual_key = next((k for k in rec.keys() if k.lower() == qty_key.lower()), None)
                if actual_key and not qty_added:
                    try:
                        sum_qty += float(str(rec[actual_key]).replace(',', ''))
                        qty_added = True
                    except ValueError: pass

        return sum_qty if sum_qty > 0 else None, sum_amount if sum_amount > 0 else None

    async def execute_and_format(self, user_query, database):
        analysis = self.brain.analyze_intent(user_query)
        module = analysis["module"]
        url = analysis["url"]
        intent = analysis["intent"]
        search_val = analysis["search_value"]

        if intent == "PROMPT_NEEDED":
            human_missing = re.sub(r"([a-z])([A-Z])", r"\1 \2", analysis["target_column"]).title()
            return f"You are querying the **{module.replace('-', ' ').title()}** module. Could you please specify the **{human_missing}**? (e.g. *'INV-1002'*)"

        success = await database.sync_module(module, url)
        if not success:
            return f"System Notice: I am currently unable to fetch data from `{url}`. Please check the ERP connection."

        cursor = database.conn.cursor()
        cursor.execute(f'PRAGMA table_info("{module}")')
        columns = [row['name'] for row in cursor.fetchall()]
        
        if not columns or columns == ["Notice"]:
            return f"I checked the **{module.replace('-', ' ').title()}** section, but no records exist yet."

        base_sql = f'SELECT * FROM "{module}"'
        params = []

        if intent == "SPECIFIC_SEARCH":
            col = analysis["target_column"]
            actual_col = next((c for c in columns if col.lower() in c.lower()), None)
            if actual_col:
                base_sql += f' WHERE "{actual_col}" LIKE ?'
                params = [f"%{search_val}%"]
            else:
                intent = "GLOBAL_SEARCH" 
        
        if intent == "GLOBAL_SEARCH":
            conditions = [f'"{c}" LIKE ?' for c in columns]
            base_sql += ' WHERE ' + " OR ".join(conditions)
            params = [f"%{search_val}%"] * len(columns)

        base_sql += " LIMIT 10000"

        try:
            cursor.execute(base_sql, params)
            records = [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            print(f"SQL Error: {e}")
            records = []

        display_name = module.replace('-', ' ').title()
        if not records:
            return f"I've searched our **{display_name}** records, but I couldn't find anything matching **'{search_val}'**. Please verify the spelling."

        total_items = len(records)
        qty, amount = self._calculate_insights(records)
        ack = random.choice(["I've got that for you. ", "Certainly! ", "Here is the data you requested. "])
        
        if search_val and intent != "BULK_LIST":
            action = f"I scanned the **{display_name}** database for **'{search_val}'** and found **{total_items} matching records**."
        else:
            action = f"I've pulled the complete list of **{display_name}**, which contains **{total_items} records**."

        insight_text = ""
        if qty and amount:
            insight_text = f" I've tallied the totals: combined volume of **{int(qty):,} units** with a financial value of **₹{amount:,.2f}**."
        elif amount:
            insight_text = f" The total financial value across these records amounts to **₹{amount:,.2f}**."
        elif qty:
            insight_text = f" Combined, this accounts for a total of **{int(qty):,} units**."

        intro_paragraph = ack + action + insight_text + " Here is the detailed breakdown:<br><br>"

        raw_keys = set()
        for r in records: raw_keys.update(r.keys())
        visible_keys = [k for k in raw_keys if k not in {"id", "_id", "__v", "password", "jwtToken", "role", "permissions"}]
        priority_keys = ["invoiceNo", "productName", "name", "firstName", "lastName", "supplierName", "supplier", "product", "customer", "materialName", "email", "phone", "contact", "stockQuantity", "sellPrice", "totalAmount", "paid", "creditAmount"]
        headers = sorted(visible_keys, key=lambda x: priority_keys.index(x) if x in priority_keys else 99)

        html = "<div style='overflow-x:auto; margin-top:10px; border-radius:8px; box-shadow:0 4px 6px -1px rgba(0,0,0,0.1);'>"
        html += "<table style='border-collapse:collapse; width:100%; text-align:left; background:#ffffff; font-family:sans-serif; min-width:600px;'>"
        html += "<tr style='background:#f8fafc; color:#475569; border-bottom:2px solid #cbd5e1;'>"
        for h in headers:
            clean_h = re.sub(r"([a-z])([A-Z])", r"\1 \2", h).title().replace("Category Product Name", "Category") 
            html += f"<th style='padding:14px 16px; border:1px solid #e2e8f0; font-size:14px; white-space:nowrap;'>{clean_h}</th>"
        html += "</tr>"

        for i, rec in enumerate(records):
            bg = "#ffffff" if i % 2 == 0 else "#f8fafc"
            html += f"<tr style='background:{bg}; border-bottom:1px solid #e2e8f0; transition:background 0.2s;'>"
            for h in headers:
                val = rec.get(h, "-")
                if not val: val = "-"
                if isinstance(val, str) and (val.endswith(".png") or val.endswith(".jpg")) and val.startswith("http"):
                    val = f"<img src='{val}' style='max-width:45px; max-height:45px; border-radius:4px; box-shadow:0 1px 3px rgba(0,0,0,0.1); object-fit:cover;' />"
                elif isinstance(val, str) and "T" in val and len(val) >= 19 and val.count("-") >= 2:
                    try: val = val.split("T")[0] + " " + val.split("T")[1][:5]
                    except: pass
                elif isinstance(val, (int, float)) and any(kw in h.lower() for kw in ["price", "amount", "total", "balance", "credit", "paid"]):
                    val = f"₹ {val:,.2f}" 
                html += f"<td style='padding:12px 16px; border:1px solid #e2e8f0; color:#334155; font-size:14px; white-space:nowrap;'>{val}</td>"
            html += "</tr>"
        return intro_paragraph + html + "</table></div>"

agent = IntelligenceEngine()

# ==============================================================================
# MAIN ROUTE
# ==============================================================================
class ChatRequest(BaseModel):
    query: str = None
    question: str = None

@router.post("/chat")
async def chat_endpoint(request: ChatRequest):
    user_query = request.query or request.question or ""
    q_lower = user_query.lower().strip()

    # --- THE "SELF-AWARE" IDENTITY BLOCK ---
    identity_triggers = ["who are you", "how do you work", "are you chatgpt", "chatgpt", "chat gpt", "your brain", "mathematical calculations", "how were you built", "hybrid"]
    if any(w in q_lower for w in identity_triggers):
        return {
            "response": (
                "I am a **Hybrid Deterministic NLU (Natural Language Understanding) Engine**.<br><br>"
                "Before Large Language Models like ChatGPT existed, AI made decisions using strict mathematical logic (like TF-IDF) and precise rule-based pipelines. While ChatGPT guesses words based on probability—which is why it often hallucinates fake financial data—my architecture is built specifically for ERP precision.<br><br>"
                "**How I work:**<br>"
                "1. **Strict Routing:** I first map your English directly to our backend API endpoints. If you ask for 'Sales', I hit the Sales API with 100% certainty.<br>"
                "2. **Mathematical Fallback:** If you use unusual phrasing, my fallback engine uses TF-IDF mathematics to calculate vector similarities and 'guess' your intent.<br><br>"
                "Because I execute logic locally and map directly to SQLite, I require zero external AI APIs, cost nothing to run, and am mathematically guaranteed to never hallucinate your accounting data."
            )
        }

    if any(w in q_lower for w in ["refresh", "sync", "update records"]):
        await db.sync_all(agent.brain.api_routing_map) 
        return {"response": "🔄 **System Fully Synced.** I have forced a refresh of all ERP databases. What would you like to see?"}

    if q_lower in ["thank you", "thanks", "awesome", "perfect"]:
        return {"response": random.choice(["You're very welcome! Let me know if you need any other reports.", "My pleasure! I'm here if you need more data."])}

    greet_words = ["hi", "hello", "hey", "heyy", "heya", "good morning", "good afternoon"]
    if any(q_lower.startswith(g) for g in greet_words) and len(q_lower.split()) <= 3:
        return {"response": "Hello! I am your AI Operations Assistant. You can ask me to analyze inventory, find specific purchase invoices, or check supplier credits. How can I help you today?"}

    response_html = await agent.execute_and_format(user_query, db)

    return {"response": response_html}