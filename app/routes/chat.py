import asyncio, re, time, difflib, random, os, math, sqlite3
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
# 1. LOCAL TRANSLATION DICTIONARY
# ==============================================================================
LOCAL_INTENT_MAP = {
    "show": ["daakhva", "dakhav", "dakhva", "दाखवा", "दाखव", "dikhao", "dekhao"],
    "stock": ["saatha", "satha", "साठा", "inventory", "maal", "mal"],
    "purchase": ["kharedi", "khardi", "खरेदी", "kharidi"],
    "supplier": ["vikreta", "purvathadar", "विक्रेता", "purvatha"],
    "all": ["sarva", "sagale", "सर्व", "सगळे", "sab", "pure"],
    "get": ["milan", "ghya", "ghyo", "घ्या"]
}

def private_preprocess(query):
    q = query.lower()
    for english_intent, synonyms in LOCAL_INTENT_MAP.items():
        for syn in synonyms:
            q = re.sub(rf"\b{syn}\b", english_intent, q)
    return q

# ==============================================================================
# 2. CUSTOM TF-IDF ENGINE
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
# 3. IN-MEMORY SQLITE MIRROR
# ==============================================================================
class SQLiteDataMirror:
    def __init__(self):
        self.conn = sqlite3.connect(':memory:', check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.last_sync = 0
        self._lock = asyncio.Lock()
        
        self.domain_profiles = {
            "Materials": "material materials fabric raw component item physical",
            "Purchases": "purchase purchases order bought buy",
            "Supplier Credits": "supplier credit credits refund owe due payment",
            "Customers": "customer customers client buyer purchaser",
            "Suppliers": "supplier suppliers vendor distributor provider",
            "Supplier Ledger": "supplier ledger statement account balance sheet history",
            "Product Stocks": "product stock stocks inventory quantity warehouse retail item available barcode",
            "Categories": "category categories type classification",
            "Purchase History": "supplier purchase history historical timeline",
            "Sells": "sell sells sold invoice sale sales retail dispatch",
            "Printers": "printer printers active machine printing device",
            "Email Configs": "email config emails configuration smtp mail",
            "Customer Ledgers": "customer ledger ledgers account history",
            "Supplier Payments": "payment payments paid settled history"
        }
        self.tfidf = LightweightTFIDF(self.domain_profiles)

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
                for list_key, child_list in list_children:
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

    async def sync(self, force=False):
        if not force and (time.time() - self.last_sync < CACHE_TTL): return True
        async with self._lock:
            if not force and (time.time() - self.last_sync < CACHE_TTL): return True
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                try:
                    auth = await client.post(LOGIN_URL, json=LOGIN_PAYLOAD)
                    token = auth.json().get("jwtToken")
                    if not token: return False
                    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
                except: return False
                
                endpoints = {
                    "Materials": "/api/material/get-all-materials",
                    "Purchases": "/api/purchase/get-all-purchases",
                    "Supplier Credits": "/api/supplier-credit/get-all-supplier-credits",
                    "Customers": "/api/customer/get-all-customers",
                    "Suppliers": "/api/supplier/get-all-suppliers",
                    "Supplier Ledger": "/api/reports/get-supplier-ledger",
                    "Product Stocks": "/api/reports/get-product-stocks-with-product",
                    "Categories": "/api/reports/get-all-product-categories",
                    "Purchase History": "/api/reports/supplier-purchase-history",
                    "Sells": "/api/sell/get-all-sells",
                    "Printers": "/api/printer/get-all-printers",
                    "Email Configs": "/api/email-config/get-all-emails",
                    "Customer Ledgers": "/api/reports/get-all-customer-ledgers",
                    "Supplier Payments": "/api/payment/supplier-payment-history"
                }
                
                tasks = [client.get(f"{BASE_URL}{url}", headers=headers) for url in endpoints.values()]
                responses = await asyncio.gather(*tasks, return_exceptions=True)
                
                for i, key in enumerate(endpoints.keys()):
                    res = responses[i]
                    if not isinstance(res, Exception) and res.status_code == 200:
                        data_list = self._extract_list(res.json())
                        flat_records = list(self._stream_flatten(data_list))
                        self._load_to_sql(key, flat_records)
            
            self.last_sync = time.time()
            return True

db = SQLiteDataMirror()

# ==============================================================================
# 4. SUPER INTELLIGENT QUERY PLANNER (ZERO RULES)
# ==============================================================================
class StructuredQueryPlanner:
    def __init__(self):
        self.stopwords = {"show", "me", "find", "get", "what", "is", "are", "the", "a", "an", "of", "for", "please", "can", "you", "tell", "details", "all", "we", "have", "our", "available", "availlable", "availble", "names", "list", "give", "in", "any", "by", "its", "their", "specific"}

    def execute_plan(self, query):
        clean_q = re.sub(r'[^a-zA-Z0-9\s-]', ' ', query.lower())
        tokens = [t for t in clean_q.split() if t not in self.stopwords]
        search_term = " ".join(tokens)
        
        if not tokens: return None, [], search_term

        scores = db.tfidf.score(search_term)
        best_table = max(scores, key=scores.get)
        if scores[best_table] == 0.0: best_table = "Product Stocks"

        cursor = db.conn.cursor()
        cursor.execute(f'PRAGMA table_info("{best_table}")')
        columns = [row['name'] for row in cursor.fetchall()]
        
        if not columns or columns == ["Notice"]:
            return best_table, [], search_term

        # --- DYNAMIC SCHEMA-AWARE COLUMN DETECTION (100% Rule-Free) ---
        intent_words = ["highest", "lowest", "most", "least", "maximum", "minimum", "top"]
        domain_keywords = db.domain_profiles[best_table].split()
        
        raw_filter_tokens = [w for w in search_term.split() if w not in intent_words]
        final_filter_tokens = [w for w in raw_filter_tokens if w not in domain_keywords]
        
        prompt_column = None
        
        # Mathematically scan the actual API column names to see if the user mentioned one
        for col in columns:
            # Splits camelCase (e.g. 'invoiceNo' -> ['invoice', 'no'])
            clean_col_words = re.sub(r"([a-z])([A-Z])", r"\1 \2", col).lower().split()
            
            for cw in clean_col_words:
                if len(cw) > 2 and cw in query.lower():
                    # Check if the user ONLY provided the column name, but gave no actual value
                    remaining_value_tokens = [t for t in final_filter_tokens if t not in clean_col_words]
                    if not remaining_value_tokens: 
                        prompt_column = re.sub(r"([a-z])([A-Z])", r"\1 \2", col).title()
                        break
            if prompt_column:
                break
                
        if prompt_column and not final_filter_tokens:
            return best_table, [], f"PROMPT_MISSING_PARAM:{prompt_column}"

        sql_clauses = {"WHERE": [], "ORDER BY": "", "LIMIT": ""}
        
        limit_match = re.search(r'top\s+(\d+)', search_term)
        if limit_match:
            sql_clauses["LIMIT"] = f"LIMIT {limit_match.group(1)}"

        numeric_cols = [c for c in columns if any(kw in c.lower() for kw in ["price", "amount", "qty", "quantity", "stock", "total", "balance"])]
        if numeric_cols:
            if any(w in search_term for w in ["highest", "most", "maximum", "top"]):
                sql_clauses["ORDER BY"] = f'ORDER BY CAST("{numeric_cols[0]}" AS REAL) DESC'
            elif any(w in search_term for w in ["lowest", "least", "minimum"]):
                sql_clauses["ORDER BY"] = f'ORDER BY CAST("{numeric_cols[0]}" AS REAL) ASC'

        # Safely remove the column names from the search string so we just search for the value
        if prompt_column:
            clean_col_words = prompt_column.lower().split()
            final_filter_tokens = [w for w in final_filter_tokens if w not in clean_col_words]
            
        filter_term = " ".join(final_filter_tokens).strip()

        if filter_term:
            conditions = [f'"{col}" LIKE ?' for col in columns]
            sql_clauses["WHERE"] = " OR ".join(conditions)

        base_sql = f'SELECT * FROM "{best_table}"'
        params = []
        
        if sql_clauses["WHERE"]:
            base_sql += f' WHERE {sql_clauses["WHERE"]}'
            params = [f"%{filter_term}%"] * len(columns)
            
        if sql_clauses["ORDER BY"]: base_sql += f' {sql_clauses["ORDER BY"]}'
        if sql_clauses["LIMIT"]: base_sql += f' {sql_clauses["LIMIT"]}'
        elif not sql_clauses["WHERE"]: base_sql += ' LIMIT 10000'

        try:
            cursor.execute(base_sql, params)
            records = [dict(row) for row in cursor.fetchall()]
            return best_table, records, filter_term 
        except Exception as e:
            print(f"SQL Error: {e}")
            return best_table, [], filter_term

planner = StructuredQueryPlanner()

# ==============================================================================
# 5. HUMAN CONVERSATIONAL GENERATOR
# ==============================================================================
class LLMStyleFormatter:
    def _calculate_insights(self, records):
        if not records: return None, None
        sum_qty, sum_amount = 0.0, 0.0
        
        for rec in records:
            amount_added = False
            for amt_key in ["totalAmount", "purchaseAmount", "creditAmount", "sellPrice"]:
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

    def create_human_response(self, domain, records, search_term):
        if search_term.startswith("PROMPT_MISSING_PARAM:"):
            missing_param = search_term.split(":")[1]
            return f"Sure! I can look that up for you in the {domain} database. Could you please specify the **{missing_param}**?"

        if not domain or not records:
            return f"I've searched our **{domain}** records, but I couldn't find anything matching **'{search_term}'**. It's possible the database is empty or the specific item isn't logged yet."

        total_items = len(records)
        qty, amount = self._calculate_insights(records)
        
        ack = random.choice(["I've got that for you. ", "Certainly! ", "Here is the data you requested. ", "All set. "])
        
        if search_term:
            action = f"I scanned the **{domain}** database for **'{search_term}'**"
            found = f" and found **{total_items} matching records**."
        else:
            action = f"I've pulled the complete list of **{domain}**"
            found = f", which currently contains **{total_items} records**."

        insight_text = ""
        if qty and amount:
            insight_text = f" I've also tallied the totals: these represent a combined volume of **{int(qty):,} units** with a total financial value of **₹{amount:,.2f}**."
        elif amount:
            insight_text = f" The total financial value across these records amounts to **₹{amount:,.2f}**."
        elif qty:
            insight_text = f" Combined, this accounts for a total of **{int(qty):,} units** in our system."

        closing = " Here is the detailed breakdown:<br><br>"
        intro_paragraph = ack + action + found + insight_text + closing

        raw_keys = set()
        for r in records: raw_keys.update(r.keys())
        visible_keys = [k for k in raw_keys if k not in {"id", "_id", "__v", "password", "jwtToken", "role", "permissions"}]
        
        priority_keys = ["invoiceNo", "productName", "name", "firstName", "lastName", "supplierName", "supplier", "product", "customer", "materialName", "email", "phone", "contact", "stockQuantity", "sellPrice", "totalAmount"]
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
                elif isinstance(val, (int, float)) and any(kw in h.lower() for kw in ["price", "amount", "total", "balance", "credit"]):
                    val = f"₹ {val:,.2f}" 
                html += f"<td style='padding:12px 16px; border:1px solid #e2e8f0; color:#334155; font-size:14px; white-space:nowrap;'>{val}</td>"
            html += "</tr>"
        return intro_paragraph + html + "</table></div>"

bot_voice = LLMStyleFormatter()

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

    if any(w in q_lower for w in ["refresh", "sync", "update records"]):
        await db.sync(force=True)
        return {"response": "🔄 **System Synced.** I have successfully connected to the ERP and downloaded the latest live data. What would you like to see?"}

    if q_lower in ["thank you", "thanks", "awesome", "perfect"]:
        return {"response": random.choice(["You're very welcome! Let me know if you need any other reports.", "My pleasure! I'm here if you need more data."])}

    greet_words = ["hi", "hello", "hey", "heyy", "heya", "good morning", "good afternoon"]
    if any(q_lower.startswith(g) for g in greet_words) and len(q_lower.split()) <= 3:
        return {"response": "Hello! I am your AI Operations Assistant. You can ask me to analyze inventory, find specific purchase invoices, or check supplier credits. How can I help you today?"}

    processed_query = private_preprocess(user_query)

    success = await db.sync()
    if not success: 
        return {"response": "System Notice: I am currently unable to securely connect to the ERP backend. Please verify your network."}

    domain, results, search_term = planner.execute_plan(processed_query)

    return {"response": bot_voice.create_human_response(domain, results, search_term)}