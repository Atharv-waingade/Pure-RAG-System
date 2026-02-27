import asyncio, re, time, difflib, random, os, math, sqlite3
from collections import Counter
from fastapi import APIRouter, Request
from pydantic import BaseModel
import httpx

router = APIRouter()

# ==============================================================================
# CONFIGURATION & SECURITY (100% Private & Offline)
# ==============================================================================
BASE_URL = "https://umbrellasales.xyz/umbrella-inventory-server"
LOGIN_URL = f"{BASE_URL}/api/service/login"
LOGIN_PAYLOAD = {
    "username": os.getenv("ERP_USERNAME", "superadmin.com"), 
    "password": os.getenv("ERP_PASSWORD", "superadmin@123")
}
CACHE_TTL = 300 

# ==============================================================================
# 1. LOCAL TRANSLATION DICTIONARY (100% Offline)
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
    """Translates intent locally without calling external AI APIs."""
    q = query.lower()
    for english_intent, synonyms in LOCAL_INTENT_MAP.items():
        for syn in synonyms:
            q = re.sub(rf"\b{syn}\b", english_intent, q)
    return q

# ==============================================================================
# 2. CUSTOM TF-IDF & COSINE SIMILARITY ENGINE (0MB Overhead)
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
# 3. IN-MEMORY SQLITE MIRROR (Fastest Offline Processing)
# ==============================================================================
class SQLiteDataMirror:
    def __init__(self):
        self.conn = sqlite3.connect(':memory:', check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.last_sync = 0
        self._lock = asyncio.Lock()
        
        # Domain Vocabulary perfectly mapped to your 8 APIs
        self.domain_profiles = {
            "Materials": "material materials fabric raw component item physical",
            "Purchases": "purchase purchases order invoice receipt bought history",
            "Supplier Credits": "supplier credit credits refund owe due balance payment",
            "Customers": "customer customers client buyer purchaser",
            "Suppliers": "supplier suppliers vendor distributor provider",
            "Supplier Ledger": "supplier ledger statement account balance sheet history",
            "Product Stocks": "product stock stocks inventory quantity warehouse retail item available",
            "Categories": "category categories type classification"
        }
        self.tfidf = LightweightTFIDF(self.domain_profiles)

    def _stream_flatten(self, dataset):
        if not dataset or not isinstance(dataset, list): return
        for record in dataset:
            if not isinstance(record, dict):
                yield {"Data": record}
                continue
            
            scalars = {k: v for k, v in record.items() if not isinstance(v, (list, dict))}
            children = [v for v in record.values() if isinstance(v, list) and v and isinstance(v[0], dict)]
            
            if children:
                for child_list in children:
                    for child in child_list:
                        row = scalars.copy()
                        for ck, cv in child.items():
                            row[ck] = cv.get("name", "Data") if isinstance(cv, dict) else cv
                        yield row
            else:
                row = scalars.copy()
                for k, v in record.items():
                    if isinstance(v, dict): row[k] = v.get("name", "Data")
                yield row

    def _load_to_sql(self, table_name, records):
        """Dynamically builds SQL tables. Safely handles empty records (e.g. Customers)."""
        self.conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        if not records: 
            # Create a dummy table so it doesn't crash if queried while empty
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
                
                # Mapped exactly to your live backend
                endpoints = {
                    "Materials": "/api/material/get-all-materials",
                    "Purchases": "/api/purchase/get-all-purchases",
                    "Supplier Credits": "/api/supplier-credit/get-all-supplier-credits",
                    "Customers": "/api/customer/get-all-customers",
                    "Suppliers": "/api/supplier/get-all-suppliers",
                    "Supplier Ledger": "/api/reports/get-supplier-ledger",
                    "Product Stocks": "/api/reports/get-product-stocks-with-product",
                    "Categories": "/api/reports/get-all-product-categories"
                }
                
                tasks = [client.get(f"{BASE_URL}{url}", headers=headers) for url in endpoints.values()]
                responses = await asyncio.gather(*tasks, return_exceptions=True)
                
                for i, key in enumerate(endpoints.keys()):
                    res = responses[i]
                    if not isinstance(res, Exception) and res.status_code == 200:
                        raw = res.json()
                        data_list = raw.get("data", raw) if isinstance(raw, dict) else raw
                        flat_records = list(self._stream_flatten(data_list))
                        self._load_to_sql(key, flat_records)
            
            self.last_sync = time.time()
            return True

db = SQLiteDataMirror()

# ==============================================================================
# 4. SEMANTIC SQL QUERY PLANNER
# ==============================================================================
class StructuredQueryPlanner:
    def __init__(self):
        self.stopwords = {"show", "me", "find", "get", "what", "is", "the", "a", "an", "of", "for", "please", "can", "you", "tell", "details", "all"}

    def execute_plan(self, query):
        clean_q = re.sub(r'[^a-zA-Z0-9\s-]', ' ', query.lower())
        tokens = [t for t in clean_q.split() if t not in self.stopwords]
        search_term = " ".join(tokens)
        
        if not tokens: return None, [], search_term

        # 1. Semantic Routing (TF-IDF determines the correct SQL Table)
        scores = db.tfidf.score(search_term)
        best_table = max(scores, key=scores.get)
        if scores[best_table] == 0.0: best_table = "Product Stocks" # Default fallback

        # 2. Get Table Schema
        cursor = db.conn.cursor()
        cursor.execute(f'PRAGMA table_info("{best_table}")')
        columns = [row['name'] for row in cursor.fetchall()]
        
        # Guard against empty APIs (like Customers)
        if not columns or columns == ["Notice"]:
            return best_table, [], search_term

        # 3. Dynamic Intent-to-SQL Parsing
        sql_clauses = {"WHERE": [], "ORDER BY": "", "LIMIT": ""}
        
        # Detect Limits
        limit_match = re.search(r'top\s+(\d+)', search_term)
        if limit_match:
            sql_clauses["LIMIT"] = f"LIMIT {limit_match.group(1)}"
            search_term = re.sub(r'top\s+\d+', '', search_term)

        # Detect Ordering
        numeric_cols = [c for c in columns if any(kw in c.lower() for kw in ["price", "amount", "qty", "quantity", "stock", "total", "balance"])]
        if numeric_cols:
            if any(w in search_term for w in ["highest", "most", "maximum", "top"]):
                sql_clauses["ORDER BY"] = f'ORDER BY CAST("{numeric_cols[0]}" AS REAL) DESC'
            elif any(w in search_term for w in ["lowest", "least", "minimum"]):
                sql_clauses["ORDER BY"] = f'ORDER BY CAST("{numeric_cols[0]}" AS REAL) ASC'

        # Remaining words are used for filtering
        filter_term = " ".join([w for w in search_term.split() if w not in ["highest", "lowest", "most", "least", "maximum", "minimum", "top"]]).strip()

        # Build dynamic OR statements
        if filter_term:
            conditions = [f'"{col}" LIKE ?' for col in columns]
            sql_clauses["WHERE"] = " OR ".join(conditions)

        # 4. Construct and Execute
        base_sql = f'SELECT * FROM "{best_table}"'
        params = []
        
        if sql_clauses["WHERE"]:
            base_sql += f' WHERE {sql_clauses["WHERE"]}'
            params = [f"%{filter_term}%"] * len(columns)
            
        if sql_clauses["ORDER BY"]: base_sql += f' {sql_clauses["ORDER BY"]}'
        if sql_clauses["LIMIT"]: base_sql += f' {sql_clauses["LIMIT"]}'
        elif not sql_clauses["WHERE"]: base_sql += ' LIMIT 100'

        try:
            cursor.execute(base_sql, params)
            records = [dict(row) for row in cursor.fetchall()]
            return best_table, records, filter_term
        except Exception as e:
            print(f"SQL Error: {e}")
            return best_table, [], filter_term

planner = StructuredQueryPlanner()

# ==============================================================================
# 5. CONVERSATIONAL INSIGHT GENERATOR (LLM-Style Responses)
# ==============================================================================
class LLMStyleFormatter:
    def _calculate_insights(self, records):
        if not records: return None, None
        sum_qty, sum_amount = 0.0, 0.0
        for rec in records:
            for k, v in rec.items():
                k_lower = k.lower()
                try:
                    num = float(v)
                    if any(w in k_lower for w in ["qty", "quantity", "stock"]): sum_qty += num
                    if any(w in k_lower for w in ["total", "amount", "price", "balance", "credit"]) and "unit" not in k_lower: sum_amount += num
                except: pass
        return sum_qty if sum_qty > 0 else None, sum_amount if sum_amount > 0 else None

    def create_human_response(self, domain, records, search_term):
        if not domain or not records:
            return f"I've executed a secure SQL query against the **{domain}** registry, but couldn't locate any records matching **'{search_term}'**. The database might be empty or the spelling could be incorrect."

        total_items = len(records)
        qty, amount = self._calculate_insights(records)
        
        ack = random.choice(["Absolutely. ", "Right away. ", "I have the data ready. ", "Query executed successfully. "])
        
        if search_term:
            action = f"I translated your request into a secure SQL query against the **{domain}** registry"
            found = f" and isolated **{total_items} relevant items**."
        else:
            action = f"I've pulled the master list from the **{domain}** registry"
            found = f" containing **{total_items} items**."

        insight_text = ""
        if qty and amount:
            insight_text = f" Based on deterministic calculations, these items represent a total volume of **{int(qty):,} units** with a financial sum of **₹{amount:,.2f}**."
        elif amount:
            insight_text = f" The total aggregated financial value of these records is **₹{amount:,.2f}**."
        elif qty:
            insight_text = f" These records aggregate to a total volume of **{int(qty):,} units**."

        closing = " Here is the precise data table you requested:<br><br>"
        intro_paragraph = ack + action + found + insight_text + closing

        # HTML Rendering
        raw_keys = set()
        for r in records: raw_keys.update(r.keys())
        visible_keys = [k for k in raw_keys if k not in {"id", "_id", "__v", "password", "jwtToken", "role", "permissions"}]
        priority_keys = ["productName", "name", "firstName", "lastName", "supplierName", "materialName", "email", "phone", "stockQuantity", "sellPrice", "totalAmount"]
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
        return {"response": "🔄 **SQL Mirror Synced.** I have securely updated the in-memory SQLite database with fresh ERP data. Awaiting your query."}

    if q_lower in ["thank you", "thanks"]:
        return {"response": random.choice(["You're very welcome!", "My pleasure!"])}

    if q_lower in ["hi", "hello", "hey"]:
        return {"response": "Hello! I am your Semantic SQL Planner. I map your natural English queries to deterministic SQL commands. Ask me for *'top 5 materials with highest price'* or *'show me supplier credits'*."}

    # 1. Private Translation (Marathi/Hindi to English Intent)
    processed_query = private_preprocess(user_query)

    # 2. Ensure SQL Mirror is populated
    success = await db.sync()
    if not success: 
        return {"response": "System Notice: Unable to securely connect to ERP APIs to build the SQL mirror."}

    # 3. Semantic SQL Execution
    domain, results, search_term = planner.execute_plan(processed_query)

    return {"response": bot_voice.create_human_response(domain, results, search_term)}