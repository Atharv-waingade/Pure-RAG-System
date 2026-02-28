import asyncio, re, time, random, os, math, sqlite3
from collections import Counter
from fastapi import APIRouter, Request
from pydantic import BaseModel
import httpx

router = APIRouter()

# ==============================================================================
# CONFIGURATION
# ==============================================================================
BASE_URL = "https://umbrellasales.xyz/umbrella-inventory-server"
LOGIN_URL = f"{BASE_URL}/api/service/login"
LOGIN_PAYLOAD = {
    "username": os.getenv("ERP_USERNAME", "superadmin.com"),
    "password": os.getenv("ERP_PASSWORD", "superadmin@123")
}
CACHE_TTL = 300

# ==============================================================================
# 1. IMPROVED NLU ENGINE
#    Key fixes:
#    - Priority-ordered matching (longer/more specific phrases win)
#    - Typo tolerance via character n-gram similarity
#    - Proper separation of "supplier-credit" vs "supplier"
#    - No more accidental stripping of meaningful query words
# ==============================================================================

def _ngram_similarity(a: str, b: str, n: int = 2) -> float:
    """
    Compute n-gram (bigram by default) similarity between two strings.
    Used for typo-tolerant matching (e.g. 'purchaces' vs 'purchases').
    Memory cost: O(unique_ngrams) — negligible.
    """
    def ngrams(s):
        s = f"_{s}_"
        return Counter(s[i:i+n] for i in range(len(s) - n + 1))

    a_ng, b_ng = ngrams(a), ngrams(b)
    shared = sum((a_ng & b_ng).values())
    total = sum(a_ng.values()) + sum(b_ng.values())
    return (2 * shared) / total if total else 0.0


class LightweightTFIDF:
    """
    Minimal TF-IDF engine for semantic fallback.
    Operates on in-memory dict — no external libraries.
    """
    def __init__(self, corpus_dict: dict):
        self.documents = corpus_dict
        self.idf: dict = {}
        self._build_idf()

    def _build_idf(self):
        N = len(self.documents)
        vocab = set()
        for doc in self.documents.values():
            vocab.update(doc.split())
        for word in vocab:
            count = sum(1 for doc in self.documents.values() if word in doc.split())
            self.idf[word] = math.log((1 + N) / (1 + count)) + 1

    def _tf(self, text: str) -> dict:
        words = text.split()
        if not words:
            return {}
        counts = Counter(words)
        total = len(words)
        return {w: counts[w] / total for w in counts}

    def score(self, query: str) -> dict:
        q_tf = self._tf(query)
        q_vec = {w: q_tf[w] * self.idf.get(w, 0) for w in q_tf}
        scores = {}
        for name, doc in self.documents.items():
            d_tf = self._tf(doc)
            d_vec = {w: d_tf[w] * self.idf.get(w, 0) for w in d_tf}
            common = set(q_vec) & set(d_vec)
            num = sum(q_vec[x] * d_vec[x] for x in common)
            denom = math.sqrt(sum(v**2 for v in q_vec.values())) * \
                    math.sqrt(sum(v**2 for v in d_vec.values()))
            scores[name] = num / denom if denom else 0.0
        return scores


class AdminBrain:
    def __init__(self):
        # -----------------------------------------------------------------------
        # ROUTING MAP
        # CRITICAL FIX: "supplier-credit" listed BEFORE "supplier" and "payment"
        # so that more-specific multi-word phrases are matched first.
        # Each entry has:
        #   url         - backend endpoint
        #   keywords    - exact word matches (higher weight)
        #   phrases     - multi-word phrase matches (checked first, highest priority)
        #   aliases     - common typos / synonyms fed into n-gram matcher
        # -----------------------------------------------------------------------
        self.api_routing_map = {
            "supplier-credit": {
                "url": "/api/supplier-credit/get-all-supplier-credits",
                "keywords": ["credit", "credits", "owe", "due", "refund", "outstanding"],
                "phrases": ["supplier credit", "supplier credits", "vendor credit", "credit note"],
                "aliases": ["credit", "credits", "owing", "dues"],
            },
            "supplier-ledger": {
                "url": "/api/reports/get-supplier-ledger",
                "keywords": [],
                "phrases": ["supplier ledger", "vendor ledger", "supplier statement", "vendor statement"],
                "aliases": ["supplier ledger", "vendor ledger"],
            },
            "customer-ledger": {
                "url": "/api/reports/get-all-customer-ledgers",
                "keywords": [],
                "phrases": ["customer ledger", "client ledger", "customer account", "customer statement"],
                "aliases": ["customer ledger"],
            },
            "sell": {
                "url": "/api/sell/get-all-sells",
                "keywords": ["sell", "sells", "sold", "sale", "sales", "dispatch", "invoice", "receipt"],
                "phrases": ["sales invoice", "sell invoice", "dispatch record"],
                "aliases": ["sells", "sales", "invoices"],
            },
            "purchase": {
                "url": "/api/purchase/get-all-purchases",
                "keywords": ["purchase", "purchases", "bought", "buy", "acquire", "procurement"],
                "phrases": ["purchase order", "purchase invoice", "purchase history", "purchase list"],
                "aliases": ["purchaces", "purchasse", "purcheases", "purhcases", "buys"],  # typo aliases
            },
            "payment": {
                "url": "/api/payment/supplier-payment-history",
                "keywords": ["payment", "payments", "paid", "settled", "transaction"],
                "phrases": ["payment history", "supplier payment", "payment record"],
                "aliases": ["payments", "paying"],
            },
            "material": {
                "url": "/api/material/get-all-materials",
                "keywords": ["material", "materials", "fabric", "raw", "component"],
                "phrases": ["raw material", "material list"],
                "aliases": ["materials", "fabrics"],
            },
            "customer": {
                "url": "/api/customer/get-all-customers",
                "keywords": ["customer", "customers", "client", "clients", "buyer"],
                "phrases": ["customer list", "client list", "buyer list"],
                "aliases": ["customers", "clients"],
            },
            "supplier": {
                "url": "/api/supplier/get-all-suppliers",
                "keywords": ["supplier", "suppliers", "vendor", "distributor", "provider"],
                "phrases": ["supplier list", "vendor list", "all suppliers"],
                "aliases": ["suppliers", "vendors"],
            },
            "product-stock": {
                "url": "/api/reports/get-product-stocks-with-product",
                "keywords": ["stock", "stocks", "inventory", "warehouse", "available", "product"],
                "phrases": ["product stock", "stock list", "inventory list", "product inventory"],
                "aliases": ["stocks", "inventory", "products"],
            },
            "printer": {
                "url": "/api/printer/get-all-printers",
                "keywords": ["printer", "printers", "machine", "print"],
                "phrases": ["printer list", "all printers"],
                "aliases": ["printers"],
            },
            "email-config": {
                "url": "/api/email-config/get-all-emails",
                "keywords": ["email", "emails", "smtp", "config", "mail"],
                "phrases": ["email config", "mail config", "smtp config"],
                "aliases": ["emails", "mails"],
            },
            "category": {
                "url": "/api/reports/get-all-product-categories",
                "keywords": ["category", "categories", "classification", "type"],
                "phrases": ["product category", "category list"],
                "aliases": ["categories"],
            },
        }

        # Build TF-IDF corpus from keywords only (not phrases — phrases are handled by exact match)
        corpus = {
            mod: " ".join(data["keywords"])
            for mod, data in self.api_routing_map.items()
            if data["keywords"]
        }
        self.tfidf = LightweightTFIDF(corpus)

        # Entity column extractors
        self.entity_triggers = {
            "invoice": "invoiceNo",
            "bill":    "invoiceNo",
            "barcode": "barcode",
            "contact": "contact",
            "phone":   "contact",
            "id":      "id",
            "email":   "email",
            "name":    "name",
        }

        self.bulk_words = {"all", "list", "every", "entire", "show", "get", "fetch", "display", "give"}

        # Fluff words — intentionally small. Do NOT add nouns that could be search values.
        self.fluff_words = {
            "me", "find", "what", "is", "are", "the", "a", "an", "of", "for",
            "please", "can", "you", "tell", "we", "have", "our", "in", "any",
            "by", "its", "their", "about", "information", "on", "who", "which",
            "specific", "details",
        }

    # ------------------------------------------------------------------
    # STEP 1: Phrase match (highest priority)
    # ------------------------------------------------------------------
    def _phrase_match(self, text_lower: str):
        for mod, data in self.api_routing_map.items():
            for phrase in data.get("phrases", []):
                if phrase in text_lower:
                    return mod
        return None

    # ------------------------------------------------------------------
    # STEP 2: Keyword match (count how many module keywords appear)
    # ------------------------------------------------------------------
    def _keyword_match(self, text_lower: str):
        best_mod, best_count = None, 0
        for mod, data in self.api_routing_map.items():
            count = sum(1 for kw in data["keywords"] if re.search(rf"\b{re.escape(kw)}\b", text_lower))
            if count > best_count:
                best_count, best_mod = count, mod
        return best_mod if best_count > 0 else None

    # ------------------------------------------------------------------
    # STEP 3: Typo / alias match using character n-gram similarity
    # ------------------------------------------------------------------
    def _alias_match(self, tokens: list, threshold=0.65):
        best_mod, best_score = None, 0.0
        for token in tokens:
            for mod, data in self.api_routing_map.items():
                for alias in data.get("aliases", []):
                    sim = _ngram_similarity(token, alias)
                    if sim > best_score:
                        best_score, best_mod = sim, mod
        return best_mod if best_score >= threshold else None

    # ------------------------------------------------------------------
    # STEP 4: TF-IDF semantic fallback
    # ------------------------------------------------------------------
    def _tfidf_match(self, clean_q: str, threshold=0.15):
        scores = self.tfidf.score(clean_q)
        if not scores:
            return None
        best = max(scores, key=scores.get)
        return best if scores[best] >= threshold else None

    # ------------------------------------------------------------------
    # MAIN INTENT ANALYSIS
    # ------------------------------------------------------------------
    def analyze_intent(self, user_query: str) -> dict:
        text_lower = user_query.lower()
        clean_q = re.sub(r"[^a-zA-Z0-9\s-]", " ", text_lower).strip()
        tokens = clean_q.split()

        analysis = {
            "intent": "UNKNOWN",
            "module": None,
            "url": None,
            "target_column": None,
            "search_value": "",
            "match_method": None,
        }

        # --- Route detection (ordered by confidence) ---
        module = self._phrase_match(text_lower)
        if module:
            analysis["match_method"] = "phrase"
        else:
            module = self._keyword_match(text_lower)
            if module:
                analysis["match_method"] = "keyword"
            else:
                module = self._alias_match(tokens)
                if module:
                    analysis["match_method"] = "alias"
                else:
                    module = self._tfidf_match(clean_q)
                    if module:
                        analysis["match_method"] = "tfidf"

        if not module:
            analysis["intent"] = "UNRECOGNIZED"
            return analysis

        analysis["module"] = module
        analysis["url"] = self.api_routing_map[module]["url"]
        matched_kws = set(self.api_routing_map[module]["keywords"])

        # --- Entity / column detection ---
        target_col = None
        trigger_words_used = set()
        for trigger, col in self.entity_triggers.items():
            if re.search(rf"\b{re.escape(trigger)}s?\b", text_lower):
                target_col = col
                trigger_words_used.update([trigger, trigger + "s"])
                break
        analysis["target_column"] = target_col

        # --- Extract remaining value tokens ---
        strip_set = (
            self.fluff_words
            | self.bulk_words
            | matched_kws
            | trigger_words_used
            # Also strip individual words from matched phrases
            | {w for p in self.api_routing_map[module].get("phrases", []) for w in p.split()}
            | {w for a in self.api_routing_map[module].get("aliases", []) for w in a.split()}
        )
        value_tokens = [t for t in tokens if t not in strip_set and len(t) > 1]
        extracted_value = " ".join(value_tokens).strip()
        analysis["search_value"] = extracted_value

        is_bulk = any(w in tokens for w in self.bulk_words)

        # --- Determine final intent ---
        if target_col:
            analysis["intent"] = "SPECIFIC_SEARCH" if extracted_value else "PROMPT_NEEDED"
        elif extracted_value and not is_bulk:
            analysis["intent"] = "GLOBAL_SEARCH"
        else:
            analysis["intent"] = "BULK_LIST"

        return analysis


# ==============================================================================
# 2. IN-MEMORY SQLITE MIRROR  (unchanged logic, same memory profile)
# ==============================================================================

class SQLiteDataMirror:
    def __init__(self):
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.last_sync_map: dict = {}
        self._lock = asyncio.Lock()

    def _extract_list(self, raw_json):
        if isinstance(raw_json, list):
            return raw_json
        if isinstance(raw_json, dict):
            if "data" in raw_json and isinstance(raw_json["data"], list):
                return raw_json["data"]
            for v in raw_json.values():
                if isinstance(v, list):
                    return v
        return []

    def _stream_flatten(self, dataset):
        if not dataset or not isinstance(dataset, list):
            return
        for record in dataset:
            if not isinstance(record, dict):
                yield {"Data": record}
                continue
            row_base, list_children = {}, []
            for k, v in record.items():
                if v is None:
                    row_base[k] = "-"
                elif not isinstance(v, (list, dict)):
                    row_base[k] = v
                elif isinstance(v, dict):
                    row_base[k] = v.get("name", v.get("title", v.get("invoiceNo", v.get("totalAmount", "Attached Record"))))
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

    def _load_to_sql(self, table_name: str, records: list):
        self.conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        if not records:
            self.conn.execute(f'CREATE TABLE "{table_name}" ("Notice" TEXT)')
            return
        keys = list({k for r in records for k in r.keys()})
        cols = ", ".join([f'"{k}" TEXT' for k in keys])
        self.conn.execute(f'CREATE TABLE "{table_name}" ({cols})')
        placeholders = ",".join(["?"] * len(keys))
        for r in records:
            self.conn.execute(
                f'INSERT INTO "{table_name}" VALUES ({placeholders})',
                [str(r.get(k, "")) for k in keys],
            )
        self.conn.commit()

    async def sync_module(self, module_key: str, api_url: str, force=False) -> bool:
        if not force and (time.time() - self.last_sync_map.get(module_key, 0) < CACHE_TTL):
            return True
        async with self._lock:
            if not force and (time.time() - self.last_sync_map.get(module_key, 0) < CACHE_TTL):
                return True
            async with httpx.AsyncClient(timeout=30.0) as client:
                try:
                    auth = await client.post(LOGIN_URL, json=LOGIN_PAYLOAD)
                    token = auth.json().get("jwtToken")
                    if not token:
                        return False
                    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
                    res = await client.get(f"{BASE_URL}{api_url}", headers=headers)
                    if res.status_code == 200:
                        flat = list(self._stream_flatten(self._extract_list(res.json())))
                        self._load_to_sql(module_key, flat)
                        self.last_sync_map[module_key] = time.time()
                        return True
                except Exception as e:
                    print(f"[Sync Error] {module_key}: {e}")
                    return False
        return False

    async def sync_all(self, routing_map: dict):
        tasks = [self.sync_module(mod, data["url"], force=True) for mod, data in routing_map.items()]
        await asyncio.gather(*tasks)


db = SQLiteDataMirror()

# ==============================================================================
# 3. RAG RETRIEVER  — SQLite full-text search layer
#    This is the "Retrieval" part of the RAG pipeline.
#    Instead of embedding vectors (too heavy for 200 MB RAM), we use
#    SQLite's LIKE-based search across all columns, which is equivalent
#    for structured ERP data and costs ~0 extra memory.
# ==============================================================================

class RAGRetriever:
    """
    Retrieves relevant records from SQLite based on the intent analysis.
    Applies a 3-tier search strategy:
      Tier 1 — Exact column match   (SPECIFIC_SEARCH)
      Tier 2 — Global LIKE scan     (GLOBAL_SEARCH)
      Tier 3 — Full table fetch     (BULK_LIST)
    """

    MAX_ROWS = 10_000  # SQLite hard cap

    def retrieve(self, conn: sqlite3.Connection, analysis: dict) -> list:
        module = analysis["module"]
        intent = analysis["intent"]
        search_val = analysis["search_value"]
        target_col = analysis["target_column"]

        cursor = conn.cursor()
        cursor.execute(f'PRAGMA table_info("{module}")')
        columns = [row["name"] for row in cursor.fetchall()]
        if not columns or columns == ["Notice"]:
            return []

        sql, params = f'SELECT * FROM "{module}"', []

        if intent == "SPECIFIC_SEARCH" and target_col:
            actual_col = next((c for c in columns if target_col.lower() in c.lower()), None)
            if actual_col:
                sql += f' WHERE "{actual_col}" LIKE ?'
                params = [f"%{search_val}%"]
            else:
                # Column not found — fall back to global search
                intent = "GLOBAL_SEARCH"

        if intent == "GLOBAL_SEARCH" and search_val:
            conditions = " OR ".join([f'"{c}" LIKE ?' for c in columns])
            sql += f" WHERE {conditions}"
            params = [f"%{search_val}%"] * len(columns)

        sql += f" LIMIT {self.MAX_ROWS}"
        try:
            cursor.execute(sql, params)
            return [dict(row) for row in cursor.fetchall()], columns
        except Exception as e:
            print(f"[SQL Error] {e}")
            return [], columns


retriever = RAGRetriever()

# ==============================================================================
# 4. RESPONSE GENERATOR — HTML table formatter
# ==============================================================================

HIDDEN_COLS = {"id", "_id", "__v", "password", "jwtToken", "role", "permissions"}
PRIORITY_KEYS = [
    "invoiceNo", "productName", "name", "firstName", "lastName",
    "supplierName", "supplier", "product", "customer", "materialName",
    "email", "phone", "contact", "stockQuantity", "sellPrice",
    "totalAmount", "paid", "creditAmount",
]


def _calc_insights(records: list):
    qty_total, amount_total = 0.0, 0.0
    for rec in records:
        for amt_k in ["totalAmount", "purchaseAmount", "creditAmount", "sellPrice", "paid"]:
            actual = next((k for k in rec if k.lower() == amt_k.lower()), None)
            if actual:
                try:
                    amount_total += float(str(rec[actual]).replace(",", ""))
                    break
                except ValueError:
                    pass
        for qty_k in ["stockQuantity", "quantity", "totalQuantity", "qty"]:
            actual = next((k for k in rec if k.lower() == qty_k.lower()), None)
            if actual:
                try:
                    qty_total += float(str(rec[actual]).replace(",", ""))
                    break
                except ValueError:
                    pass
    return qty_total or None, amount_total or None


def _build_html_table(records: list, columns: list) -> str:
    visible = [c for c in columns if c not in HIDDEN_COLS]
    headers = sorted(visible, key=lambda x: PRIORITY_KEYS.index(x) if x in PRIORITY_KEYS else 99)

    html = (
        "<div style='overflow-x:auto;margin-top:10px;border-radius:8px;"
        "box-shadow:0 4px 6px -1px rgba(0,0,0,0.1);'>"
        "<table style='border-collapse:collapse;width:100%;text-align:left;"
        "background:#fff;font-family:sans-serif;min-width:600px;'>"
    )
    # Header row
    html += "<tr style='background:#f8fafc;color:#475569;border-bottom:2px solid #cbd5e1;'>"
    for h in headers:
        label = re.sub(r"([a-z])([A-Z])", r"\1 \2", h).title()
        html += f"<th style='padding:14px 16px;border:1px solid #e2e8f0;font-size:14px;white-space:nowrap;'>{label}</th>"
    html += "</tr>"

    for i, rec in enumerate(records):
        bg = "#fff" if i % 2 == 0 else "#f8fafc"
        html += f"<tr style='background:{bg};border-bottom:1px solid #e2e8f0;'>"
        for h in headers:
            val = str(rec.get(h, "") or "-")
            # Image
            if val.startswith("http") and val.lower().endswith((".png", ".jpg")):
                val = f"<img src='{val}' style='max-width:45px;max-height:45px;border-radius:4px;object-fit:cover;'/>"
            # Datetime cleanup
            elif "T" in val and val.count("-") >= 2 and len(val) >= 19:
                try:
                    val = val.split("T")[0] + " " + val.split("T")[1][:5]
                except Exception:
                    pass
            # Currency formatting
            elif any(kw in h.lower() for kw in ["price", "amount", "total", "balance", "credit", "paid"]):
                try:
                    val = f"₹ {float(val.replace(',', '')):,.2f}"
                except ValueError:
                    pass
            html += f"<td style='padding:12px 16px;border:1px solid #e2e8f0;color:#334155;font-size:14px;white-space:nowrap;'>{val}</td>"
        html += "</tr>"

    html += "</table></div>"
    return html


# ==============================================================================
# 5. INTELLIGENCE ENGINE — orchestrates Brain → Retriever → Generator
# ==============================================================================

class IntelligenceEngine:
    def __init__(self):
        self.brain = AdminBrain()

    async def execute_and_format(self, user_query: str, database: SQLiteDataMirror) -> str:
        analysis = self.brain.analyze_intent(user_query)
        intent = analysis["intent"]

        if intent == "UNRECOGNIZED":
            return (
                "I'm not sure which data section you're referring to. "
                "Could you clarify? For example, you can ask about "
                "**Purchases, Sales, Materials, Customers, Suppliers, "
                "Supplier Credits, Stock, or Payments**."
            )

        module = analysis["module"]
        url = analysis["url"]

        if intent == "PROMPT_NEEDED":
            col_label = re.sub(r"([a-z])([A-Z])", r"\1 \2", analysis["target_column"]).title()
            return (
                f"You're querying the **{module.replace('-', ' ').title()}** module. "
                f"Could you specify the **{col_label}**? (e.g. *'INV-1002'*)"
            )

        success = await database.sync_module(module, url)
        if not success:
            return (
                f"⚠️ Unable to reach `{url}`. "
                "Please check the ERP connection or try again shortly."
            )

        records, columns = retriever.retrieve(database.conn, analysis)

        display_name = module.replace("-", " ").title()
        search_val = analysis["search_value"]

        if not records:
            msg = f"No **{display_name}** records found"
            msg += f" matching **'{search_val}'**. Please check your spelling." if search_val else "."
            return msg

        total = len(records)
        qty, amount = _calc_insights(records)

        ack = random.choice(["I've got that for you. ", "Certainly! ", "Here is the data you requested. "])

        if search_val and intent != "BULK_LIST":
            action = f"I scanned **{display_name}** for **'{search_val}'** and found **{total} matching record(s)**."
        else:
            action = f"Here is the complete **{display_name}** list — **{total} record(s)**."

        insight = ""
        if qty and amount:
            insight = f" Totals: **{int(qty):,} units** · **₹{amount:,.2f}**."
        elif amount:
            insight = f" Total value: **₹{amount:,.2f}**."
        elif qty:
            insight = f" Total units: **{int(qty):,}**."

        intro = ack + action + insight + "<br><br>"
        table = _build_html_table(records, columns)
        return intro + table


agent = IntelligenceEngine()

# ==============================================================================
# 6. FASTAPI ENDPOINT
# ==============================================================================

class ChatRequest(BaseModel):
    query: str = None
    question: str = None


IDENTITY_TRIGGERS = {
    "who are you", "how do you work", "chatgpt", "chat gpt",
    "your brain", "mathematical calculations", "how were you built", "hybrid",
}
GREET_WORDS = ["hi", "hello", "hey", "heyy", "heya", "good morning", "good afternoon"]
THANKS_WORDS = {"thank you", "thanks", "awesome", "perfect"}


@router.post("/chat")
async def chat_endpoint(request: ChatRequest):
    user_query = (request.query or request.question or "").strip()
    q_lower = user_query.lower()

    # --- Identity ---
    if any(t in q_lower for t in IDENTITY_TRIGGERS):
        return {
            "response": (
                "I am a **Hybrid Deterministic NLU Engine** built for ERP precision.<br><br>"
                "**How I work (in order):**<br>"
                "1. **Phrase Match** — detects multi-word intent like 'supplier credit' or 'purchase history'.<br>"
                "2. **Keyword Match** — maps individual words to API endpoints.<br>"
                "3. **Typo Match** — n-gram similarity catches misspellings like 'purchaces'.<br>"
                "4. **TF-IDF Fallback** — semantic vector scoring for unusual phrasing.<br>"
                "5. **RAG Retrieval** — structured SQLite search returns only relevant records.<br><br>"
                "All computation is local. Zero hallucinations. Zero AI API costs."
            )
        }

    # --- Sync / refresh ---
    if any(w in q_lower for w in ["refresh", "sync", "update records"]):
        await db.sync_all(agent.brain.api_routing_map)
        return {"response": "🔄 **All modules synced.** What would you like to see?"}

    # --- Thanks ---
    if q_lower in THANKS_WORDS:
        return {"response": random.choice(["You're welcome! Let me know if you need more data.", "My pleasure!"])}

    # --- Greeting ---
    if any(q_lower.startswith(g) for g in GREET_WORDS) and len(q_lower.split()) <= 3:
        return {
            "response": (
                "Hello! I'm your AI Operations Assistant.<br>"
                "You can ask me things like:<br>"
                "• <i>List all purchases</i><br>"
                "• <i>Show supplier credits</i><br>"
                "• <i>Find customer by name: Rahul</i><br>"
                "• <i>Get stock inventory</i>"
            )
        }

    response_html = await agent.execute_and_format(user_query, db)
    return {"response": response_html}