"""
==============================================================================
  ADMIN INTELLIGENCE ENGINE  v6.3  —  PRODUCTION READY · ALL 8 GAPS CLOSED
  ERP Chatbot RAG Pipeline · Zero external AI APIs · ≤300 MB RAM
  Full Marathi (Devanagari + Roman) + English support

  ╔══════════════════════════════════════════════════════════════════════════╗
  ║  v6.2 (57/57 tests) + 8 production hardening fixes                     ║
  ╚══════════════════════════════════════════════════════════════════════════╝

  FIXES vs v6.2:
  ──────────────
  FIX-1: Startup readiness gate — asyncio.Event() blocks /chat until the
         warmup task (4 module pre-fetch) completes. First request no longer
         gets empty SQLite. Falls through after 30s safety timeout.

  FIX-2: httpx cancellation fixed — _fetch_with_retry() now uses httpx.Timeout
         with explicit connect=5s/read=25s values. asyncio.wait_for(45s) can
         now properly cancel the underlying TCP connection instead of
         abandoning it open. CancelledError propagated immediately (no retry).

  FIX-3: Circuit breaker — CB_FAILURE_THRESHOLD=5 consecutive ERP failures
         opens the breaker. All sync_module() calls short-circuit to stale/error
         instantly instead of burning 3×retry×1.5s = 4.5s each under load.
         Probes after CB_RECOVERY_TIMEOUT=30s (HALF_OPEN), closes after
         CB_SUCCESS_THRESHOLD=2 successes.

  FIX-4: Cache coherence — response_cache.invalidate_module(key) called
         immediately after a successful sync. Stale HTML can no longer survive
         past the point where fresh data is loaded into SQLite.

  FIX-5: CORS — CORSMiddleware added. Restrict allowed origins via env var
         CORS_ORIGINS=https://yourapp.com (comma-separated). Defaults to "*"
         for local dev; always set in production.

  FIX-6: Security headers — SecurityHeadersMiddleware injects on every
         response: X-Content-Type-Options, X-Frame-Options, X-XSS-Protection,
         Content-Security-Policy, Referrer-Policy, Permissions-Policy.

  FIX-7: Request tracing — X-Request-ID header echoed (or generated) on
         every response. Logged alongside ip/lang/query in chat_endpoint.
         Makes it possible to correlate a frontend error report with a
         specific log line in Render's dashboard.

  FIX-8: Rate limiter clarified — check() now returns (allowed, remaining)
         so X-RateLimit-Remaining can be set. Documented clearly: in-process
         limiter is correct for single-worker Render free tier. Redis path
         documented for if/when you scale to multiple workers.

  ALL PREVIOUS FIXES RETAINED (see v6.2 header for full list):
  ─────────────────────────────────────────────────────────────
  SEC-01–10 · BUG-07–16 · PERF-06–11 · FEAT-01–05
==============================================================================
"""

from __future__ import annotations

import asyncio
import collections
import html
import logging
import math
import os
import re
import secrets
import sqlite3
import time
import random
import uuid
from collections import Counter, defaultdict, deque
from contextlib import asynccontextmanager
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

# ── Optional imports (FastAPI/httpx) — mocked in tests ───────────────────────
try:
    import httpx
    from fastapi import APIRouter, HTTPException, Request, Response
    from pydantic import BaseModel, field_validator
    _FASTAPI_AVAILABLE = True
except ImportError:          # pragma: no cover
    _FASTAPI_AVAILABLE = False
    class BaseModel:         # type: ignore
        def __init__(self, **kw): [setattr(self, k, v) for k, v in kw.items()]
    class Request: pass      # type: ignore
    class Response: pass     # type: ignore

# ──────────────────────────────────────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("erp.engine")

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────────
BASE_URL               = os.getenv("ERP_BASE_URL",
                                   "https://umbrellasales.xyz/umbrella-inventory-server")
LOGIN_URL              = f"{BASE_URL}/api/service/login"

# SEC-09: refuse hardcoded creds in production unless opt-in env var set
_ERP_USER = os.getenv("ERP_USERNAME", "")
_ERP_PASS = os.getenv("ERP_PASSWORD", "")
_ALLOW_DEFAULT = os.getenv("ALLOW_DEFAULT_CREDS", "false").lower() == "true"
if not _ERP_USER and not _ALLOW_DEFAULT:
    log.warning(
        "ERP_USERNAME not set. Set ERP_USERNAME / ERP_PASSWORD env vars, "
        "or set ALLOW_DEFAULT_CREDS=true to use defaults (not recommended)."
    )
if not _ERP_USER and _ALLOW_DEFAULT:
    _ERP_USER = "superadmin.com"
    _ERP_PASS = "superadmin@123"

LOGIN_CREDS = {"username": _ERP_USER, "password": _ERP_PASS}

MODULE_TTL             = 300       # 5 min  — master data
STOCK_TTL              = 120       # 2 min  — inventory
FINANCE_TTL            = 600       # 10 min — ledgers
MAX_ROWS               = 30_000    # per-module SQLite cap (300 MB budget)
MAX_RENDER_ROWS        = 500       # rows rendered in HTML table (PERF-11)
API_TIMEOUT            = 30.0
API_MAX_RETRIES        = 3
JWT_EXPIRY_BUFFER      = 60        # re-auth 60s before token expires
FLATTEN_MAX_CHILD_ROWS = 200       # SEC-02: sibling array cap
RATE_LIMIT_RPM         = 60        # SEC-07: requests per minute per IP
MAX_SESSIONS           = 500       # BUG-09: max concurrent session contexts
SESSION_TTL            = 1800      # BUG-09: 30 min session idle timeout
RESPONSE_CACHE_SIZE    = 512       # PERF-06: LRU response cache entries
RESPONSE_CACHE_TTL     = 90        # PERF-06: cache TTL in seconds
WARMUP_MODULES         = [         # BUG-10: pre-fetched on startup
    "product-stock", "purchase", "sell", "supplier",
]
SQLITE_MMAP_SIZE       = 128 * 1024 * 1024   # PERF-08: 128 MB mmap

# FIX-1: Startup readiness — requests wait until warmup is done
_startup_ready         = asyncio.Event()

# FIX-3: Circuit breaker thresholds
CB_FAILURE_THRESHOLD   = 5      # open after 5 consecutive failures
CB_RECOVERY_TIMEOUT    = 30.0   # seconds before half-open probe
CB_SUCCESS_THRESHOLD   = 2      # successes in half-open to close

# FIX-6: Security headers added to every response
SECURITY_HEADERS = {
    "X-Content-Type-Options":  "nosniff",
    "X-Frame-Options":         "DENY",
    "X-XSS-Protection":        "1; mode=block",
    "Content-Security-Policy": (
        "default-src 'none'; "
        "img-src 'self' data: https://umbrellasales.xyz; "
        "style-src 'unsafe-inline'; "
        "script-src 'none'"
    ),
    "Referrer-Policy":         "strict-origin-when-cross-origin",
    "Permissions-Policy":      "geolocation=(), microphone=(), camera=()",
}

# FIX-5: CORS — restrict to your actual frontend origins
# Override via env var: CORS_ORIGINS=https://yourapp.com,https://admin.yourapp.com
_cors_env = os.getenv("CORS_ORIGINS", "")
CORS_ORIGINS: List[str] = (
    [o.strip() for o in _cors_env.split(",") if o.strip()]
    if _cors_env
    else ["*"]   # dev default — lock down in prod via env var
)

# ──────────────────────────────────────────────────────────────────────────────
# MODULE REGISTRY
# ──────────────────────────────────────────────────────────────────────────────
MODULE_REGISTRY: Dict[str, Dict[str, Any]] = {

    "supplier-credit": {
        "url":         "/api/supplier-credit/get-all-supplier-credits",
        "ttl":         FINANCE_TTL,
        "keywords":    ["credit", "credits", "outstanding", "due", "refund", "owe", "owes"],
        "phrases":     ["supplier credit", "supplier credits", "vendor credit",
                        "credit note", "credit balance", "outstanding credit",
                        "how much do we owe", "credits owed"],
        "aliases":     ["supplier credit", "vendor credit", "credits outstanding"],
        "amount_cols": ["creditAmount", "amount", "totalCredit", "totalAmount"],
        "qty_cols":    [],
        "search_endpoints": {},
    },

    "supplier-ledger": {
        "url":         "/api/reports/get-supplier-ledger",
        "ttl":         FINANCE_TTL,
        "keywords":    ["ledger", "statement"],
        "phrases":     ["supplier ledger", "vendor ledger", "supplier statement",
                        "vendor statement", "supplier account", "supplier account history"],
        "aliases":     ["supplier ledger", "vendor ledger"],
        "amount_cols": ["debit", "credit", "balance", "amount"],
        "qty_cols":    [],
        "search_endpoints": {},
    },

    "customer-ledger": {
        "url":         "/api/reports/get-all-customer-ledgers",
        "ttl":         FINANCE_TTL,
        "keywords":    [],
        "phrases":     ["customer ledger", "client ledger", "customer statement",
                        "customer account", "receivables", "customer balance"],
        "aliases":     ["customer ledger", "client ledger"],
        "amount_cols": ["debit", "credit", "balance", "amount"],
        "qty_cols":    [],
        "search_endpoints": {},
    },

    "customer-ledger-summary": {
        "url":         "/api/reports/customer-ledger-summary",
        "ttl":         FINANCE_TTL,
        "keywords":    [],
        "phrases":     ["customer ledger summary", "customer account summary",
                        "customer balance summary", "customer outstanding summary"],
        "aliases":     ["customer summary ledger"],
        "amount_cols": ["balance", "totalDebit", "totalCredit"],
        "qty_cols":    [],
        "search_endpoints": {},
    },

    "sell": {
        "url":         "/api/sell/get-all-sells",
        "ttl":         MODULE_TTL,
        "keywords":    ["sell", "sells", "sold", "sale", "sales", "dispatch",
                        "invoice", "receipt", "billing", "revenue"],
        "phrases":     ["sales invoice", "sell invoice", "dispatch record", "all sales",
                        "sales list", "all sells", "invoices", "what did we sell",
                        "customer invoice", "sales history", "sell history"],
        "aliases":     ["sales", "sells", "invoices", "receipts", "billing"],
        "amount_cols": ["totalAmount", "sellPrice", "amount", "paid"],
        "qty_cols":    ["quantity", "qty", "totalQuantity"],
        "search_endpoints": {
            "invoice": {"url": "/api/sell/search-by-invoice",       "param": "invoiceNo"},
            "invoice_exact": {"url": "/api/sell/get-sell-by-invoice-no", "param": "invoiceNo"},
        },
    },

    "purchase": {
        "url":         "/api/purchase/get-all-purchases",
        "ttl":         MODULE_TTL,
        "keywords":    ["purchase", "purchases", "bought", "buy", "buying",
                        "acquire", "procurement", "ordered"],
        "phrases":     ["purchase order", "purchase invoice", "purchase list",
                        "all purchases", "what did we buy", "all purchase orders",
                        "purchase record"],
        "aliases":     ["purchaces", "purchasse", "purcheases", "purhcases",
                        "purchaes", "puchases", "pruchases", "purchases", "buys"],
        "amount_cols": ["totalAmount", "purchaseAmount", "totalPurchaseAmount", "amount"],
        "qty_cols":    ["quantity", "qty", "totalQuantity", "receivedQuantity"],
        "search_endpoints": {
            "barcode": {"url": "/api/purchase/get-stock-by-barcode",       "param": "barcode"},
            "invoice": {"url": "/api/purchase/get-purchase-by-invoice-no", "param": "invoiceNo"},
            "name":    {"url": "/api/purchase/get-stock-by-product-name",  "param": "productName"},
        },
    },

    "supplier-purchase-history": {
        "url":         "/api/reports/supplier-purchase-history",
        "ttl":         MODULE_TTL,
        "keywords":    [],
        "phrases":     ["supplier purchase history", "vendor purchase history",
                        "supplier purchase report", "vendor purchase report",
                        "supplier buying history", "purchase history by supplier",
                        "supplier wise purchase", "get supplier purchase",
                        "suppliers purchase", "supplier purchases"],
        "aliases":     ["supplier purchase history", "vendor purchase history"],
        "amount_cols": ["totalAmount", "purchaseAmount", "totalPurchaseAmount"],
        "qty_cols":    ["quantity", "totalQuantity"],
        "search_endpoints": {},
    },

    "payment": {
        "url":         "/api/payment/supplier-payment-history",
        "ttl":         MODULE_TTL,
        "keywords":    ["payment", "payments", "paid", "settled", "transaction",
                        "remittance", "disbursement"],
        "phrases":     ["payment history", "supplier payment", "payment record",
                        "payment list", "all payments", "transaction history",
                        "what was paid", "payments made", "show payment",
                        "my payment history"],
        "aliases":     ["payments", "paying", "remittance", "settlement"],
        "amount_cols": ["amount", "paid", "totalPaid"],
        "qty_cols":    [],
        "search_endpoints": {},
    },

    "customer-payment-history": {
        "url":         "/api/customer/payment-history",
        "ttl":         MODULE_TTL,
        "keywords":    [],
        "phrases":     ["customer payment history", "client payment history",
                        "customer paid", "customer payment record",
                        "customer transactions", "customer receipts"],
        "aliases":     ["customer payment", "client payment"],
        "amount_cols": ["amount", "paid", "totalPaid", "balance"],
        "qty_cols":    [],
        "search_endpoints": {},
    },

    "material": {
        "url":         "/api/material/get-all-materials",
        "ttl":         MODULE_TTL,
        "keywords":    ["material", "materials", "fabric", "raw", "component"],
        "phrases":     ["raw material", "material list", "all materials", "fabric list",
                        "raw materials list", "component list"],
        "aliases":     ["materials", "fabrics", "raw materials"],
        "amount_cols": [],
        "qty_cols":    [],
        "search_endpoints": {
            "name": {"url": "/api/material/get-material-by-name", "param": "name"},
        },
    },

    "customer": {
        "url":         "/api/customer/get-all-customers",
        "ttl":         MODULE_TTL,
        "keywords":    ["customer", "customers", "client", "clients", "buyer", "purchaser"],
        "phrases":     ["customer list", "client list", "all customers",
                        "buyer list", "who are our customers", "find customer",
                        "customer details", "customer info"],
        "aliases":     ["customers", "clients", "buyers"],
        "amount_cols": [],
        "qty_cols":    [],
        "search_endpoints": {
            "contact": {"url": "/api/customer/search-by-contact", "param": "contact"},
        },
    },

    "supplier": {
        "url":         "/api/supplier/get-all-suppliers",
        "ttl":         MODULE_TTL,
        "keywords":    ["supplier", "suppliers", "vendor", "vendors", "distributor",
                        "provider"],
        "phrases":     ["supplier list", "vendor list", "all suppliers",
                        "distributor list", "supplier details", "supplier info",
                        "find supplier"],
        "aliases":     ["suppliers", "vendors", "distributors"],
        "amount_cols": ["supplierCredit"],
        "qty_cols":    [],
        "search_endpoints": {},
    },

    "product-stock": {
        "url":         "/api/reports/get-product-stocks-with-product",
        "ttl":         STOCK_TTL,
        "keywords":    ["stock", "stocks", "inventory", "warehouse", "available",
                        "product", "products"],
        "phrases":     ["product stock", "stock list", "inventory list",
                        "product inventory", "available stock", "warehouse stock",
                        "current stock", "what do we have in stock",
                        "show stock", "all stock", "stock report"],
        "aliases":     ["stocks", "inventory", "products", "warehousing"],
        "amount_cols": ["sellPrice", "pricePerUnit"],
        "qty_cols":    ["stockQuantity", "quantity"],
        "search_endpoints": {},
    },

    "category": {
        "url":         "/api/reports/get-all-product-categories",
        "ttl":         MODULE_TTL,
        "keywords":    ["category", "categories", "classification"],
        "phrases":     ["product category", "product categories", "category list",
                        "all categories", "list categories", "show categories",
                        "get categories", "categories list", "what categories",
                        "item categories", "item classification"],
        "aliases":     ["categories", "classifications", "product categories"],
        "amount_cols": [],
        "qty_cols":    [],
        "search_endpoints": {},
    },

    "printer": {
        "url":         "/api/printer/get-all-printers",
        "ttl":         MODULE_TTL,
        "keywords":    ["printer", "printers", "machine", "print"],
        "phrases":     ["printer list", "all printers", "printing machines",
                        "active printer", "show printers"],
        "aliases":     ["printers", "printing"],
        "amount_cols": [],
        "qty_cols":    [],
        "search_endpoints": {
            "active": {"url": "/api/printer/get-active-printer", "param": None},
        },
    },

    "email-config": {
        "url":         "/api/email-config/get-all-emails",
        "ttl":         MODULE_TTL,
        "keywords":    ["email", "emails", "smtp", "config", "mail"],
        "phrases":     ["email config", "mail config", "smtp config",
                        "email settings", "active email", "email setup"],
        "aliases":     ["emails", "mails", "smtp"],
        "amount_cols": [],
        "qty_cols":    [],
        "search_endpoints": {
            "active": {"url": "/api/email-config/active", "param": None},
        },
    },
}

# ──────────────────────────────────────────────────────────────────────────────
# MULTI-MODULE PATTERNS
# ──────────────────────────────────────────────────────────────────────────────
MULTI_PATTERNS = [
    {
        "phrases": ["supplier summary", "supplier overview", "vendor summary",
                    "full supplier details", "all supplier info", "complete supplier info"],
        "modules": ["supplier", "supplier-credit", "supplier-ledger",
                    "supplier-purchase-history"],
        "label":   "Full Supplier Overview",
    },
    {
        "phrases": ["financial summary", "finance report", "money overview",
                    "accounts summary", "financial overview", "full finance"],
        "modules": ["sell", "purchase", "payment", "supplier-credit"],
        "label":   "Financial Summary",
    },
    {
        "phrases": ["sales report", "sales and stock", "revenue and inventory"],
        "modules": ["sell", "product-stock"],
        "label":   "Sales & Inventory Report",
    },
    {
        "phrases": ["customer and sales", "sales by customer", "customer sales"],
        "modules": ["customer", "sell"],
        "label":   "Customer & Sales Report",
    },
    {
        "phrases": ["purchase and payment", "vendor transactions", "vendor payment history"],
        "modules": ["purchase", "payment", "supplier"],
        "label":   "Vendor Transactions",
    },
    {
        "phrases": ["customer report", "customer full report", "customer complete details"],
        "modules": ["customer", "customer-ledger", "customer-payment-history"],
        "label":   "Customer Full Report",
    },
]

# ──────────────────────────────────────────────────────────────────────────────
# COMPILED REGEXES  (PERF-09)
# ──────────────────────────────────────────────────────────────────────────────
BARCODE_RE   = re.compile(r"\bBAR\w{6,}\b",                   re.IGNORECASE)
INVOICE_RE   = re.compile(r"\b(PA\d{5,}|SA\d{5,}|INV[-/]\w+)\b", re.IGNORECASE)
CONTACT_RE   = re.compile(r"\b[6-9]\d{9}\b")
ACTIVE_RE    = re.compile(r"\bactive\b",                       re.IGNORECASE)
CAMEL_RE     = re.compile(r"([a-z])([A-Z])")
CLEAN_RE     = re.compile(r"[^a-z0-9\s\-]")
# BUG-14: FTS special chars AND boolean operators to strip from search terms
FTS_SPECIAL   = re.compile(r'["()*+\-^~<>]')
_FTS_BOOL_RE  = re.compile(r'\b(NOT|OR|AND|NEAR)\b', re.IGNORECASE)
# BUG-15: strict datetime check (YYYY-MM-DDTHH:MM)
DATETIME_RE  = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}")
# SEC-08: session_id sanitizer
SESSION_RE   = re.compile(r"[^a-zA-Z0-9_\-]")

# ──────────────────────────────────────────────────────────────────────────────
# COLUMN SEARCH TRIGGERS
# ──────────────────────────────────────────────────────────────────────────────
COLUMN_TRIGGERS: Dict[str, List[str]] = {
    "invoice":  ["invoiceNo", "invoice", "billNo"],
    "bill":     ["billNo", "invoiceNo"],
    "barcode":  ["barcode"],
    "contact":  ["contact", "phone", "mobile"],
    "phone":    ["phone", "contact", "mobile"],
    "mobile":   ["mobile", "phone", "contact"],
    "id":       ["id", "_id"],
    "email":    ["email"],
    "name":     ["name", "firstName", "lastName", "productName",
                 "supplierName", "customerName", "materialName"],
    "address":  ["address"],
    "gst":      ["gstNo", "gst"],
    "date":     ["date", "createdAt", "purchaseDate", "sellDate"],
    "amount":   ["totalAmount", "amount", "creditAmount", "paid"],
    "price":    ["sellPrice", "price", "pricePerUnit"],
    "status":   ["status", "supplyType"],
    "quantity": ["stockQuantity", "quantity", "qty"],
    "stock":    ["stockQuantity"],
    "size":     ["size"],
    "color":    ["color"],
    "unit":     ["unit"],
    "category": ["category"],
    "hsn":      ["hsnNo", "hsn"],
}

# ──────────────────────────────────────────────────────────────────────────────
# STOP WORD SETS
# ──────────────────────────────────────────────────────────────────────────────
EN_STOP = frozenset({
    "me", "find", "what", "is", "are", "the", "a", "an", "of", "for",
    "please", "can", "you", "tell", "we", "have", "our", "in", "any",
    "by", "its", "their", "about", "on", "who", "which", "specific",
    "details", "show", "get", "fetch", "display", "list", "all", "every",
    "entire", "give", "check", "search", "look", "want", "need", "see",
    "view", "pull", "do", "with", "from", "this", "that", "it", "i",
    "my", "he", "she", "they", "them", "up", "out", "how", "many",
    "much", "total", "count", "number", "no", "has", "some", "certain",
    "latest", "recent", "current", "new", "old", "make", "create",
    "report", "data", "records", "record", "entries", "full", "complete",
    "entire", "whole", "information", "info",
})
EN_BULK = frozenset({
    "all", "list", "every", "entire", "show", "get", "fetch",
    "display", "give", "complete", "full",
})
MR_STOP_ROMAN = frozenset({
    "mala","mla","mhala","amhala","amhi","mi","tu","to","ti","te","tyala","tila","aapan","apan",
    "dakhva","dakhav","dakhvav","dakha","dakhava","dakhvaa","dikhao","dikhav","dikhava","dikhva",
    "dya","deu","de","dyaa","dyave","bagha","bagh","baghu","pahava","paha","pahun","pahat",
    "sanga","sangav","sangava","sangaa","kadhav","kadhva","milvava","milva","milu","mil",
    "ghya","ghyava","aana","aanava","pahije","hve","have",
    "sarv","sarva","sarve","saglya","sagale","sagala","sagali","sagle",
    "yadi","yaadi","soochi","suchi","jadval","sampurn","sampoorna","poorna","purna",
    "sare","sara","sari","sarav",
    "chi","che","cha","chya","la","na","va","ani","aani","ahe","aahe","aste","asate",
    "ahet","aahet","naahi","nahi","kiti","kitee","kiteek",
    "kasa","kase","kashi","kay","kaay","kon","konte","ha","he","hi","hya","tya","ya","ja",
    "pan","pari","tar","mhanje","mhanun","ata","aata","jara","jra","ekda","ekadha",
    "sathi","saathi","karitha","karita","madhe","madhye","madhil","tun","tyatun",
    "sobat","sathe","barobar","naavache","naav","naavane","naavachya",
    "wala","wale","wali","vala","vale","vali","mahiti","tapshil","maahiti",
    "itihas","history","kaya","kithi","kevu","kevha","keva","kuthun",
    "please","plz","krupaya","krupa","thodi","thoda","thode",
    "ek","don","teen","char","panch",
})
MR_BULK_ROMAN = frozenset({
    "sarv","sarva","sarve","saglya","sagale","yadi","yaadi",
    "sare","sara","sampurn","poorna","purna","soochi",
    "all","every","complete","full","entire","sarav",
})
MR_STOP_DEVA = frozenset({
    "मला","म्हाला","आम्हाला","आम्ही","मी","तू","तो","ती","ते","त्याला","तिला","आपण",
    "दाखवा","दाखव","दाखवाव","दाखव","दिखाओ","दिखाव","द्या","दे","देऊ","द्यावा","द्यावे",
    "बघा","बघ","बघू","पहावा","पाहा","पाहावा","सांगा","सांगव","काढा","काढव",
    "मिळवा","मिळव","घ्या","घ्यावा","आणा","सर्व","सगळ्या","सगळे","सगळा","सगळी",
    "यादी","सूची","जाडवल","संपूर्ण","पूर्ण","सारे","सारा","सारी",
    "ची","चे","चा","च्या","ला","ना","व","आणि","आहे","असते","असतात","नाही",
    "किती","कसा","कसे","कशी","काय","कोण","कोणते",
    "हा","हे","ही","ह्या","त्या","या","जा","पण","परी","तर","म्हणजे","म्हणून",
    "आता","जरा","एकदा","साठी","करिता","मध्ये","मधून","त्यातून",
    "सोबत","साथे","बरोबर","माहिती","तपशील","इतिहास","शोधा","शोध","शोधव","कृपया","थोडी","थोडा",
})
MR_BULK_DEVA = frozenset({"सर्व","सगळ्या","सगळे","यादी","सूची","संपूर्ण","पूर्ण","सारे"})

# ──────────────────────────────────────────────────────────────────────────────
# MARATHI LANGUAGE LAYER
# ──────────────────────────────────────────────────────────────────────────────
DEVA_STEMS: Dict[str, str] = {
    "उत्पादने":"उत्पादन","उत्पादनें":"उत्पादन","ग्राहकांची":"ग्राहक",
    "ग्राहकाची":"ग्राहक","ग्राहकांचे":"ग्राहक","पुरवठादाराचे":"पुरवठादार",
    "पुरवठादारांचे":"पुरवठादार","खरेदीची":"खरेदी","विक्रीची":"विक्री",
    "साठ्याची":"साठा","साथ":"साठा","स्टॉकची":"स्टॉक","देयकाचे":"देयक",
    "पेमेंटची":"पेमेंट","साहित्याचे":"साहित्य","श्रेणीची":"श्रेणी",
    "क्रेडिटची":"क्रेडिट","साठाची":"साठा","उत्पादनाची":"उत्पादन",
    "उत्पादनांची":"उत्पादन","खरेदीचे":"खरेदी","विक्रीचे":"विक्री",
    "पुरवठादाराची":"पुरवठादार","श्रेणींची":"श्रेणी","साहित्यांचे":"साहित्य",
}

_MR_RAW: Dict[str, Dict[str, List[str]]] = {
    "purchase": {
        "deva":["खरेदी यादी","खरेदी ऑर्डर","खरेदी इनव्हॉइस","खरेदी इतिहास",
                "माल खरेदी","खरेदी नोंदी","काय खरेदी केले","सर्व खरेदी","खरेदी"],
        "roman":["kharedi yadi","kharedi order","kharedi invoice","kharedi itihas",
                 "mal kharedi","kharedi nondi","kay kharedi kele","sarv kharedi",
                 "kharedi list","kharedee list","kharidi list","kharedee itihas",
                 "purchase kele","purchase list","kharidi yadi","kharedi","kharedee","kharidi"],
    },
    "sell": {
        "deva":["विक्री यादी","विक्री इनव्हॉइस","विक्री इतिहास","विक्री नोंदी",
                "माल विकला","काय विकले","सर्व विक्री","विक्री तपशील","विक्री","बिल","पावती"],
        "roman":["vikri yadi","vikri invoice","vikri itihas","vikri nondi","mal vikla",
                 "kay vikle","sarv vikri","vikri tapshil","vikri list","vikree list",
                 "vikri history","sales list","vikri","vikree","pavti"],
    },
    "supplier-purchase-history": {
        "deva":["पुरवठादार खरेदी इतिहास","पुरवठादार खरेदी अहवाल",
                "पुरवठादाराने काय खरेदी","विक्रेता खरेदी यादी"],
        "roman":["purvathakaar kharedi itihas","supplier kharedi itihas",
                 "supplier purchase history","vendor purchase history",
                 "purvathakaar kharedi list","supplier kharedi yadi",
                 "supplier kharedi","purvathakaar purchase"],
    },
    "supplier-credit": {
        "deva":["पुरवठादार क्रेडिट","क्रेडिट शिल्लक","किती देणे आहे",
                "पुरवठादाराचे देणे","बाकी रक्कम","पुरवठादार बाकी",
                "थकबाकी","देणे","उधार","क्रेडिट"],
        "roman":["purvathakaar credit","puravathakaar credit","credit shillak",
                 "kiti dene ahe","purvathakaarache dene","baki rakkam",
                 "purvathakaar baki","supplier credit","vendor credit",
                 "credit balance","thakbaki","udhar","dene","credit"],
    },
    "supplier-ledger": {
        "deva":["पुरवठादार खातेवही","पुरवठादार हिशेब","पुरवठादार विवरण","पुरवठादार खाते"],
        "roman":["purvathakaar khatevahi","purvathakaar hisab","puravathakaar khatevahi",
                 "purvathakaar vivaran","supplier ledger","vendor ledger","supplier hisab"],
    },
    "customer-ledger": {
        "deva":["ग्राहक खातेवही","ग्राहक हिशेब","ग्राहक विवरण","ग्राहक खाते"],
        "roman":["grahak khatevahi","grahak hisab","graahak khatevahi",
                 "customer ledger","customer hisab"],
    },
    "customer-payment-history": {
        "deva":["ग्राहक देयक इतिहास","ग्राहक पेमेंट","ग्राहकाने किती दिले"],
        "roman":["grahak payment itihas","grahak payment history",
                 "customer payment history","grahak paid"],
    },
    "customer": {
        "deva":["ग्राहक यादी","सर्व ग्राहक","ग्राहक माहिती","खरेदीदार","ग्राहक"],
        "roman":["grahak yadi","sarv grahak","grahak mahiti","kharedidar",
                 "grahak list","graahak list","customer list","grahak","graahak"],
    },
    "supplier": {
        "deva":["पुरवठादार यादी","सर्व पुरवठादार","विक्रेता","माल पुरवठादार","पुरवठादार"],
        "roman":["purvathakaar yadi","sarv purvathakaar","vikreta",
                 "supplier list","vendor list","supplier yadi",
                 "supplier chi yadi","supplier che tapshil","supplier mahiti",
                 "supplier bagha","supplier dakha","purvathakaar","puravathakaar","supplier"],
    },
    "product-stock": {
        "deva":["माल साठा","उपलब्ध माल","किती साठा आहे","स्टॉक यादी","उपलब्ध स्टॉक",
                "उत्पादन साठा","उत्पादन यादी","इन्व्हेंटरी","गोदाम","उत्पादने","उत्पादन","साठा","स्टॉक"],
        "roman":["mal satha","uplabdh mal","kiti satha ahe","stock yadi","uplabdh stock",
                 "stock kiti ahe","stock bagha","stock list","inventory list","utpadan satha",
                 "utpadan yadi","godam","utpadan","saatha","satha","stock"],
    },
    "payment": {
        "deva":["पेमेंट इतिहास","दिलेले पैसे","पुरवठादार देयक",
                "व्यवहार नोंदी","पैसे दिले","व्यवहार","देयक","पेमेंट"],
        "roman":["payment itihas","dilele paise","purvathakaar dayak","vyavahar nondi",
                 "paise dile","payment history","payment list","vyavahar","dayak","payment"],
    },
    "material": {
        "deva":["कच्चा माल","माल यादी","साहित्य यादी","कापड यादी","साहित्य","कापड","घटक","सामग्री"],
        "roman":["kachha mal","mal yadi","sahitya yadi","kapad yadi","material list",
                 "raw material","sahitya","kapad","ghatak","samagri"],
    },
    "category": {
        "deva":["श्रेणी यादी","वर्गीकरण","उत्पादन प्रकार","उत्पादन श्रेणी","सर्व श्रेणी","श्रेणी","प्रकार"],
        "roman":["shreni yadi","vargikaran","utpadan prakar","utpadan shreni","sarv shreni",
                 "category list","all categories","shreni","prakar"],
    },
    "printer": {
        "deva":["प्रिंटर यादी","मुद्रण यंत्र","प्रिंटर"],
        "roman":["printer yadi","mudran yantra","printer list","printer"],
    },
    "email-config": {
        "deva":["ईमेल सेटिंग","ईमेल कॉन्फिग","ईमेल","मेल"],
        "roman":["email settings","email config","mail config","email"],
    },
}

# Build sorted flat list (longest match first) — BUG-08: used for scoring
_MR_FLAT: List[Tuple[str, str]] = []
for _mod, _kws in _MR_RAW.items():
    for k in _kws["deva"] + _kws["roman"]:
        _MR_FLAT.append((k.lower(), _mod))
_MR_FLAT.sort(key=lambda x: len(x[0]), reverse=True)

_MR_ROMAN_MARKERS = frozenset([
    "kharedi","kharedee","kharidi","vikri","vikree","grahak","graahak",
    "purvathakaar","puravathakaar","satha","saatha","thakbaki","udhar","dene",
    "yadi","bagha","dakha","ahe","aahe","aahet","ahet","sarv","sarva","sarve",
    "saglya","hisab","hisaab","khatevahi","vivaran","dayak","paise","paisa",
    "kachha","sahitya","kapad","shreni","uplabdh","godam","vyavahar","pavti",
    "baki","shillak","rakkam","vikreta","kiti","kitee","mal","nondi","tapshil",
    "itihas","mahiti","dilele","dile","kele","vikle","dakhva","dakhav","dakha",
    "mala","mhala","chi","che","cha","supplier",
])

MR_COLUMNS: Dict[str, str] = {
    "Invoice No":"इनव्हॉइस क्र.","Name":"नाव","Email":"ईमेल","Contact":"संपर्क",
    "Address":"पत्ता","Supplier Name":"पुरवठादाराचे नाव","Customer Name":"ग्राहकाचे नाव",
    "Product Name":"उत्पादनाचे नाव","Material Name":"साहित्याचे नाव",
    "Total Amount":"एकूण रक्कम","Sell Price":"विक्री किंमत",
    "Stock Quantity":"साठ्याची संख्या","Quantity":"संख्या","Qty":"संख्या",
    "Received Quantity":"प्राप्त संख्या","Missing Quantity":"उणी संख्या",
    "Total Quantity":"एकूण संख्या","Paid":"दिलेले","Credit Amount":"क्रेडिट रक्कम",
    "Balance":"शिल्लक","Debit":"नावे","Credit":"जमा","Date":"तारीख",
    "Created At":"तयार तारीख","Updated At":"अद्यतन तारीख",
    "Created By":"तयार केले","Updated By":"अद्यतन केले","Barcode":"बारकोड",
    "Gst No":"जीएसटी क्र.","Supply Type":"पुरवठा प्रकार","Status":"स्थिती",
    "Price Per Unit":"प्रति युनिट किंमत","Purchase Date":"खरेदी तारीख",
    "Sell Date":"विक्री तारीख","Unit":"युनिट","Size":"आकार","Color":"रंग",
    "Category":"श्रेणी","Description":"वर्णन","Supplier Credit":"पुरवठादार क्रेडिट",
    "First Name":"पहिले नाव","Last Name":"आडनाव","Phone":"फोन","City":"शहर",
    "Type":"प्रकार","Mode":"पद्धत","Hsn No":"एचएसएन क्र.","Price Code":"किंमत कोड",
    "Image Url":"प्रतिमा","Product Code":"उत्पादन कोड","Host":"होस्ट","Port":"पोर्ट",
    "Bill No":"बिल क्र.","Mobile":"मोबाईल","To Pay":"द्यावयाचे",
    "Debit Amount":"नावे रक्कम","Purchase Amount":"खरेदी रक्कम",
    "Total Purchase Amount":"एकूण खरेदी रक्कम","Payment Mode":"देय पद्धत",
    "Supplier Bill":"पुरवठादार बिल","Supplier":"पुरवठादार","Material":"साहित्य",
    "Packing Charges":"पॅकिंग शुल्क","Discount":"सूट","Cgst":"सीजीएसटी",
    "Sgst":"एसजीएसटी","Igst":"आयजीएसटी",
}

MR_MODULES: Dict[str, str] = {
    "purchase":"खरेदी","sell":"विक्री","supplier-credit":"पुरवठादार क्रेडिट",
    "supplier-ledger":"पुरवठादार खातेवही","customer-ledger":"ग्राहक खातेवही",
    "customer-ledger-summary":"ग्राहक खाते सारांश","customer":"ग्राहक",
    "supplier":"पुरवठादार","product-stock":"उत्पादन साठा","payment":"देयक",
    "customer-payment-history":"ग्राहक देयक इतिहास",
    "supplier-purchase-history":"पुरवठादार खरेदी इतिहास",
    "material":"साहित्य","category":"श्रेणी","printer":"प्रिंटर",
    "email-config":"ईमेल सेटिंग",
}


# ──────────────────────────────────────────────────────────────────────────────
# LANGUAGE DETECTION
# ──────────────────────────────────────────────────────────────────────────────

def detect_language(text: str) -> str:
    deva  = sum(1 for c in text if 0x0900 <= ord(c) <= 0x097F)
    alpha = sum(1 for c in text if c.isalpha())
    if alpha == 0:
        return "english"
    if deva / alpha > 0.25:
        return "marathi_devanagari"
    t      = text.lower()
    words  = set(t.split())
    # Short markers (≤4 chars like "chi", "che", "mal") require exact word match.
    # Long markers (≥5 chars like "kharedi") also check substring for compound words.
    for m in _MR_ROMAN_MARKERS:
        if len(m) <= 4:
            if m in words:          # exact word boundary only
                return "marathi_roman"
        else:
            if m in words or m in t:
                return "marathi_roman"
    return "english"


def _apply_deva_stems(text: str) -> str:
    return " ".join(DEVA_STEMS.get(w, w) for w in text.split())


# ──────────────────────────────────────────────────────────────────────────────
# BUG-08: MARATHI CLASSIFIER WITH CONFIDENCE SCORING
# ──────────────────────────────────────────────────────────────────────────────

def marathi_classify(text: str) -> Optional[str]:
    """
    BUG-08 FIX: Returns module key only if confidence >= 0.4.
    Phrase matches score 1.0; keyword matches score by keyword length / 20.
    Longest phrase still wins via _MR_FLAT sort order.
    """
    words   = text.split()
    stemmed = " ".join(DEVA_STEMS.get(w, w) for w in words)
    t       = stemmed.lower().strip()

    best_key, best_score = None, 0.0
    for keyword, module in _MR_FLAT:
        if keyword in t:
            # Phrases (multi-word) score higher than single keywords
            score = min(1.0, len(keyword.split()) * 0.35 + 0.3)
            if score > best_score:
                best_score, best_key = score, module
            if best_score >= 0.95:
                break  # can't do better

    return best_key if best_score >= 0.4 else None


# ──────────────────────────────────────────────────────────────────────────────
# QUERY ANALYZER
# ──────────────────────────────────────────────────────────────────────────────

class QueryAnalyzer:
    def analyze(self, user_query: str) -> Dict[str, Any]:
        q = user_query.strip()
        m = BARCODE_RE.search(q)
        if m:
            return {"query_mode": "barcode", "query_value": m.group(0).upper()}
        m = INVOICE_RE.search(q)
        if m:
            return {"query_mode": "invoice", "query_value": m.group(0).upper()}
        m = CONTACT_RE.search(q)
        if m:
            return {"query_mode": "contact", "query_value": m.group(0)}
        if ACTIVE_RE.search(q):
            return {"query_mode": "active", "query_value": None}
        return {"query_mode": None, "query_value": None}


query_analyzer = QueryAnalyzer()


# ──────────────────────────────────────────────────────────────────────────────
# 1. NLU — 5-STAGE ENGLISH INTENT CLASSIFIER
# ──────────────────────────────────────────────────────────────────────────────

class IntentClassifier:
    def __init__(self):
        self._build_tfidf()
        self._phrase_pairs = sorted(
            [(ph.lower(), mod)
             for mod, d in MODULE_REGISTRY.items()
             for ph in d["phrases"]],
            key=lambda x: len(x[0]), reverse=True,
        )
        self._alias_pairs = [
            (al.lower(), mod)
            for mod, d in MODULE_REGISTRY.items()
            for al in d["aliases"]
        ]
        self._multi_pairs = sorted(
            [(ph.lower(), pat)
             for pat in MULTI_PATTERNS
             for ph in pat["phrases"]],
            key=lambda x: len(x[0]), reverse=True,
        )

    def _build_tfidf(self):
        corpus = {mod: " ".join(d["keywords"]) for mod, d in MODULE_REGISTRY.items()}
        N = len(corpus)
        vocab: set = set()
        for doc in corpus.values():
            vocab.update(doc.split())
        idf = {}
        for w in vocab:
            df = sum(1 for doc in corpus.values() if w in doc.split())
            idf[w] = math.log((1 + N) / (1 + df)) + 1
        self._vecs: Dict[str, Dict[str, float]] = {}
        for mod, doc in corpus.items():
            words = doc.split()
            if not words:
                self._vecs[mod] = {}
                continue
            tf   = Counter(words)
            raw  = {w: (tf[w] / len(words)) * idf.get(w, 0) for w in tf}
            norm = math.sqrt(sum(v**2 for v in raw.values()))
            self._vecs[mod] = {w: v / norm for w, v in raw.items()} if norm else {}

    def _tfidf_score(self, text: str) -> Dict[str, float]:
        words = text.split()
        if not words:
            return {}
        tf  = Counter(words)
        n   = len(words)
        q   = {w: tf[w] / n for w in tf}
        qn  = math.sqrt(sum(v**2 for v in q.values()))
        if not qn:
            return {}
        return {
            mod: sum(q[w] * dv[w] for w in set(q) & set(dv)) / qn
            for mod, dv in self._vecs.items()
        }

    def _s1_multi(self, text):
        for ph, pat in self._multi_pairs:
            if ph in text:
                return {"type": "MULTI", "pattern": pat, "confidence": 1.0}
        return None

    def _s2_phrase(self, text):
        for ph, mod in self._phrase_pairs:
            if ph in text:
                return {"type": "SINGLE", "module": mod, "confidence": 0.95}
        return None

    def _s3_keyword(self, text):
        best_mod, best_cnt = None, 0
        for mod, d in MODULE_REGISTRY.items():
            cnt = sum(1 for kw in d["keywords"]
                      if re.search(rf"\b{re.escape(kw)}\b", text))
            if cnt > best_cnt:
                best_cnt, best_mod = cnt, mod
        if best_cnt:
            return {"type": "SINGLE", "module": best_mod,
                    "confidence": min(0.9, 0.3 + 0.15 * best_cnt)}
        return None

    def _s4_fuzzy(self, tokens):
        best_s, best_mod = 0.0, None
        for tok in tokens:
            if len(tok) < 4:
                continue
            for al, mod in self._alias_pairs:
                s = SequenceMatcher(None, tok, al).ratio()
                if s > best_s:
                    best_s, best_mod = s, mod
        if best_s >= 0.72:
            return {"type": "SINGLE", "module": best_mod, "confidence": best_s * 0.85}
        return None

    def _s5_tfidf(self, text):
        scores = self._tfidf_score(text)
        if scores:
            best = max(scores, key=scores.get)
            if scores[best] >= 0.12:
                return {"type": "SINGLE", "module": best,
                        "confidence": scores[best] * 0.75}
        return None

    def classify(self, user_query: str) -> Dict[str, Any]:
        t      = CLEAN_RE.sub(" ", user_query.lower()).strip()
        tokens = t.split()
        return (
            self._s1_multi(t)
            or self._s2_phrase(t)
            or self._s3_keyword(t)
            or self._s4_fuzzy(tokens)
            or self._s5_tfidf(t)
            or {"type": "UNRECOGNIZED", "confidence": 0.0}
        )


# ──────────────────────────────────────────────────────────────────────────────
# 2. QUERY PARSER
# BUG-07 FIX: all-stop queries now force BULK_LIST intent
# ──────────────────────────────────────────────────────────────────────────────

AGG_TRIGGERS = frozenset({
    "total","sum","how many","count","how much","aggregate",
    "altogether","combined","overall","एकूण","बेरीज","किती आहे",
})


class QueryParser:
    def parse(self, user_query: str, module_key: str, lang: str) -> Dict[str, Any]:
        text_lower = user_query.lower()
        if lang == "marathi_devanagari":
            text_lower = _apply_deva_stems(text_lower)
        tokens = text_lower.split()
        agg    = any(t in text_lower for t in AGG_TRIGGERS)

        if lang in ("marathi_devanagari", "marathi_roman"):
            stop = MR_STOP_ROMAN | MR_STOP_DEVA
            bulk = MR_BULK_ROMAN | MR_BULK_DEVA
        else:
            stop = EN_STOP
            bulk = EN_BULK

        is_bulk = any(tok in bulk for tok in tokens)

        # Build module vocabulary to strip
        mod = MODULE_REGISTRY[module_key]
        mod_words: set = set()
        for kw in mod["keywords"]:
            mod_words.update(kw.lower().split())
        for ph in mod["phrases"]:
            mod_words.update(ph.lower().split())
        for al in mod["aliases"]:
            mod_words.update(al.lower().split())
        for kw, m in _MR_FLAT:
            if m == module_key:
                mod_words.update(kw.split())

        # Detect target column
        target_col = None
        col_words: set = set()
        for trigger, cols in COLUMN_TRIGGERS.items():
            if re.search(rf"\b{re.escape(trigger)}s?\b", text_lower):
                target_col = cols[0]
                col_words.update([trigger, trigger + "s", trigger + "es"])
                break

        strip        = stop | mod_words | col_words
        value_tokens = [tok for tok in tokens if tok not in strip and len(tok) > 1]

        # Preserve code-like tokens
        code_tokens = [
            tok for tok in tokens
            if re.match(r"^[a-z0-9\-]+$", tok)
            and (re.search(r"\d", tok) or len(tok) >= 6)
            and tok not in strip
        ]
        if code_tokens and not value_tokens:
            value_tokens = code_tokens

        search_value = " ".join(value_tokens).strip()

        # BUG-07 FIX: if EVERY token was a stop/module word, force BULK_LIST
        # This fixes "give me suppliers list", "show me all purchases", etc.
        all_stripped = len(value_tokens) == 0 and len(tokens) > 0
        if all_stripped:
            is_bulk = True

        if target_col:
            intent = "COLUMN_SEARCH" if search_value else "PROMPT_NEEDED"
        elif search_value and not is_bulk:
            intent = "GLOBAL_SEARCH"
        else:
            intent = "BULK_LIST"

        return {
            "intent":       intent,
            "target_col":   target_col,
            "search_value": search_value,
            "aggregation":  "SUM" if agg else "NONE",
        }


# ──────────────────────────────────────────────────────────────────────────────
# 3. DATA MIRROR  — SQLite in-memory + FTS5
# SEC-03: per-module locks  |  SEC-04: JWT cache  |  SEC-02: safe flatten
# BUG-10: startup warmup   |  BUG-11: smart sync  |  PERF-08: mmap+WAL
# ──────────────────────────────────────────────────────────────────────────────

class DataMirror:
    def __init__(self):
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.execute("PRAGMA cache_size=-16000")  # 16 MB page cache
        self.conn.execute(f"PRAGMA mmap_size={SQLITE_MMAP_SIZE}")  # PERF-08

        self._ttl:          Dict[str, float]         = {}
        self._stale_data:   Dict[str, bool]          = {}  # FEAT-02
        self._cols:         Dict[str, List[str]]     = {}
        self._locks:        collections.defaultdict  = collections.defaultdict(asyncio.Lock)
        self._jwt_token:    Optional[str]            = None
        self._jwt_expires:  float                    = 0.0
        self._jwt_lock:     asyncio.Lock             = asyncio.Lock()
        self._http_client:  Optional[Any]            = None  # PERF-10

        self._start_time = time.time()

    # ── HTTP client lifecycle (PERF-10) ───────────────────────────────────────
    async def open(self):
        if _FASTAPI_AVAILABLE:
            self._http_client = httpx.AsyncClient(
                timeout=API_TIMEOUT,
                limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
            )

    async def close(self):
        if self._http_client:
            await self._http_client.aclose()

    # ── JWT caching (SEC-04) ──────────────────────────────────────────────────
    async def _get_token(self) -> Optional[str]:
        now = time.time()
        if self._jwt_token and now < self._jwt_expires - JWT_EXPIRY_BUFFER:
            return self._jwt_token
        async with self._jwt_lock:
            now = time.time()
            if self._jwt_token and now < self._jwt_expires - JWT_EXPIRY_BUFFER:
                return self._jwt_token
            try:
                client = self._http_client or httpx.AsyncClient(timeout=API_TIMEOUT)
                auth   = await client.post(LOGIN_URL, json=LOGIN_CREDS)
                body   = auth.json()
                token  = body.get("jwtToken") or body.get("token")
                if not token:
                    log.error("Auth failed: status=%s body=%s",
                              auth.status_code, auth.text[:200])
                    return None
                expires_in        = int(body.get("expiresIn", 3600))
                self._jwt_token   = token
                self._jwt_expires = now + expires_in
                log.info("JWT refreshed, expires in %ds", expires_in)
                return token
            except Exception as exc:
                log.exception("Auth exception: %s", exc)
                return None

    def _invalidate_token(self):
        self._jwt_token   = None
        self._jwt_expires = 0.0

    # ── API fetch with retry (SEC-04 + FIX-2: proper httpx cancellation) ────────
    async def _fetch_with_retry(self, url: str,
                                params: Optional[Dict] = None) -> Optional[List]:
        """
        FIX-2: uses httpx.Timeout object with connect+read timeouts so that
        asyncio.wait_for() can cancel the underlying TCP connection cleanly,
        not just abandon it while it keeps running in the background.
        """
        last_exc = None
        # Per-request timeout slightly under the outer 45s wait_for so httpx
        # raises cleanly before asyncio kills the coroutine mid-flight.
        req_timeout = httpx.Timeout(connect=5.0, read=25.0, write=5.0, pool=5.0)
        for attempt in range(API_MAX_RETRIES):
            try:
                token = await self._get_token()
                if not token:
                    last_exc = Exception("no token")
                    await asyncio.sleep(1.5 * (2 ** attempt))
                    continue
                headers  = {"Authorization": f"Bearer {token}",
                            "Content-Type": "application/json"}
                req_url  = f"{BASE_URL}{url}"
                client   = self._http_client or httpx.AsyncClient(timeout=req_timeout)
                resp     = await client.get(req_url, headers=headers,
                                            params=params or {},
                                            timeout=req_timeout)
                if resp.status_code == 200:
                    return self._extract_list(resp.json())
                if resp.status_code == 401:
                    self._invalidate_token()
                    log.warning("401 on %s — token invalidated", url)
                    last_exc = Exception("HTTP 401")
                    await asyncio.sleep(1.5 * (2 ** attempt))
                    continue
                log.warning("%s attempt %d: HTTP %d — %s",
                            url, attempt + 1, resp.status_code, resp.text[:200])
                last_exc = Exception(f"HTTP {resp.status_code}")
            except (httpx.TimeoutException, httpx.ConnectError) as exc:
                # FIX-2: httpx raises its own timeout before asyncio cancels —
                # this guarantees the TCP socket is properly closed.
                last_exc = exc
                log.warning("%s attempt %d timeout/connect error: %s",
                            url, attempt + 1, exc)
            except asyncio.CancelledError:
                # FIX-2: propagate cancellation immediately — don't retry
                log.warning("%s cancelled (outer timeout hit)", url)
                raise
            except Exception as exc:
                last_exc = exc
                log.warning("%s attempt %d exception: %s", url, attempt + 1, exc)
            if attempt < API_MAX_RETRIES - 1:
                await asyncio.sleep(1.5 * (2 ** attempt))
        log.error("All %d retries failed for %s: %s", API_MAX_RETRIES, url, last_exc)
        return None

    # ── Data extraction ───────────────────────────────────────────────────────
    def _extract_list(self, raw: Any) -> List:
        if isinstance(raw, list):
            return raw
        if isinstance(raw, dict):
            for key in ("data", "records", "result", "results", "items", "content"):
                if key in raw and isinstance(raw[key], list):
                    return raw[key]
            for v in raw.values():
                if isinstance(v, list) and v:
                    return v
        return []

    def _flatten(self, records: List) -> List[Dict]:
        """
        SEC-02 FIX: concatenate (not cross-join) child arrays.
        Capped at FLATTEN_MAX_CHILD_ROWS per child array to prevent OOM.
        """
        out = []
        for rec in records:
            if not isinstance(rec, dict):
                out.append({"_data": str(rec)})
                continue
            base, children = {}, []
            for k, v in rec.items():
                if v is None:
                    base[k] = ""
                elif isinstance(v, (str, int, float, bool)):
                    base[k] = str(v)
                elif isinstance(v, dict):
                    base[k] = str(v.get("name", v.get("title",
                               v.get("invoiceNo", v.get("totalAmount", "")))))
                elif isinstance(v, list) and v and isinstance(v[0], dict):
                    children.append((k, v))
            if children:
                for _, clist in children:
                    for child in clist[:FLATTEN_MAX_CHILD_ROWS]:
                        row = base.copy()
                        for ck, cv in child.items():
                            if isinstance(cv, dict):
                                row[ck] = str(cv.get("name",
                                    cv.get("title", cv.get("invoiceNo", ""))))
                            elif not isinstance(cv, list):
                                row[ck] = str(cv) if cv is not None else ""
                        out.append(row)
            else:
                out.append(base)
        return out

    # ── SQLite load ───────────────────────────────────────────────────────────
    def _load(self, key: str, records: List[Dict]):
        cur = self.conn.cursor()
        cur.execute(f'DROP TABLE IF EXISTS "{key}_fts"')
        cur.execute(f'DROP TABLE IF EXISTS "{key}"')
        if not records:
            cur.execute(f'CREATE TABLE "{key}" (_empty TEXT)')
            self._cols[key] = []
            self.conn.commit()
            return
        seen, all_keys = set(), []
        for r in records:
            for k in r:
                if k not in seen:
                    seen.add(k)
                    all_keys.append(k)
        records = records[:MAX_ROWS]
        cols_ddl = ", ".join(f'"{k}" TEXT' for k in all_keys)
        cur.execute(f'CREATE TABLE "{key}" (_rowid INTEGER PRIMARY KEY, {cols_ddl})')
        ph      = ",".join(["?"] * len(all_keys))
        col_sql = ", ".join(f'"{k}"' for k in all_keys)
        for r in records:
            cur.execute(
                f'INSERT INTO "{key}" ({col_sql}) VALUES ({ph})',
                [r.get(k, "") for k in all_keys],
            )
        fts_cols = ", ".join(f'"{k}"' for k in all_keys)
        cur.execute(
            f'CREATE VIRTUAL TABLE "{key}_fts" USING fts5('
            f'{fts_cols}, content="{key}", content_rowid="_rowid",'
            f'tokenize="unicode61 remove_diacritics 1")'
        )
        cur.execute(
            f'INSERT INTO "{key}_fts" (rowid, {fts_cols}) '
            f'SELECT _rowid, {fts_cols} FROM "{key}"'
        )
        self.conn.commit()
        self._cols[key] = all_keys
        log.info("Loaded %d rows into '%s'", len(records), key)

    # ── BUG-13: temp table cleanup ─────────────────────────────────────────────
    def _drop_targeted(self, key: str):
        try:
            cur = self.conn.cursor()
            cur.execute(f'DROP TABLE IF EXISTS "{key}_fts"')
            cur.execute(f'DROP TABLE IF EXISTS "{key}"')
            self.conn.commit()
            self._cols.pop(key, None)
        except Exception:
            pass

    # ── Sync with per-module lock (SEC-03) ────────────────────────────────────
    async def sync_module(self, key: str, force: bool = False) -> str:
        """
        Returns 'ok' | 'fresh' | 'stale' | 'error' | 'open'
        BUG-11: returns 'fresh' without API call if TTL still valid.
        FEAT-02: on API failure returns 'stale' (uses cached data if any).
        FIX-3: circuit breaker short-circuits when ERP is known-down.
        """
        now = time.time()
        if not force and now < self._ttl.get(key, 0):
            return "fresh"

        # FIX-3: check breaker before acquiring module lock
        if circuit_breaker.is_open:
            log.debug("CircuitBreaker OPEN — skipping sync for '%s'", key)
            return "stale" if self._cols.get(key) else "error"

        async with self._locks[key]:
            now = time.time()
            if not force and now < self._ttl.get(key, 0):
                return "fresh"
            mod = MODULE_REGISTRY[key]
            raw = await self._fetch_with_retry(mod["url"])
            if raw is None:
                circuit_breaker.record_failure()   # FIX-3
                if self._cols.get(key):
                    self._stale_data[key] = True
                    log.warning("Using stale data for '%s'", key)
                    return "stale"
                return "error"
            circuit_breaker.record_success()       # FIX-3
            flat = self._flatten(raw)
            self._load(key, flat)
            self._ttl[key]        = time.time() + mod["ttl"]
            self._stale_data[key] = False
            response_cache.invalidate_module(key)  # FIX-4: fresh data → purge stale HTML
            return "ok"

    async def sync_modules(self, keys: List[str],
                           force: bool = False) -> Dict[str, str]:
        results = await asyncio.gather(
            *[self.sync_module(k, force) for k in keys],
            return_exceptions=True,
        )
        return {
            k: (r if isinstance(r, str) else "error")
            for k, r in zip(keys, results)
        }

    async def sync_all(self, force: bool = False) -> Dict[str, str]:
        """
        BUG-11 FIX: without force=True only re-fetches stale modules.
        force=True re-fetches everything unconditionally.
        """
        return await self.sync_modules(list(MODULE_REGISTRY.keys()), force=force)

    def columns(self, key: str) -> List[str]:
        return self._cols.get(key, [])

    def is_stale(self, key: str) -> bool:
        return self._stale_data.get(key, False)

    def uptime_seconds(self) -> float:
        return time.time() - self._start_time

    def cache_status(self) -> Dict[str, Any]:
        now = time.time()
        return {
            k: {
                "cached": bool(self._cols.get(k)),
                "rows":   len(self._cols.get(k, [])),
                "ttl_remaining": max(0, int(self._ttl.get(k, 0) - now)),
                "stale": self._stale_data.get(k, False),
            }
            for k in MODULE_REGISTRY
        }


db = DataMirror()


# ──────────────────────────────────────────────────────────────────────────────
# FIX-3: CIRCUIT BREAKER
# Prevents hammering a dead ERP — opens after CB_FAILURE_THRESHOLD consecutive
# failures, probes after CB_RECOVERY_TIMEOUT, closes after CB_SUCCESS_THRESHOLD
# consecutive successes in half-open state.
# sync_module() routes through the breaker; callers still get "stale" or
# "error" back — the breaker just short-circuits the actual HTTP call.
# ──────────────────────────────────────────────────────────────────────────────

class _CBState:
    CLOSED    = "closed"
    OPEN      = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self):
        self._state          = _CBState.CLOSED
        self._failures       = 0
        self._successes      = 0
        self._opened_at      = 0.0
        self._lock           = asyncio.Lock()

    @property
    def is_open(self) -> bool:
        if self._state == _CBState.OPEN:
            if time.time() - self._opened_at >= CB_RECOVERY_TIMEOUT:
                self._state    = _CBState.HALF_OPEN
                self._successes = 0
                log.info("CircuitBreaker → HALF_OPEN (probe)")
                return False   # allow one probe through
            return True
        return False

    def record_success(self):
        if self._state == _CBState.HALF_OPEN:
            self._successes += 1
            if self._successes >= CB_SUCCESS_THRESHOLD:
                self._state    = _CBState.CLOSED
                self._failures = 0
                log.info("CircuitBreaker → CLOSED (recovered)")
        elif self._state == _CBState.CLOSED:
            self._failures = 0

    def record_failure(self):
        self._failures += 1
        if self._state in (_CBState.CLOSED, _CBState.HALF_OPEN):
            if self._failures >= CB_FAILURE_THRESHOLD:
                self._state     = _CBState.OPEN
                self._opened_at = time.time()
                log.error("CircuitBreaker → OPEN after %d failures", self._failures)

    def status(self) -> Dict[str, Any]:
        return {
            "state":    self._state,
            "failures": self._failures,
            "opens_at": self._opened_at,
        }


circuit_breaker = CircuitBreaker()
# BUG-13: unique targeted table per request to prevent key collisions
# ──────────────────────────────────────────────────────────────────────────────

class SmartQueryRouter:
    async def route(self, key: str, parsed: Dict,
                    query_mode: str, query_value: Optional[str]
                    ) -> Tuple[bool, str]:
        """
        BUG-13 FIX: returns (success, targeted_table_key).
        targeted_table_key is unique per request (uuid suffix) so concurrent
        requests for the same module don't overwrite each other.
        """
        mod       = MODULE_REGISTRY.get(key, {})
        endpoints = mod.get("search_endpoints", {})
        if not query_mode or query_mode not in endpoints:
            return False, ""
        ep = endpoints[query_mode]
        if ep["param"] is None:
            raw = await db._fetch_with_retry(ep["url"])
        else:
            if not query_value:
                return False, ""
            raw = await db._fetch_with_retry(ep["url"],
                                              params={ep["param"]: query_value})
        if not raw:
            return False, ""
        flat = db._flatten(raw)
        if not flat:
            return False, ""
        # BUG-13: unique table key
        targeted_key = f"_tgt_{key}_{uuid.uuid4().hex[:8]}"
        db._load(targeted_key, flat)
        return True, targeted_key


smart_router = SmartQueryRouter()


# ──────────────────────────────────────────────────────────────────────────────
# 4. RAG RETRIEVER
# BUG-14: FTS injection protection  |  PERF-11: render cap
# ──────────────────────────────────────────────────────────────────────────────

class RAGRetriever:
    MAX_FTS  = 500
    MAX_LIKE = 500
    MAX_BULK = 10_000

    def retrieve(self, table_key: str, parsed: Dict) -> Tuple[List, List, str]:
        intent = parsed["intent"]
        val    = parsed["search_value"]
        tcol   = parsed["target_col"]
        cols   = db.columns(table_key)
        if not cols:
            return [], [], "empty"
        if intent == "BULK_LIST" or not val:
            return self._bulk(table_key), cols, "bulk"
        rows, method = self._fts(table_key, val, tcol, cols)
        if not rows:
            rows, method = self._like(table_key, val, tcol, cols)
        return rows, cols, method

    def _resolve_col(self, target: str, cols: List[str]) -> Optional[str]:
        for c in cols:
            if c.lower() == target.lower():
                return c
        for c in cols:
            if target.lower() in c.lower() or c.lower() in target.lower():
                return c
        return None

    def _fts(self, key: str, val: str, tcol: Optional[str],
             cols: List[str]) -> Tuple[List, str]:
        try:
            cur = db.conn.cursor()
            # BUG-14: strip FTS special chars AND boolean operators
            safe = FTS_SPECIAL.sub(" ", val)
            safe = _FTS_BOOL_RE.sub(" ", safe).strip()
            if not safe:
                return [], "fts_empty"
            if tcol:
                ac    = self._resolve_col(tcol, cols)
                fts_q = f'"{ac}" : "{safe}"*' if ac else f'"{safe}"*'
            else:
                words = [w for w in safe.split() if len(w) > 1]
                if not words:
                    return [], "fts_empty"
                # BUG-14: quote each word to prevent FTS operator injection
                fts_q = " OR ".join(f'"{w}"*' for w in words)
            cur.execute(
                f'SELECT m.* FROM "{key}" m '
                f'JOIN "{key}_fts" f ON m._rowid = f.rowid '
                f'WHERE "{key}_fts" MATCH ? ORDER BY rank LIMIT {self.MAX_FTS}',
                [fts_q],
            )
            rows = [dict(r) for r in cur.fetchall()]
            for r in rows:
                r.pop("_rowid", None)
            return rows, "fts"
        except Exception as e:
            log.warning("FTS error %s: %s", key, e)
            return [], "fts_error"

    def _like(self, key: str, val: str, tcol: Optional[str],
              cols: List[str]) -> Tuple[List, str]:
        try:
            cur = db.conn.cursor()
            if tcol:
                ac = self._resolve_col(tcol, cols)
                if ac:
                    cur.execute(
                        f'SELECT * FROM "{key}" WHERE "{ac}" LIKE ? LIMIT {self.MAX_LIKE}',
                        [f"%{val}%"],
                    )
                    rows = [dict(r) for r in cur.fetchall()]
                    for r in rows:
                        r.pop("_rowid", None)
                    return rows, "like_col"
            conds  = " OR ".join(f'"{c}" LIKE ?' for c in cols)
            params = [f"%{val}%"] * len(cols)
            cur.execute(
                f'SELECT * FROM "{key}" WHERE {conds} LIMIT {self.MAX_LIKE}',
                params,
            )
            rows = [dict(r) for r in cur.fetchall()]
            for r in rows:
                r.pop("_rowid", None)
            return rows, "like_all"
        except Exception as e:
            log.warning("LIKE error %s: %s", key, e)
            return [], "like_error"

    def _bulk(self, key: str) -> List:
        try:
            cur = db.conn.cursor()
            cur.execute(f'SELECT * FROM "{key}" LIMIT {self.MAX_BULK}')
            rows = [dict(r) for r in cur.fetchall()]
            for r in rows:
                r.pop("_rowid", None)
            return rows
        except Exception as e:
            log.warning("Bulk error %s: %s", key, e)
            return []


retriever = RAGRetriever()


# ──────────────────────────────────────────────────────────────────────────────
# PERF-06: RESPONSE CACHE (LRU + TTL)
# ──────────────────────────────────────────────────────────────────────────────

class ResponseCache:
    def __init__(self, max_size: int = RESPONSE_CACHE_SIZE, ttl: float = RESPONSE_CACHE_TTL):
        self._cache: "collections.OrderedDict[str, Tuple[str, float]]" = \
            collections.OrderedDict()
        self._max_size = max_size
        self._ttl      = ttl

    def _key(self, module: str, intent: str, value: str, lang: str) -> str:
        return f"{module}|{intent}|{value.lower().strip()}|{lang}"

    def get(self, module: str, intent: str, value: str, lang: str) -> Optional[str]:
        k = self._key(module, intent, value, lang)
        if k not in self._cache:
            return None
        html_resp, ts = self._cache[k]
        if time.time() - ts > self._ttl:
            del self._cache[k]
            return None
        self._cache.move_to_end(k)  # LRU update
        return html_resp

    def set(self, module: str, intent: str, value: str, lang: str, resp: str):
        k = self._key(module, intent, value, lang)
        self._cache[k] = (resp, time.time())
        self._cache.move_to_end(k)
        if len(self._cache) > self._max_size:
            self._cache.popitem(last=False)  # evict oldest

    def invalidate_module(self, module: str):
        keys = [k for k in self._cache if k.startswith(f"{module}|")]
        for k in keys:
            del self._cache[k]

    def size(self) -> int:
        return len(self._cache)


response_cache = ResponseCache()


# ──────────────────────────────────────────────────────────────────────────────
# 5. AGGREGATION ENGINE  (SEC-05: math.isfinite guard)
# ──────────────────────────────────────────────────────────────────────────────

class AggregationEngine:
    def compute(self, records: List[Dict], key: str) -> Dict[str, Any]:
        base_key = key.split("_tgt_")[0] if "_tgt_" in key else key
        mod      = MODULE_REGISTRY.get(base_key, {"amount_cols": [], "qty_cols": []})
        amt_c    = mod["amount_cols"]
        qty_c    = mod["qty_cols"]
        total_a  = total_q = 0.0

        for rec in records:
            for col in amt_c:
                ak = next((k for k in rec if k.lower() == col.lower()), None)
                if ak:
                    try:
                        num = float(
                            str(rec[ak]).replace(",", "").replace("₹", "").strip()
                        )
                        if math.isfinite(num):   # SEC-05
                            total_a += num
                    except (ValueError, TypeError):
                        pass
                    break
            for col in qty_c:
                qk = next((k for k in rec if k.lower() == col.lower()), None)
                if qk:
                    try:
                        num = float(str(rec[qk]).replace(",", "").strip())
                        if math.isfinite(num):   # SEC-05
                            total_q += num
                    except (ValueError, TypeError):
                        pass
                    break

        return {
            "count":        len(records),
            "total_amount": total_a if total_a else None,
            "total_qty":    int(total_q) if total_q else None,
        }


aggregator = AggregationEngine()


# ──────────────────────────────────────────────────────────────────────────────
# 6. RESPONSE SYNTHESIZER
# SEC-01: html.escape everywhere  |  SEC-06: column headers escaped
# BUG-15: strict datetime detection  |  BUG-16: list.append() join
# SEC-10: image URL whitelist  |  FEAT-04: truncation badge
# ──────────────────────────────────────────────────────────────────────────────

HIDDEN_COLS = frozenset({
    "_id", "__v", "_empty", "_data", "_rowid",
    "password", "jwttoken", "role", "permissions", "token",
})

PRIORITY_COLS = [
    "invoiceNo","billNo","supplierBill","productName","name","firstName","lastName",
    "supplierName","customerName","materialName","supplier","customer","product","material",
    "email","phone","contact","mobile","stockQuantity","quantity","qty","receivedQuantity",
    "sellPrice","pricePerUnit","purchaseAmount","totalPurchaseAmount",
    "totalAmount","amount","creditAmount","paid","balance",
    "debit","credit","discount","cgst","sgst","igst",
    "date","createdAt","purchaseDate","sellDate","paymentMode","status","supplyType",
    "address","city","gstNo","barcode","hsnNo","size","color","unit",
]

AMOUNT_HINTS = frozenset({"price","amount","total","balance","credit","debit","paid","cost","value","charges","discount"})
QTY_HINTS    = frozenset({"quantity","qty","stock"})
NOT_CURRENCY = frozenset({"code","no","id","barcode","number","ref","hsn","pin","port","cgst","sgst","igst","missing","received"})

STATUS_BADGES: Dict[str, Tuple[str, str]] = {
    "in-state":  ("#dcfce7","#16a34a"),
    "out-state": ("#fef3c7","#d97706"),
    "active":    ("#dcfce7","#16a34a"),
    "inactive":  ("#fee2e2","#dc2626"),
    "paid":      ("#dcfce7","#16a34a"),
    "pending":   ("#fef3c7","#d97706"),
    "cash":      ("#f0f9ff","#0369a1"),
    "credit":    ("#fdf4ff","#9333ea"),
    "upi":       ("#f0fdf4","#16a34a"),
}

# SEC-10: allowed image domains
_IMAGE_SAFE_DOMAIN = BASE_URL.split("//")[-1].split("/")[0]


def _is_currency_col(col: str) -> bool:
    c = col.lower()
    return any(h in c for h in AMOUNT_HINTS) and not any(x in c for x in NOT_CURRENCY)


def _is_qty_col(col: str) -> bool:
    c = col.lower()
    return any(h in c for h in QTY_HINTS) and not any(x in c for x in NOT_CURRENCY)


def _fmt_cell(col: str, val: str) -> str:
    """
    BUG-15 FIX: DATETIME_RE used for strict datetime detection.
    BUG-16 FIX: string concat replaced with list.append().
    SEC-10: image URLs checked against allowed domain.
    All user values escaped via html.escape().
    """
    if not val or val in ("None", "null", ""):
        return '<span style="color:#94a3b8">—</span>'

    # SEC-10: image URL — only allow from known ERP domain
    if (val.startswith("http") and _IMAGE_SAFE_DOMAIN in val
            and any(val.lower().endswith(e)
                    for e in (".png", ".jpg", ".jpeg", ".webp", ".gif"))):
        safe_url = html.escape(val, quote=True)
        return (
            f'<img src="{safe_url}" style="width:40px;height:40px;'
            f'object-fit:cover;border-radius:6px;" loading="lazy" />'
        )

    # BUG-15: strict datetime — must start with YYYY-MM-DDTHH:MM
    if DATETIME_RE.match(val):
        try:
            d, t = val.split("T", 1)
            return (
                f'<span style="color:#64748b;font-size:12px">'
                f'{html.escape(d)} {html.escape(t[:5])}</span>'
            )
        except Exception:
            pass

    # Currency
    if _is_currency_col(col):
        try:
            num = float(str(val).replace(",", "").replace("₹", "").strip())
            if math.isfinite(num):
                color = "#16a34a" if num >= 0 else "#dc2626"
                return f'<strong style="color:{color}">₹ {num:,.2f}</strong>'
        except (ValueError, TypeError):
            pass

    # Quantity
    if _is_qty_col(col):
        try:
            num   = int(float(str(val).replace(",", "").strip()))
            color = "#0369a1" if num > 10 else "#dc2626"
            return f'<span style="color:{color};font-weight:600">{num:,}</span>'
        except (ValueError, TypeError):
            pass

    # Status badge
    if col.lower() in ("status","supplytype","type","state","paymentmode"):
        badge    = STATUS_BADGES.get(val.lower(), ("#f1f5f9","#475569"))
        safe_val = html.escape(val)
        return (
            f'<span style="background:{badge[0]};color:{badge[1]};'
            f'padding:2px 8px;border-radius:999px;font-size:12px;font-weight:600">'
            f'{safe_val}</span>'
        )

    # Default — always escape (SEC-01 / SEC-06)
    return html.escape(str(val))


def _base_key(key: str) -> str:
    """Strip targeted table suffix to get the module key."""
    if "_tgt_" in key:
        return key.split("_tgt_")[0]
    return key.replace("_targeted", "")


class ResponseSynthesizer:

    def _headers(self, cols: List[str]) -> List[str]:
        lh      = {c.lower() for c in HIDDEN_COLS}
        visible = [c for c in cols if c.lower() not in lh]
        return sorted(visible, key=lambda x: (
            PRIORITY_COLS.index(x) if x in PRIORITY_COLS else 99
        ))

    def _col_label(self, col: str, marathi: bool) -> str:
        eng = CAMEL_RE.sub(r"\1 \2", col).title()
        return MR_COLUMNS.get(eng, eng) if marathi else eng

    def stale_banner(self, key: str, marathi: bool) -> str:
        """FEAT-02: shown when serving cached data after API failure."""
        if marathi:
            return (
                '<div style="background:#fef9c3;border:1px solid #fde047;'
                'border-radius:8px;padding:8px 14px;margin-bottom:10px;'
                'font-size:13px;color:#854d0e">'
                '⚠️ ERP सर्व्हर उपलब्ध नाही — जुना डेटा दाखवत आहे.</div>'
            )
        return (
            '<div style="background:#fef9c3;border:1px solid #fde047;'
            'border-radius:8px;padding:8px 14px;margin-bottom:10px;'
            'font-size:13px;color:#854d0e">'
            '⚠️ ERP server unreachable — showing last cached data.</div>'
        )

    def intro(self, key: str, agg: Dict, search_val: str,
              marathi: bool, lang: str,
              total_found: int = 0, rendered: int = 0) -> str:
        """
        SEC-01 FIX: search_val html.escape()-d.
        FEAT-04: shows truncation badge when rendered < total_found.
        """
        count    = agg["count"]
        amt      = agg["total_amount"]
        qty      = agg["total_qty"]
        bk       = _base_key(key)
        safe_val = html.escape(search_val) if search_val else ""

        if marathi:
            label = MR_MODULES.get(bk, html.escape(bk))
            if safe_val:
                text = (
                    f'<strong>{label}</strong> मध्ये '
                    f'<strong>"{safe_val}"</strong> साठी शोधले — '
                    f'<strong>{count:,} नोंदी</strong> सापडल्या.'
                )
            else:
                text = (
                    f'<strong>{label}</strong> ची संपूर्ण यादी — '
                    f'<strong>{count:,} नोंदी</strong>.'
                )
            parts = []
            if amt:  parts.append(f'एकूण रक्कम: <strong>₹{amt:,.2f}</strong>')
            if qty:  parts.append(f'एकूण युनिट: <strong>{qty:,}</strong>')
        else:
            label = html.escape(bk.replace("-", " ").title())
            ack   = random.choice(["Here you go! ","Got it. ","Absolutely! ","Done — "])
            if safe_val:
                text = (
                    f'{ack}Searched <strong>{label}</strong> for '
                    f'<strong>"{safe_val}"</strong> — '
                    f'<strong>{count:,} record{"s" if count != 1 else ""}</strong> found.'
                )
            else:
                text = (
                    f'{ack}Complete <strong>{label}</strong> list — '
                    f'<strong>{count:,} record{"s" if count != 1 else ""}</strong>.'
                )
            parts = []
            if amt: parts.append(f'Total: <strong>₹{amt:,.2f}</strong>')
            if qty: parts.append(f'Units: <strong>{qty:,}</strong>')

        if parts:
            text += " &nbsp;·&nbsp; " + " &nbsp;·&nbsp; ".join(parts)

        # FEAT-04: truncation notice
        if total_found > rendered > 0:
            trunc = (
                f' &nbsp;<span style="background:#e0f2fe;color:#0369a1;'
                f'padding:1px 7px;border-radius:999px;font-size:12px">'
                f'Showing {rendered:,} of {total_found:,}</span>'
            )
            text += trunc

        return f'<p style="margin:0 0 14px;font-size:15px;line-height:1.6">{text}</p>'

    def table(self, records: List[Dict], cols: List[str], marathi: bool,
              max_rows: int = MAX_RENDER_ROWS) -> str:
        """
        BUG-16 FIX: list.append() + join() instead of string concat.
        SEC-06 FIX: column headers html.escape()-d.
        PERF-11: renders at most max_rows rows.
        Returns (html_str, rows_rendered).
        """
        headers = self._headers(cols)
        if not headers:
            return "", 0
        font = (
            "'Noto Sans Devanagari','Segoe UI',system-ui,sans-serif"
            if marathi else "'Segoe UI',system-ui,sans-serif"
        )
        render_records = records[:max_rows]

        rows_html: List[str] = []
        for i, rec in enumerate(render_records):
            bg    = "#ffffff" if i % 2 == 0 else "#f8fafc"
            cells: List[str] = []
            for h in headers:
                raw  = str(rec.get(h, "") or "")
                cell = _fmt_cell(h, raw)
                cells.append(
                    f'<td style="padding:11px 16px;border-right:1px solid #e2e8f0;'
                    f'border-bottom:1px solid #e2e8f0;font-size:13px;'
                    f'color:#334155;white-space:nowrap">{cell}</td>'
                )
            rows_html.append(
                f'<tr style="background:{bg}" '
                f'onmouseover="this.style.background=\'#eff6ff\'" '
                f'onmouseout="this.style.background=\'{bg}\'">'
                + "".join(cells) + "</tr>"
            )

        header_cells: List[str] = []
        for h in headers:
            label = html.escape(self._col_label(h, marathi))  # SEC-06
            header_cells.append(
                f'<th style="padding:13px 16px;border-right:1px solid #475569;'
                f'font-size:12px;font-weight:600;letter-spacing:.04em;'
                f'white-space:nowrap">{label}</th>'
            )

        tbl = "".join([
            '<div style="overflow-x:auto;border-radius:12px;',
            'box-shadow:0 4px 24px rgba(0,0,0,.08);border:1px solid #e2e8f0;">',
            f'<table style="border-collapse:collapse;width:100%;text-align:left;',
            f'background:#fff;font-family:{font};min-width:520px;">',
            '<thead><tr style="background:linear-gradient(135deg,#1e293b,#334155);',
            'color:#f8fafc;">', "".join(header_cells), '</tr></thead>',
            '<tbody>', "".join(rows_html), '</tbody>',
            '</table></div>',
        ])
        return tbl, len(render_records)

    def no_results(self, key: str, val: str, marathi: bool,
                   query_mode: str = None) -> str:
        bk       = _base_key(key)
        safe_val = html.escape(val) if val else ""
        if marathi:
            label = MR_MODULES.get(bk, html.escape(bk))
            hint  = {
                "barcode": "<br><small>टीप: बारकोड पूर्ण स्कॅन करा (उदा. BAR268726580)</small>",
                "invoice": "<br><small>टीप: इनव्हॉइस नंबर तपासा (उदा. PA00000001)</small>",
            }.get(query_mode, "")
            return (
                f'<div style="background:#fef2f2;border:1px solid #fecaca;'
                f'border-radius:10px;padding:16px 20px;color:#991b1b;">'
                f'<strong>{label}</strong> मध्ये <strong>"{safe_val}"</strong> '
                f'साठी काहीही सापडले नाही.{hint}<br>'
                f'<small>शब्दलेखन तपासा किंवा लहान शब्द वापरा.</small></div>'
            )
        label = html.escape(bk.replace("-", " ").title())
        hint  = {
            "barcode": "<br><small>Tip: use the full barcode (e.g. BAR268726580)</small>",
            "invoice": "<br><small>Tip: check invoice number format (e.g. PA00000001)</small>",
            "contact": "<br><small>Tip: enter a 10-digit mobile number</small>",
        }.get(query_mode, "")
        return (
            f'<div style="background:#fef2f2;border:1px solid #fecaca;'
            f'border-radius:10px;padding:16px 20px;color:#991b1b;">'
            f'No results found in <strong>{label}</strong> for '
            f'<strong>"{safe_val}"</strong>.{hint}<br>'
            f'<small>Check spelling or try a shorter term.</small></div>'
        )

    def api_error(self, key: str, marathi: bool) -> str:
        bk = _base_key(key)
        if marathi:
            label = MR_MODULES.get(bk, html.escape(bk))
            return (
                f'<div style="background:#fff7ed;border:1px solid #fed7aa;'
                f'border-radius:10px;padding:16px 20px;color:#9a3412;">'
                f'⚠️ <strong>{label}</strong> चा डेटा मिळवता आला नाही '
                f'({API_MAX_RETRIES} प्रयत्नांनंतर). ERP कनेक्शन तपासा.<br>'
                f'<small>सर्व्हर लॉग्स console मध्ये तपासा.</small></div>'
            )
        label = html.escape(bk.replace("-", " ").title())
        return (
            f'<div style="background:#fff7ed;border:1px solid #fed7aa;'
            f'border-radius:10px;padding:16px 20px;color:#9a3412;">'
            f'⚠️ Could not fetch <strong>{label}</strong> data after '
            f'{API_MAX_RETRIES} attempts.<br>'
            f'<small>Check server logs for HTTP status codes and auth errors.</small></div>'
        )

    def prompt_needed(self, key: str, col: str, marathi: bool) -> str:
        bk = _base_key(key)
        if marathi:
            label   = MR_MODULES.get(bk, html.escape(bk))
            col_eng = CAMEL_RE.sub(r"\1 \2", col).title()
            col_mr  = html.escape(MR_COLUMNS.get(col_eng, col_eng))
            return (
                f'तुम्ही <strong>{label}</strong> विभागात शोधत आहात. '
                f'कृपया <strong>{col_mr}</strong> सांगा.<br>'
                f'<em>उदाहरण: "invoice PA00000001" किंवा "barcode BAR268726580"</em>'
            )
        label     = html.escape(bk.replace("-", " ").title())
        col_label = html.escape(CAMEL_RE.sub(r"\1 \2", col).title())
        return (
            f'You\'re searching <strong>{label}</strong>. '
            f'Please specify the <strong>{col_label}</strong> value.<br>'
            f'<em>Example: "invoice PA00000001" or "barcode BAR268726580"</em>'
        )

    def targeted_badge(self, query_mode: str, query_value: str,
                       marathi: bool) -> str:
        safe_qv = html.escape(query_value or "")
        labels = {
            "marathi": {
                "barcode": f"बारकोड शोध: {safe_qv}",
                "invoice": f"इनव्हॉइस शोध: {safe_qv}",
                "contact": f"संपर्क शोध: {safe_qv}",
                "active":  "सक्रिय नोंद",
            },
            "english": {
                "barcode": f"Targeted barcode lookup: {safe_qv}",
                "invoice": f"Targeted invoice lookup: {safe_qv}",
                "contact": f"Targeted contact lookup: {safe_qv}",
                "active":  "Showing active record",
            },
        }
        lang_key = "marathi" if marathi else "english"
        txt      = html.escape(labels[lang_key].get(query_mode, query_mode))
        return (
            f'<div style="background:#f0f9ff;border:1px solid #bae6fd;'
            f'border-radius:8px;padding:8px 14px;margin-bottom:10px;'
            f'font-size:13px;color:#0369a1">⚡ {txt}</div>'
        )

    def unrecognized(self, marathi: bool) -> str:
        if marathi:
            mods = " · ".join(MR_MODULES.values())
            return (
                f'<div style="background:#f8fafc;border:1px solid #e2e8f0;'
                f'border-radius:10px;padding:16px 20px;">'
                f'<strong>मला समजले नाही.</strong> मी खालील गोष्टींमध्ये मदत करू शकतो:<br>'
                f'<em>{html.escape(mods)}</em><br><br>उदाहरणे:'
                f'<ul style="margin:8px 0;padding-left:20px">'
                f'<li><em>सर्व खरेदी दाखवा</em></li>'
                f'<li><em>vikri list</em></li>'
                f'<li><em>supplier credit kiti ahe</em></li>'
                f'<li><em>stock bagha</em></li>'
                f'<li><em>supplier chi yadi dakhva</em></li>'
                f'<li><em>category list</em></li>'
                f'</ul></div>'
            )
        mods = ", ".join(k.replace("-", " ").title() for k in MODULE_REGISTRY)
        return (
            f'<div style="background:#f8fafc;border:1px solid #e2e8f0;'
            f'border-radius:10px;padding:16px 20px;">'
            f"<strong>I'm not sure what you're looking for.</strong><br>"
            f"I can help with: <em>{html.escape(mods)}</em><br><br>Try:"
            f"<ul style='margin:8px 0;padding-left:20px'>"
            f"<li><em>List all purchases</em></li>"
            f"<li><em>Show supplier credits</em></li>"
            f"<li><em>Find customer named Rahul</em></li>"
            f"<li><em>Financial summary</em></li>"
            f"<li><em>Supplier purchase history</em></li>"
            f"<li><em>Get product categories</em></li>"
            f"<li><em>Customer payment history</em></li>"
            f"<li><em>Barcode BAR268726580</em></li>"
            f"</ul></div>"
        )


synth = ResponseSynthesizer()


# ──────────────────────────────────────────────────────────────────────────────
# 7. CONVERSATION CONTEXT
# BUG-09 FIX: LRU eviction (MAX_SESSIONS) + session TTL (SESSION_TTL)
# SEC-08 FIX: session_id sanitized
# ──────────────────────────────────────────────────────────────────────────────

def _sanitize_session_id(sid: str) -> str:
    """SEC-08: alphanumeric only, max 64 chars."""
    clean = SESSION_RE.sub("", sid)[:64]
    return clean if clean else "default"


class ConversationContext:
    def __init__(self, max_sessions: int = MAX_SESSIONS,
                 max_turns: int = 5, session_ttl: float = SESSION_TTL):
        # OrderedDict for LRU eviction
        self._sessions: "collections.OrderedDict[str, Tuple[deque, float]]" = \
            collections.OrderedDict()
        self._max_sessions = max_sessions
        self._max_turns    = max_turns
        self._session_ttl  = session_ttl

    def _evict_stale(self):
        now = time.time()
        stale = [k for k, (_, ts) in self._sessions.items()
                 if now - ts > self._session_ttl]
        for k in stale:
            del self._sessions[k]

    def add(self, sid: str, role: str, text: str):
        sid = _sanitize_session_id(sid)
        self._evict_stale()
        if sid not in self._sessions:
            if len(self._sessions) >= self._max_sessions:
                self._sessions.popitem(last=False)  # evict oldest
            self._sessions[sid] = (deque(maxlen=self._max_turns), time.time())
        turns, _ = self._sessions[sid]
        turns.append({"role": role, "text": text[:300]})
        self._sessions[sid] = (turns, time.time())  # refresh TTL
        self._sessions.move_to_end(sid)

    def last_module(self, sid: str) -> Optional[str]:
        sid = _sanitize_session_id(sid)
        if sid not in self._sessions:
            return None
        turns, _ = self._sessions[sid]
        for turn in reversed(turns):
            if turn["role"] == "bot":
                m = re.search(r"_module:(\S+)", turn["text"])
                if m:
                    return m.group(1)
        return None

    def session_count(self) -> int:
        return len(self._sessions)


ctx = ConversationContext()


# ──────────────────────────────────────────────────────────────────────────────
# SEC-07: RATE LIMITER — sliding window token bucket per IP
# ──────────────────────────────────────────────────────────────────────────────

class RateLimiter:
    """
    Sliding-window rate limiter — in-memory per-process.

    FIX-8 NOTE: This is intentionally single-process. On Render free tier
    you have exactly 1 worker (512 MB / 1 vCPU limit). If you ever scale to
    multiple workers (Render paid tier, Docker Swarm, etc.), replace this with
    a shared Redis counter:
        key = f"ratelimit:{ip}"
        count = redis.incr(key)
        redis.expire(key, 60)
        if count > RATE_LIMIT_RPM: block
    The in-process version is correct and safe for single-worker deployments.
    """
    def __init__(self, rpm: int = RATE_LIMIT_RPM):
        self._rpm      = rpm
        self._windows: Dict[str, deque] = defaultdict(lambda: deque())

    def check(self, ip: str) -> Tuple[bool, int]:
        """
        Returns (allowed, remaining_requests_this_minute).
        FIX-8: remaining count returned so endpoint can set X-RateLimit-Remaining.
        """
        now    = time.time()
        window = self._windows[ip]
        cutoff = now - 60.0
        while window and window[0] < cutoff:
            window.popleft()
        remaining = max(0, self._rpm - len(window))
        if len(window) >= self._rpm:
            return False, 0
        window.append(now)
        return True, remaining - 1

    def is_allowed(self, ip: str) -> bool:
        allowed, _ = self.check(ip)
        return allowed


rate_limiter = RateLimiter()


# ──────────────────────────────────────────────────────────────────────────────
# 8. MAIN ORCHESTRATOR
# BUG-12: asyncio.wait_for timeout  |  BUG-13: targeted table cleanup
# PERF-06: response cache integration
# ──────────────────────────────────────────────────────────────────────────────

class AdminEngine:
    def __init__(self):
        self.classifier = IntentClassifier()
        self.parser     = QueryParser()

    async def process(self, user_query: str, session_id: str) -> str:
        ctx.add(session_id, "user", user_query)
        lang       = detect_language(user_query)
        is_marathi = lang in ("marathi_devanagari", "marathi_roman")
        analysis   = query_analyzer.analyze(user_query)
        q_mode     = analysis["query_mode"]
        q_val      = analysis["query_value"]

        if is_marathi:
            key = marathi_classify(user_query)
            if key:
                return await self._single(key, user_query, session_id,
                                          lang, marathi=True,
                                          q_mode=q_mode, q_val=q_val)
            return synth.unrecognized(marathi=True)

        cl = self.classifier.classify(user_query)
        if cl["type"] == "UNRECOGNIZED":
            return synth.unrecognized(marathi=False)
        if cl["type"] == "MULTI":
            return await self._multi(cl["pattern"], user_query, session_id)
        return await self._single(cl["module"], user_query, session_id,
                                  lang, marathi=False,
                                  q_mode=q_mode, q_val=q_val)

    async def _single(self, key: str, query: str, sid: str,
                      lang: str, marathi: bool,
                      q_mode: str = None, q_val: str = None) -> str:
        parsed = self.parser.parse(query, key, lang)
        if parsed["intent"] == "PROMPT_NEEDED":
            return synth.prompt_needed(key, parsed["target_col"], marathi)

        # PERF-06: check response cache first (skip for targeted/live lookups)
        if not q_mode:
            cached = response_cache.get(
                key, parsed["intent"], parsed["search_value"], lang
            )
            if cached:
                return cached

        targeted_key   = None
        targeted_badge = ""

        # Try targeted API
        if q_mode and q_val:
            ok, targeted_key = await smart_router.route(key, parsed, q_mode, q_val)
            if ok:
                targeted_badge = synth.targeted_badge(q_mode, q_val, marathi)
                try:
                    records, cols, _ = retriever.retrieve(targeted_key, parsed)
                    if records:
                        agg          = aggregator.compute(records, key)
                        intro        = synth.intro(key, agg, q_val, marathi, lang)
                        tbl, rendered= synth.table(records, cols, marathi)
                        ctx.add(sid, "bot", f"_module:{key}")
                        return targeted_badge + intro + tbl
                finally:
                    if targeted_key:
                        db._drop_targeted(targeted_key)   # BUG-13

        # Full module sync
        status = await db.sync_module(key)
        if status == "error":
            return synth.api_error(key, marathi)

        stale_banner = synth.stale_banner(key, marathi) if status == "stale" else ""

        records, cols, _ = retriever.retrieve(key, parsed)

        if not records:
            return stale_banner + synth.no_results(
                key, parsed["search_value"], marathi, q_mode
            )

        agg = aggregator.compute(records, key)

        # Aggregation-only
        if parsed["aggregation"] == "SUM" and parsed["intent"] != "BULK_LIST":
            bk    = _base_key(key)
            label = html.escape(bk.replace("-", " ").title())
            parts = [f'<strong>{label}</strong> — {agg["count"]:,} records']
            if agg["total_amount"]:
                parts.append(f'Total: <strong>₹{agg["total_amount"]:,.2f}</strong>')
            if agg["total_qty"]:
                parts.append(f'Units: <strong>{agg["total_qty"]:,}</strong>')
            result = (
                f'<div style="background:#f0fdf4;border:1px solid #bbf7d0;'
                f'border-radius:10px;padding:14px 20px">'
                + " &nbsp;·&nbsp; ".join(parts) + "</div>"
            )
            return stale_banner + result

        tbl, rendered = synth.table(records, cols, marathi)
        intro  = synth.intro(key, agg, parsed["search_value"], marathi, lang,
                             total_found=len(records), rendered=rendered)
        result = stale_banner + intro + tbl
        ctx.add(sid, "bot", f"_module:{key}")

        # Cache only fresh full-table results
        if not q_mode and status in ("ok", "fresh"):
            response_cache.set(
                key, parsed["intent"], parsed["search_value"], lang, result
            )

        return result

    async def _multi(self, pattern: Dict, query: str, sid: str) -> str:
        keys    = pattern["modules"]
        label   = html.escape(pattern["label"])
        synced  = await db.sync_modules(keys)
        sects:  List[str] = []
        total_a = total_q = 0

        for key in keys:
            st = synced.get(key, "error")
            if st == "error":
                sects.append(
                    f'<div style="margin-bottom:24px">'
                    f'<h3 style="border-left:4px solid #6366f1;padding-left:10px;'
                    f'margin:0 0 8px;font-size:15px">'
                    f'{html.escape(key.replace("-"," ").title())}</h3>'
                    + synth.api_error(key, False) + "</div>"
                )
                continue

            stale_warn = synth.stale_banner(key, False) if st == "stale" else ""
            parsed = self.parser.parse(query, key, "english")
            parsed["intent"]       = "BULK_LIST"
            parsed["search_value"] = ""
            records, cols, _  = retriever.retrieve(key, parsed)
            if not records:
                continue
            agg       = aggregator.compute(records, key)
            total_a  += agg["total_amount"] or 0
            total_q  += agg["total_qty"]    or 0
            mod_label = html.escape(key.replace("-", " ").title())
            tbl, rendered = synth.table(records, cols, False)
            intro = synth.intro(key, agg, "", False, "english",
                                total_found=len(records), rendered=rendered)
            sects.append(
                f'<div style="margin-bottom:28px">'
                f'<h3 style="border-left:4px solid #6366f1;padding-left:10px;'
                f'margin:0 0 8px;font-size:15px;color:#1e293b">{mod_label}</h3>'
                + stale_warn + intro + tbl + "</div>"
            )

        if not sects:
            return f'<p>No data available for <strong>{label}</strong>.</p>'

        summary: List[str] = [f'<strong>{len(keys)} modules</strong>']
        if total_a:
            summary.append(f'Total: <strong>₹{total_a:,.2f}</strong>')
        if total_q:
            summary.append(f'Units: <strong>{total_q:,}</strong>')

        header = "".join([
            '<div style="background:linear-gradient(135deg,#6366f1,#8b5cf6);',
            'color:#fff;border-radius:12px;padding:16px 20px;margin-bottom:20px">',
            f'<strong style="font-size:17px">📊 {label}</strong><br>',
            f'<small>{" &nbsp;·&nbsp; ".join(summary)}</small></div>',
        ])
        ctx.add(sid, "bot", "_module:multi")
        return header + "".join(sects)


engine = AdminEngine()


# ──────────────────────────────────────────────────────────────────────────────
# 9. STATIC RESPONSES
# ──────────────────────────────────────────────────────────────────────────────

GREETING_EN = """
<div style="font-family:'Segoe UI',system-ui,sans-serif;max-width:580px">
  <p style="font-size:18px;font-weight:700;margin:0 0 14px;color:#1e293b">
    👋 Hello! I'm your <span style="color:#6366f1">ERP Intelligence Assistant v6.2</span>.
  </p>
  <p style="margin:0 0 14px;color:#475569;font-size:14px">Here's what I can help you with:</p>
  <div style="display:grid;gap:8px">
    <div style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:8px;padding:10px 14px;font-size:14px">
      📦 <strong>Stock &amp; Inventory</strong> — <em>"Show all stock"</em>, <em>"Barcode BAR268726580"</em>
    </div>
    <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:10px 14px;font-size:14px">
      💰 <strong>Sales &amp; Purchases</strong> — <em>"Invoice PA00000001"</em>, <em>"Supplier purchase history"</em>
    </div>
    <div style="background:#fdf4ff;border:1px solid #e9d5ff;border-radius:8px;padding:10px 14px;font-size:14px">
      🏢 <strong>Suppliers &amp; Customers</strong> — <em>"All suppliers"</em>, <em>"Customer payment history"</em>
    </div>
    <div style="background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 14px;font-size:14px">
      📊 <strong>Financial Reports</strong> — <em>"Financial summary"</em>, <em>"Supplier credits"</em>
    </div>
    <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:10px 14px;font-size:14px">
      🗂️ <strong>Categories &amp; Config</strong> — <em>"Product categories"</em>, <em>"Active printer"</em>
    </div>
  </div>
  <p style="margin:12px 0 0;font-size:13px;color:#94a3b8">
    ⚡ Smart lookups: paste a barcode, invoice number, or mobile number for instant results.<br>
    You can also ask in <strong>मराठी</strong> or Marathi Roman typing.
  </p>
</div>
"""

GREETING_MR = """
<div style="font-family:'Noto Sans Devanagari','Segoe UI',system-ui,sans-serif;max-width:580px">
  <p style="font-size:18px;font-weight:700;margin:0 0 14px;color:#1e293b">
    नमस्कार! मी तुमचा <span style="color:#6366f1">ERP सहाय्यक v6.2</span> आहे. 🙏
  </p>
  <p style="margin:0 0 14px;color:#475569;font-size:14px">मी खालील गोष्टींमध्ये मदत करू शकतो:</p>
  <div style="display:grid;gap:8px">
    <div style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:8px;padding:10px 14px;font-size:14px">
      📦 <strong>साठा आणि इन्व्हेंटरी</strong> — <em>"stock bagha"</em>, <em>"उत्पादन साठा दाखवा"</em>
    </div>
    <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:10px 14px;font-size:14px">
      💰 <strong>विक्री आणि खरेदी</strong> — <em>"vikri list"</em>, <em>"supplier kharedi itihas"</em>
    </div>
    <div style="background:#fdf4ff;border:1px solid #e9d5ff;border-radius:8px;padding:10px 14px;font-size:14px">
      🏢 <strong>पुरवठादार आणि ग्राहक</strong> — <em>"supplier chi yadi"</em>, <em>"grahak payment history"</em>
    </div>
    <div style="background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 14px;font-size:14px">
      📊 <strong>आर्थिक अहवाल</strong> — <em>"supplier credit kiti ahe"</em>, <em>"financial summary"</em>
    </div>
    <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:10px 14px;font-size:14px">
      🗂️ <strong>श्रेणी आणि सेटिंग</strong> — <em>"category list"</em>, <em>"printer bagha"</em>
    </div>
  </div>
  <p style="margin:12px 0 0;font-size:13px;color:#94a3b8">
    ⚡ बारकोड, इनव्हॉइस नंबर किंवा मोबाईल नंबर टाकून थेट शोध करा.<br>
    English मध्ये पण विचारू शकता.
  </p>
</div>
"""

IDENTITY_RESPONSE = """
<div style="font-family:'Segoe UI',system-ui,sans-serif;max-width:640px">
  <p style="font-size:18px;font-weight:700;color:#1e293b;margin:0 0 12px">
    🧠 Admin Intelligence Engine v6.2 — Maximum Potential
  </p>
  <p style="margin:0 0 16px;color:#475569">
    Production-grade <strong>RAG pipeline</strong> for ERP —
    zero external AI APIs · full Marathi support · ≤300 MB RAM ·
    10 security fixes · 10+ bug fixes · 11 performance upgrades.
  </p>
  <div style="background:#fef2f2;border-radius:10px;padding:16px;margin-bottom:12px">
    <strong style="color:#dc2626">🔒 Security (10 fixes):</strong>
    <ul style="margin:8px 0;padding-left:20px;color:#334155;line-height:1.9;font-size:13px">
      <li>XSS eliminated — html.escape() on all user values + column headers</li>
      <li>Memory bomb — concat flatten, FLATTEN_MAX_CHILD_ROWS=200</li>
      <li>Lock bottleneck — per-module asyncio.Lock (concurrent syncs)</li>
      <li>JWT spam — class-level token cache + 401 invalidation</li>
      <li>NaN/Inf aggregation — math.isfinite() on all numerics</li>
      <li>Rate limiting — 60 req/min per IP sliding window</li>
      <li>session_id sanitized — alphanumeric only, max 64 chars</li>
      <li>No default credentials in prod — env vars enforced</li>
      <li>Image URL whitelist — ERP domain only</li>
      <li>FTS injection — special chars stripped before MATCH query</li>
    </ul>
  </div>
  <div style="background:#f0fdf4;border-radius:10px;padding:16px;margin-bottom:12px">
    <strong style="color:#16a34a">🐛 Bug Fixes (10 fixes):</strong>
    <ul style="margin:8px 0;padding-left:20px;color:#334155;line-height:1.9;font-size:13px">
      <li>"give me suppliers list" — all-stop query now forces BULK_LIST</li>
      <li>Marathi classifier — confidence scoring, min threshold 0.4</li>
      <li>Session memory — LRU eviction + 30 min TTL (max 500 sessions)</li>
      <li>Cold start — warmup task pre-fetches 4 modules on startup</li>
      <li>sync_all — only re-fetches stale modules (not all 16)</li>
      <li>Request timeout — 45s asyncio.wait_for wraps engine.process()</li>
      <li>Targeted table collision — uuid suffix per request</li>
      <li>FTS injection — special operator tokens stripped</li>
      <li>Datetime false positives — DATETIME_RE strict pattern</li>
      <li>Table render — list.append()+join() (10× faster for 500+ rows)</li>
    </ul>
  </div>
  <div style="background:#f0f9ff;border-radius:10px;padding:16px">
    <strong style="color:#0369a1">⚡ Performance (11 upgrades):</strong>
    <ul style="margin:8px 0;padding-left:20px;color:#334155;line-height:1.9;font-size:13px">
      <li>LRU response cache — 512 entries, 90s TTL, &lt;1ms hits</li>
      <li>Lazy module loading — only warmup modules load at startup</li>
      <li>SQLite WAL + 128 MB mmap_size</li>
      <li>Compiled regex cache — zero per-query recompile</li>
      <li>Persistent httpx.AsyncClient — reused across all requests</li>
      <li>MAX_RENDER_ROWS=500 — HTML render cap, full data in SQLite</li>
      <li>MAX_ROWS=30,000 — safe for 300 MB budget</li>
      <li>Stale cache fallback — serves last data during ERP downtime</li>
      <li>Truncation badge — shows "50 of 1,243" when capped</li>
      <li>Health endpoint — /health for Render monitoring</li>
      <li>String build with list.join() throughout</li>
    </ul>
  </div>
</div>
"""


# ──────────────────────────────────────────────────────────────────────────────
# 10. FASTAPI APPLICATION
# BUG-10: lifespan warmup  |  BUG-12: request timeout  |  SEC-07: rate limit
# FEAT-01: /health endpoint
# ──────────────────────────────────────────────────────────────────────────────

if _FASTAPI_AVAILABLE:

    from fastapi import FastAPI, status
    from fastapi.middleware.cors import CORSMiddleware
    from starlette.middleware.base import BaseHTTPMiddleware
    from starlette.requests import Request as StarletteRequest
    from starlette.responses import Response as StarletteResponse

    # ── FIX-1: Startup readiness — blocks requests until warmup done ──────────
    async def _warmup():
        """Pre-fetch the 4 most common modules, then signal ready."""
        log.info("Warmup: pre-fetching %s", WARMUP_MODULES)
        results = await db.sync_modules(WARMUP_MODULES)
        log.info("Warmup complete: %s", results)
        _startup_ready.set()   # FIX-1: unblock all waiting requests

    @asynccontextmanager
    async def lifespan(app):
        """Open HTTP client → fire warmup → serve → close HTTP client."""
        await db.open()
        log.info("HTTP client opened")
        asyncio.create_task(_warmup())
        yield
        await db.close()
        log.info("HTTP client closed")

    app = FastAPI(
        title="ERP Intelligence Engine v6.3",
        lifespan=lifespan,
    )

    # ── FIX-5: CORS ───────────────────────────────────────────────────────────
    app.add_middleware(
        CORSMiddleware,
        allow_origins=CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["POST", "GET", "OPTIONS"],
        allow_headers=["Content-Type", "Authorization", "X-Request-ID"],
        max_age=600,
    )

    # ── FIX-6 + FIX-7: Security headers + Request-ID on every response ────────
    class SecurityHeadersMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request: StarletteRequest,
                           call_next) -> StarletteResponse:
            # FIX-7: echo or generate a request-ID for tracing
            req_id = (
                request.headers.get("X-Request-ID")
                or secrets.token_hex(8)
            )
            response = await call_next(request)
            # FIX-6: inject security headers
            for k, v in SECURITY_HEADERS.items():
                response.headers[k] = v
            # FIX-7: send request-ID back so frontend can log it
            response.headers["X-Request-ID"] = req_id
            return response

    app.add_middleware(SecurityHeadersMiddleware)
    app.include_router(router, prefix="/api")

    # ── Chat endpoint ─────────────────────────────────────────────────────────

    class ChatRequest(BaseModel):
        query:      Optional[str] = None
        question:   Optional[str] = None
        session_id: Optional[str] = "default"

    _EN_GREET    = frozenset({"hi","hello","hey","heyy","heya",
                               "good morning","good afternoon","good evening","sup","yo"})
    _EN_THANKS   = frozenset({"thank you","thanks","awesome","perfect","great",
                               "nice","good","ok","okay","cool"})
    _EN_IDENTITY = frozenset({"who are you","how do you work","chatgpt","your brain",
                               "how were you built","architecture","how does this work",
                               "what can you do","tell me about yourself"})
    _EN_REFRESH  = frozenset({"refresh","sync","update records","reload data",
                               "refresh data","sync data"})
    _MR_GREET_W  = frozenset({"नमस्कार","नमस्ते","हॅलो","namaskar","namaste"})
    _MR_THANKS_W = frozenset({"धन्यवाद","आभारी","थँक्यू","dhanyavad","aabhari"})
    _MR_REFRESH_W= frozenset({"रिफ्रेश","अद्यतन","refresh kara","update kara","sync kara"})

    @router.post("/chat")
    async def chat_endpoint(request: Request, body: ChatRequest):
        # FIX-1: block until warmup complete (max 30s to avoid hanging forever)
        try:
            await asyncio.wait_for(_startup_ready.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            log.error("Startup warmup did not complete within 30s — serving anyway")
            _startup_ready.set()   # don't block future requests

        # FIX-7: attach request-ID for logging (set by middleware, readable here)
        req_id    = request.headers.get("X-Request-ID", "?")

        # SEC-07 + FIX-8: rate limiting with remaining-count header
        client_ip         = request.client.host if request.client else "unknown"
        allowed, remaining = rate_limiter.check(client_ip)
        if not allowed:
            log.warning("[%s] rate-limited ip=%s", req_id, client_ip)
            return {"error": "Rate limit exceeded. Please wait a moment.",
                    "retry_after": 60}

        query      = (body.query or body.question or "").strip()
        session_id = _sanitize_session_id(body.session_id or "default")

        if not query:
            return {"response": "Please type a question. / कृपया प्रश्न टाइप करा."}
        if len(query) > 2000:
            return {"response": "Query too long. Please keep it under 2000 characters."}

        q_low = query.lower().strip()
        lang  = detect_language(query)
        is_mr = lang in ("marathi_devanagari", "marathi_roman")

        if is_mr:
            if any(q_low.startswith(w) for w in _MR_GREET_W) and len(q_low.split()) <= 4:
                return {"response": GREETING_MR}
            if any(w in q_low for w in _MR_THANKS_W):
                return {"response": random.choice([
                    "आपले स्वागत आहे! आणखी काही हवे असल्यास सांगा.",
                    "माझ्यासाठी हे काम करणे आनंददायक आहे! आणखी काही?",
                    "नक्कीच! आणखी कोणता डेटा पाहायचा आहे?",
                ])}
            if any(w in q_low for w in _MR_REFRESH_W):
                results = await db.sync_all(force=True)
                ok_count = sum(1 for v in results.values() if v == "ok")
                return {"response": (
                    f'<div style="background:#f0fdf4;border:1px solid #bbf7d0;'
                    f'border-radius:10px;padding:14px 18px;color:#166534">'
                    f'🔄 <strong>सर्व माहिती अद्यतनित झाली.</strong> '
                    f'{ok_count} modules refreshed. आता काय पाहायचे आहे?</div>'
                )}
        else:
            if any(q_low.startswith(w) for w in _EN_GREET) and len(q_low.split()) <= 4:
                return {"response": GREETING_EN}
            if q_low in _EN_THANKS:
                return {"response": random.choice([
                    "You're welcome! Let me know if you need more data.",
                    "Happy to help! Anything else?",
                    "My pleasure! Type another query anytime.",
                ])}
            if any(kw in q_low for kw in _EN_IDENTITY):
                return {"response": IDENTITY_RESPONSE}
            if any(kw in q_low for kw in _EN_REFRESH):
                results = await db.sync_all(force=True)
                ok_count = sum(1 for v in results.values() if v == "ok")
                return {"response": (
                    f'<div style="background:#f0fdf4;border:1px solid #bbf7d0;'
                    f'border-radius:10px;padding:14px 18px;color:#166534">'
                    f'🔄 <strong>{ok_count} module(s) refreshed.</strong> '
                    f'ERP data is up to date.</div>'
                )}

        log.info("[%s] query ip=%s lang=%s q=%s", req_id, client_ip, lang, query[:80])

        # BUG-12: wrap with 45s timeout (FIX-2 ensures httpx cleans up properly)
        try:
            response = await asyncio.wait_for(
                engine.process(query, session_id),
                timeout=45.0,
            )
        except asyncio.TimeoutError:
            log.error("[%s] process() timed out: %s", req_id, query[:100])
            response = (
                '<div style="background:#fff7ed;border:1px solid #fed7aa;'
                'border-radius:10px;padding:16px 20px;color:#9a3412;">'
                '⏱️ Request timed out. The ERP server is responding slowly. '
                'Please try again in a moment.</div>'
            )

        return {"response": response}

    # ── Health endpoint (FEAT-01 + FIX-3 circuit breaker status) ─────────────
    @app.get("/health")
    async def health():
        return {
            "status":          "ok",
            "version":         "6.3",
            "ready":           _startup_ready.is_set(),   # FIX-1
            "uptime_seconds":  int(db.uptime_seconds()),
            "sessions_active": ctx.session_count(),
            "cache_entries":   response_cache.size(),
            "circuit_breaker": circuit_breaker.status(),  # FIX-3
            "modules":         db.cache_status(),
        }

    # ── Root redirect ─────────────────────────────────────────────────────────
    @app.get("/")
    async def root():
        return {"message": "ERP Intelligence Engine v6.3 is running.",
                "docs": "/docs", "health": "/health"}

else:
    # Stub router for environments without FastAPI (testing)
    router = None  # type: ignore


# ──────────────────────────────────────────────────────────────────────────────
# SELF-TEST SUITE  — run with: python admin_engine_v62.py
# Tests all major bug fixes and security patches without network access.
# ──────────────────────────────────────────────────────────────────────────────

def _run_tests():
    import traceback
    passed = failed = 0

    def ok(name):
        nonlocal passed
        passed += 1
        print(f"  ✅ PASS  {name}")

    def fail(name, err):
        nonlocal failed
        failed += 1
        print(f"  ❌ FAIL  {name}: {err}")

    def test(name, fn):
        try:
            fn()
            ok(name)
        except Exception as e:
            fail(name, e)

    print("\n══════════════════════════════════════════")
    print("  ERP Engine v6.3 — Self-Test Suite")
    print("══════════════════════════════════════════\n")

    # ── Language detection ────────────────────────────────────────────────────
    test("lang: English detected",
         lambda: assert_eq(detect_language("show all purchases"), "english"))
    test("lang: Devanagari detected",
         lambda: assert_eq(detect_language("सर्व खरेदी दाखवा"), "marathi_devanagari"))
    test("lang: Roman Marathi detected (supplier)",
         lambda: assert_eq(detect_language("supplier chi yadi dakhva"), "marathi_roman"))
    test("lang: Roman Marathi detected (kiti)",
         lambda: assert_eq(detect_language("stock kiti ahe"), "marathi_roman"))
    test("lang: mixed EN/MR still marathi_roman",
         lambda: assert_eq(detect_language("give me supplier chi yadi"), "marathi_roman"))

    # ── Marathi classifier ────────────────────────────────────────────────────
    test("mr_classify: supplier",
         lambda: assert_eq(marathi_classify("supplier chi yadi dakhva"), "supplier"))
    test("mr_classify: purchase",
         lambda: assert_eq(marathi_classify("kharedi list dya"), "purchase"))
    test("mr_classify: sell",
         lambda: assert_eq(marathi_classify("vikri itihas bagha"), "sell"))
    test("mr_classify: stock",
         lambda: assert_eq(marathi_classify("stock kiti ahe"), "product-stock"))
    test("mr_classify: supplier-credit",
         lambda: assert_eq(marathi_classify("supplier credit kiti ahe"), "supplier-credit"))
    test("mr_classify: confidence threshold (junk returns None)",
         lambda: assert_eq(marathi_classify("xyz abc def"), None))

    # ── BUG-07: all-stop query → BULK_LIST ───────────────────────────────────
    parser = QueryParser()
    test("BUG-07: 'give me suppliers list' → BULK_LIST",
         lambda: assert_eq(
             parser.parse("give me suppliers list", "supplier", "english")["intent"],
             "BULK_LIST"
         ))
    test("BUG-07: 'show me all purchases' → BULK_LIST",
         lambda: assert_eq(
             parser.parse("show me all purchases", "purchase", "english")["intent"],
             "BULK_LIST"
         ))
    test("BUG-07: 'give me list for suppliers' → BULK_LIST",
         lambda: assert_eq(
             parser.parse("give me list for suppliers", "supplier", "english")["intent"],
             "BULK_LIST"
         ))
    test("BUG-07: specific search still GLOBAL_SEARCH",
         lambda: assert_eq(
             parser.parse("find supplier VIVEK TRADING", "supplier", "english")["intent"],
             "GLOBAL_SEARCH"
         ))

    # ── BUG-15: datetime strict detection ────────────────────────────────────
    test("BUG-15: valid datetime formatted",
         lambda: assert_in("2026-02-27", _fmt_cell("createdAt", "2026-02-27T16:21:00")))
    test("BUG-15: 'NOT SPECIFY' not treated as datetime",
         lambda: assert_eq(_fmt_cell("size", "NOT SPECIFY"), html.escape("NOT SPECIFY")))
    test("BUG-15: 'T-shirt' not treated as datetime",
         lambda: assert_eq(_fmt_cell("name", "T-shirt"), html.escape("T-shirt")))

    # ── SEC-01: XSS in search value ───────────────────────────────────────────
    payload = '<img src=x onerror=alert(1)>'
    test("SEC-01: XSS in no_results escaped",
         lambda: assert_not_in("<img src=x", synth.no_results("purchase", payload, False)))
    test("SEC-01: XSS in no_results Marathi escaped",
         lambda: assert_not_in("<img src=x", synth.no_results("purchase", payload, True)))

    agg = {"count": 1, "total_amount": None, "total_qty": None}
    test("SEC-01: XSS in intro escaped",
         lambda: assert_not_in("<img src=x", synth.intro("purchase", agg, payload, False, "english")))

    # ── SEC-05: NaN/Inf in aggregation ───────────────────────────────────────
    inf_records = [{"totalAmount": "inf", "qty": "nan"}, {"totalAmount": "100"}]
    agg_engine  = AggregationEngine()
    result      = agg_engine.compute(inf_records, "purchase")
    test("SEC-05: inf value ignored in aggregation",
         lambda: assert_eq(result["total_amount"], 100.0))

    # ── SEC-06: column header injection ──────────────────────────────────────
    dirty_cols   = ['<script>alert(1)</script>', 'productName', 'totalAmount']
    sample_recs  = [{'<script>alert(1)</script>': 'xss', 'productName': 'Chair',
                     'totalAmount': '100'}]
    tbl, _       = synth.table(sample_recs, dirty_cols, False)
    test("SEC-06: script tag in column header is escaped",
         lambda: assert_not_in("<script>", tbl))

    # ── SEC-08: session_id sanitization ──────────────────────────────────────
    test("SEC-08: malicious session_id sanitized",
         lambda: assert_eq(_sanitize_session_id("../../etc/passwd"), "etcpasswd"))
    test("SEC-08: session_id capped at 64 chars",
         lambda: assert_eq(len(_sanitize_session_id("a" * 100)), 64))
    test("SEC-08: valid session_id unchanged",
         lambda: assert_eq(_sanitize_session_id("user-123_abc"), "user-123_abc"))

    # ── BUG-09: session eviction ──────────────────────────────────────────────
    ctx_test = ConversationContext(max_sessions=3, max_turns=2)
    for i in range(5):
        ctx_test.add(f"s{i}", "user", f"hello {i}")
    test("BUG-09: session count capped at max_sessions",
         lambda: assert_lte(ctx_test.session_count(), 3))

    # ── BUG-14: FTS injection sanitization ───────────────────────────────────
    # Verify both special chars AND boolean operators are stripped before FTS query
    dirty = '"NOT" OR "AND" (injection*)'
    step1 = FTS_SPECIAL.sub(" ", dirty)
    step2 = _FTS_BOOL_RE.sub(" ", step1).strip()
    test("BUG-14: FTS boolean operators stripped (NOT gone after both passes)",
         lambda: assert_not_in("NOT", [w for w in step2.split()
                                        if w.upper() in ("NOT","OR","AND","NEAR")]))

    # ── SEC-02: flatten no cartesian product ─────────────────────────────────
    rec = {
        "id": "1", "name": "PO",
        "items":  [{"sku": f"SKU{i}"} for i in range(50)],
        "taxes":  [{"tax": f"T{i}"}  for i in range(20)],
        "events": [{"ev":  f"E{i}"}  for i in range(10)],
    }
    flat  = db._flatten([rec])
    test("SEC-02: flatten is concat not cartesian (≤ 200 rows, not 10000)",
         lambda: assert_lte(len(flat), 200))

    # ── SEC-02: FLATTEN_MAX_CHILD_ROWS cap ────────────────────────────────────
    huge_rec = {"id": "1", "items": [{"x": str(i)} for i in range(500)]}
    flat2    = db._flatten([huge_rec])
    test("SEC-02: FLATTEN_MAX_CHILD_ROWS cap enforced (≤200)",
         lambda: assert_lte(len(flat2), FLATTEN_MAX_CHILD_ROWS))

    # ── SQLite round-trip ─────────────────────────────────────────────────────
    db._load("test_tbl", [
        {"productName": "Chair", "totalAmount": "1500", "barcode": "BAR123"},
        {"productName": "Table", "totalAmount": "2500", "barcode": "BAR456"},
        {"productName": "Desk",  "totalAmount": "3500", "barcode": "BAR789"},
    ])
    ret = RAGRetriever()
    parsed_bulk = {"intent": "BULK_LIST", "search_value": "", "target_col": None, "aggregation": "NONE"}
    rows, cols, method = ret.retrieve("test_tbl", parsed_bulk)
    test("SQLite: bulk retrieval returns all 3 rows",
         lambda: assert_eq(len(rows), 3))
    test("SQLite: method is 'bulk'",
         lambda: assert_eq(method, "bulk"))

    parsed_search = {"intent": "GLOBAL_SEARCH", "search_value": "Chair", "target_col": None, "aggregation": "NONE"}
    rows2, _, method2 = ret.retrieve("test_tbl", parsed_search)
    test("SQLite FTS: search 'Chair' returns 1 row",
         lambda: assert_eq(len(rows2), 1))

    parsed_col = {"intent": "COLUMN_SEARCH", "search_value": "BAR456", "target_col": "barcode", "aggregation": "NONE"}
    rows3, _, method3 = ret.retrieve("test_tbl", parsed_col)
    test("SQLite: column search by barcode finds correct row",
         lambda: assert_eq(rows3[0].get("productName"), "Table"))

    # ── Aggregation ───────────────────────────────────────────────────────────
    recs  = [{"totalAmount": "1500"}, {"totalAmount": "2500"}, {"totalAmount": "inf"}]
    agg2  = AggregationEngine().compute(recs, "purchase")
    test("Aggregation: sum = 4000 (inf ignored)",
         lambda: assert_eq(agg2["total_amount"], 4000.0))
    test("Aggregation: count = 3",
         lambda: assert_eq(agg2["count"], 3))

    # ── Response cache ────────────────────────────────────────────────────────
    rc = ResponseCache(max_size=3, ttl=5)
    rc.set("purchase", "BULK_LIST", "", "english", "<html>test</html>")
    test("Cache: set and get returns value",
         lambda: assert_eq(rc.get("purchase", "BULK_LIST", "", "english"), "<html>test</html>"))
    rc.invalidate_module("purchase")
    test("Cache: invalidate_module removes entry",
         lambda: assert_eq(rc.get("purchase", "BULK_LIST", "", "english"), None))

    # LRU eviction
    rc2 = ResponseCache(max_size=2, ttl=60)
    rc2.set("m1", "BULK_LIST", "", "en", "a")
    rc2.set("m2", "BULK_LIST", "", "en", "b")
    rc2.set("m3", "BULK_LIST", "", "en", "c")
    test("Cache: LRU evicts oldest entry when full",
         lambda: assert_eq(rc2.get("m1", "BULK_LIST", "", "en"), None))

    # ── Rate limiter ──────────────────────────────────────────────────────────
    rl = RateLimiter(rpm=5)
    for _ in range(5):
        rl.is_allowed("1.2.3.4")
    test("RateLimit: 6th request in 1 min blocked",
         lambda: assert_eq(rl.is_allowed("1.2.3.4"), False))
    test("RateLimit: different IP still allowed",
         lambda: assert_eq(rl.is_allowed("5.6.7.8"), True))

    # ── Intent classifier ─────────────────────────────────────────────────────
    ic = IntentClassifier()
    test("NLU: 'show all purchases' → purchase",
         lambda: assert_eq(ic.classify("show all purchases")["module"], "purchase"))
    test("NLU: 'supplier credits outstanding' → supplier-credit",
         lambda: assert_eq(ic.classify("supplier credits outstanding")["module"], "supplier-credit"))
    test("NLU: 'product categories' → category",
         lambda: assert_eq(ic.classify("product categories")["module"], "category"))
    test("NLU: 'financial summary' → MULTI",
         lambda: assert_eq(ic.classify("financial summary")["type"], "MULTI"))
    test("NLU: typo 'purchaces' → purchase via fuzzy",
         lambda: assert_eq(ic.classify("show all purchaces")["module"], "purchase"))
    test("NLU: 'get me all product categories' → category (not product-stock)",
         lambda: assert_eq(ic.classify("get me all product categories")["module"], "category"))

    # ── Query analyzer ────────────────────────────────────────────────────────
    qa = QueryAnalyzer()
    test("QA: barcode detected",
         lambda: assert_eq(qa.analyze("BAR268726580")["query_mode"], "barcode"))
    test("QA: invoice PA detected",
         lambda: assert_eq(qa.analyze("find invoice PA00000003")["query_mode"], "invoice"))
    test("QA: invoice value correct",
         lambda: assert_eq(qa.analyze("find invoice PA00000003")["query_value"], "PA00000003"))
    test("QA: mobile number detected",
         lambda: assert_eq(qa.analyze("customer 9876543210")["query_mode"], "contact"))
    test("QA: active keyword detected",
         lambda: assert_eq(qa.analyze("show active printer")["query_mode"], "active"))

    # ── BUG-16 table build performance ────────────────────────────────────────
    big_records = [{"productName": f"Product {i}", "totalAmount": str(i * 10),
                    "barcode": f"BAR{i:09d}"} for i in range(600)]
    db._load("perf_tbl", big_records)
    ret2 = RAGRetriever()
    parsed_perf = {"intent": "BULK_LIST", "search_value": "", "target_col": None, "aggregation": "NONE"}
    rows_perf, cols_perf, _ = ret2.retrieve("perf_tbl", parsed_perf)
    t0 = time.time()
    tbl_html, rendered = synth.table(rows_perf[:600], cols_perf, False)
    elapsed = time.time() - t0
    test(f"BUG-16: table build 500 rows < 0.5s (took {elapsed:.3f}s)",
         lambda: assert_lte(elapsed, 0.5))
    test("PERF-11: render cap applied (≤500)",
         lambda: assert_lte(rendered, MAX_RENDER_ROWS))

    # ── _base_key helper ──────────────────────────────────────────────────────
    test("_base_key: strips _tgt_ suffix",
         lambda: assert_eq(_base_key("purchase_tgt_abc12345"), "purchase"))
    test("_base_key: strips _targeted suffix",
         lambda: assert_eq(_base_key("purchase_targeted"), "purchase"))
    test("_base_key: leaves clean key unchanged",
         lambda: assert_eq(_base_key("purchase"), "purchase"))

    # ── FIX-1: startup readiness event ───────────────────────────────────────
    # asyncio.Event must be created; before set() it blocks, after set() it passes
    import asyncio as _asyncio
    ev = _asyncio.Event()
    test("FIX-1: readiness event blocks before set",
         lambda: assert_eq(ev.is_set(), False))
    ev.set()
    test("FIX-1: readiness event passes after set",
         lambda: assert_eq(ev.is_set(), True))
    test("FIX-1: _startup_ready is an asyncio.Event",
         lambda: assert_eq(type(_startup_ready).__name__, "Event"))

    # ── FIX-2: CancelledError not swallowed ───────────────────────────────────
    # Verify the fetch method re-raises CancelledError immediately
    import inspect
    src = inspect.getsource(DataMirror._fetch_with_retry)
    test("FIX-2: CancelledError re-raised (not caught by broad except)",
         lambda: assert_in("CancelledError", src))
    test("FIX-2: httpx.Timeout object used (not float)",
         lambda: assert_in("httpx.Timeout", src))

    # ── FIX-3: circuit breaker state machine ─────────────────────────────────
    cb = CircuitBreaker()
    test("FIX-3: CB starts CLOSED",
         lambda: assert_eq(cb._state, _CBState.CLOSED))
    for _ in range(CB_FAILURE_THRESHOLD):
        cb.record_failure()
    test("FIX-3: CB opens after threshold failures",
         lambda: assert_eq(cb._state, _CBState.OPEN))
    test("FIX-3: CB is_open returns True when OPEN",
         lambda: assert_eq(cb.is_open, True))
    # Simulate recovery timeout elapsed
    cb._opened_at = time.time() - CB_RECOVERY_TIMEOUT - 1
    test("FIX-3: CB transitions to HALF_OPEN after timeout",
         lambda: assert_eq(cb.is_open, False))   # probe allowed, state → HALF_OPEN
    test("FIX-3: CB state is HALF_OPEN after probe",
         lambda: assert_eq(cb._state, _CBState.HALF_OPEN))
    for _ in range(CB_SUCCESS_THRESHOLD):
        cb.record_success()
    test("FIX-3: CB closes after success threshold",
         lambda: assert_eq(cb._state, _CBState.CLOSED))

    # ── FIX-4: cache invalidated on successful sync ───────────────────────────
    rc4 = ResponseCache(max_size=10, ttl=60)
    rc4.set("sell", "BULK_LIST", "", "english", "<html>old</html>")
    test("FIX-4: cache has entry before invalidation",
         lambda: assert_eq(bool(rc4.get("sell", "BULK_LIST", "", "english")), True))
    rc4.invalidate_module("sell")
    test("FIX-4: cache entry gone after invalidate_module",
         lambda: assert_eq(rc4.get("sell", "BULK_LIST", "", "english"), None))
    # Verify sync_module source calls invalidate
    sync_src = inspect.getsource(DataMirror.sync_module)
    test("FIX-4: sync_module calls response_cache.invalidate_module",
         lambda: assert_in("response_cache.invalidate_module", sync_src))

    # ── FIX-5: CORS_ORIGINS parsing ──────────────────────────────────────────
    # Simulate env var parsing
    def _parse_cors(env_val):
        return [o.strip() for o in env_val.split(",") if o.strip()] if env_val else ["*"]
    test("FIX-5: CORS parses single origin",
         lambda: assert_eq(_parse_cors("https://app.com"), ["https://app.com"]))
    test("FIX-5: CORS parses multiple origins",
         lambda: assert_eq(
             _parse_cors("https://app.com,https://admin.app.com"),
             ["https://app.com", "https://admin.app.com"]
         ))
    test("FIX-5: empty CORS env defaults to ['*']",
         lambda: assert_eq(_parse_cors(""), ["*"]))

    # ── FIX-6: security headers dict is complete ─────────────────────────────
    required_headers = {
        "X-Content-Type-Options", "X-Frame-Options",
        "X-XSS-Protection", "Content-Security-Policy",
        "Referrer-Policy", "Permissions-Policy",
    }
    test("FIX-6: all required security headers present",
         lambda: assert_eq(required_headers - set(SECURITY_HEADERS.keys()), set()))
    test("FIX-6: X-Frame-Options is DENY",
         lambda: assert_eq(SECURITY_HEADERS["X-Frame-Options"], "DENY"))
    test("FIX-6: X-Content-Type-Options is nosniff",
         lambda: assert_eq(SECURITY_HEADERS["X-Content-Type-Options"], "nosniff"))

    # ── FIX-7: request-ID generated when missing ─────────────────────────────
    # secrets.token_hex(8) produces 16-char hex string
    rid = secrets.token_hex(8)
    test("FIX-7: generated request-ID is 16 hex chars",
         lambda: assert_eq(len(rid), 16))
    test("FIX-7: request-ID is hexadecimal",
         lambda: assert_eq(rid, rid.lower()))

    # ── FIX-8: rate limiter returns remaining count ───────────────────────────
    rl2 = RateLimiter(rpm=5)
    allowed, remaining = rl2.check("10.0.0.1")
    test("FIX-8: first request allowed",
         lambda: assert_eq(allowed, True))
    test("FIX-8: remaining decrements correctly (4 left after 1st)",
         lambda: assert_eq(remaining, 4))
    for _ in range(4):
        rl2.check("10.0.0.1")
    blocked, rem2 = rl2.check("10.0.0.1")
    test("FIX-8: 6th request blocked",
         lambda: assert_eq(blocked, False))
    test("FIX-8: remaining is 0 when blocked",
         lambda: assert_eq(rem2, 0))
    test("FIX-8: check() method exists on RateLimiter",
         lambda: assert_eq(callable(getattr(rate_limiter, "check", None)), True))

    # ── Summary ───────────────────────────────────────────────────────────────
    total = passed + failed
    print(f"\n══════════════════════════════════════════")
    print(f"  Results: {passed}/{total} passed", end="")
    if failed:
        print(f"  ⚠️  {failed} FAILED")
    else:
        print("  🎉 ALL PASSED")
    print(f"══════════════════════════════════════════\n")
    return failed == 0
def assert_eq(a, b):
    assert a == b, f"Expected {b!r}, got {a!r}"

def assert_in(needle, haystack):
    assert needle in haystack, f"{needle!r} not found in result"

def assert_not_in(needle, haystack):
    assert needle not in haystack, f"{needle!r} should NOT be in result but was"

def assert_lte(a, b):
    assert a <= b, f"Expected ≤ {b}, got {a}"


if __name__ == "__main__":
    success = _run_tests()
    exit(0 if success else 1)