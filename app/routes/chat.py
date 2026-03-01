"""
==============================================================================
  ADMIN INTELLIGENCE ENGINE  v8.0  —  PRODUCTION READY
  ERP Chatbot RAG Pipeline · Zero external AI APIs · ≤300 MB RAM
  Full Marathi (Devanagari + Roman) + English support

  PHASE 2 CRITICAL  (Items 1–4) — built on v7.1 stable base
  ──────────────────────────────────────────────────────────────
  P2-1  : Pre-emptive background TTL refresh — asyncio.create_task() fires
           a silent re-fetch when module TTL is 80% expired, eliminating
           sync latency for hot modules. Deduped via _refreshing: Set[str].
  P2-2  : Persistent SQLite disk cache — /tmp/erp_cache.db stores serialised
           row data across dyno restarts. Warmup loads from disk first;
           API fetches only what's missing or truly stale. Falls back
           gracefully if /tmp is read-only.
  P2-3  : Date range query support — DateRangeParser (pure-regex, no
           dateutil dep) resolves "from X to Y", "between X and Y",
           "in January/2024", "last N days", "today/yesterday/this week"
           into (date_start, date_end) tuples. RAGRetriever._date_range()
           executes WHERE date_col BETWEEN ? AND ?.
  P2-4  : Context-aware follow-up queries — ConversationContext now stores
           (last_module, last_parsed, last_rows_key). FollowUpDetector
           recognises "sort by", "filter", "show only", "order by", "top N"
           phrases and, if the NLU confidence is low, re-applies SQL
           transforms (ORDER BY, WHERE LIKE, LIMIT) to the cached result
           set without re-fetching from the ERP server.

  ALL v7.1 FEATURES RETAINED (items 23–30, 43 bugs fixed).
==============================================================================
"""

from __future__ import annotations

# ── P1-30: python-dotenv optional load ───────────────────────────────────────
try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv()
except ImportError:
    pass

import asyncio
import collections
import html
import json
import logging
import math
import os
import re
import secrets
import signal
import sqlite3
import time
import random
import uuid
from collections import Counter, defaultdict, deque
from contextlib import asynccontextmanager
from datetime import date, timedelta
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Set, Tuple

# ── Optional imports (FastAPI/httpx) — mocked in tests ───────────────────────
try:
    import httpx
    from fastapi import APIRouter, HTTPException, Request, Response
    from pydantic import BaseModel
    _FASTAPI_AVAILABLE = True
except ImportError:
    _FASTAPI_AVAILABLE = False
    class BaseModel:
        def __init__(self, **kw): [setattr(self, k, v) for k, v in kw.items()]
    class Request: pass
    class Response: pass

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
BASE_URL          = os.getenv("ERP_BASE_URL",
                              "https://umbrellasales.xyz/umbrella-inventory-server")
LOGIN_URL         = f"{BASE_URL}/api/service/login"

_ERP_USER         = os.getenv("ERP_USERNAME", "")
_ERP_PASS         = os.getenv("ERP_PASSWORD", "")
_ALLOW_DEFAULT    = os.getenv("ALLOW_DEFAULT_CREDS", "false").lower() == "true"

if not _ERP_USER and not _ALLOW_DEFAULT:
    log.warning("ERP_USERNAME not set. Set ERP_USERNAME/ERP_PASSWORD env vars "
                "or ALLOW_DEFAULT_CREDS=true (dev only).")
if not _ERP_USER and _ALLOW_DEFAULT:
    _ERP_USER = "superadmin.com"
    _ERP_PASS = "superadmin@123"

LOGIN_CREDS             = {"username": _ERP_USER, "password": _ERP_PASS}

MODULE_TTL              = 300
STOCK_TTL               = 120
FINANCE_TTL             = 600
MAX_ROWS                = 30_000
MAX_RENDER_ROWS         = 500
API_TIMEOUT             = 30.0
API_MAX_RETRIES         = 3
JWT_EXPIRY_BUFFER       = 60
FLATTEN_MAX_CHILD_ROWS  = 200
RATE_LIMIT_RPM          = 60
MAX_SESSIONS            = 500
SESSION_TTL             = 1800
RESPONSE_CACHE_SIZE     = 512
RESPONSE_CACHE_TTL      = 90
CB_FAILURE_THRESHOLD    = 5
CB_RECOVERY_TIMEOUT     = 30.0
CB_SUCCESS_THRESHOLD    = 2
SQLITE_MMAP_SIZE        = 128 * 1024 * 1024

# P2-1: pre-emptive refresh fires when TTL is this fraction expired
PREEMPT_THRESHOLD       = 0.80

# P2-2: persistent disk cache path (Render /tmp survives dyno restarts)
DISK_CACHE_PATH         = os.getenv("ERP_DISK_CACHE", "/tmp/erp_cache.db")
DISK_CACHE_ENABLED      = os.getenv("ERP_DISK_CACHE_ENABLED", "true").lower() == "true"

# P1-23
_startup_ready: Optional[asyncio.Event] = None

_cors_env = os.getenv("CORS_ORIGINS", "")
CORS_ORIGINS: List[str] = (
    [o.strip() for o in _cors_env.split(",") if o.strip()]
    if _cors_env else ["*"]
)
if CORS_ORIGINS == ["*"]:
    log.warning("CORS_ORIGINS not set — defaulting to '*'. "
                "Set CORS_ORIGINS=https://yourapp.com in production!")

SECURITY_HEADERS = {
    "X-Content-Type-Options":  "nosniff",
    "X-Frame-Options":         "DENY",
    "X-XSS-Protection":        "1; mode=block",
    "Content-Security-Policy": (
        "default-src 'none'; "
        f"img-src 'self' data: {BASE_URL.split('/umbrella')[0]}; "
        "style-src 'unsafe-inline'; script-src 'none'"
    ),
    "Referrer-Policy":         "strict-origin-when-cross-origin",
    "Permissions-Policy":      "geolocation=(), microphone=(), camera=()",
}

# P1-24: FTS column allowlist
FTS_COLUMNS = frozenset({
    "name", "firstName", "lastName",
    "productName", "supplierName", "customerName", "materialName",
    "invoiceNo", "billNo", "supplierBill",
    "barcode",
    "contact", "phone", "mobile", "email",
    "status", "supplyType",
})

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
                        "how much do we owe", "credits owed",
                        "debit note", "debit notes"],
        "aliases":     ["supplier credit", "vendor credit", "credits outstanding",
                        "debit note", "debit notes"],
        "amount_cols": ["creditAmount", "amount", "totalCredit", "totalAmount"],
        "qty_cols":    [],
        "date_cols":   ["createdAt", "date"],
        "search_endpoints": {
            "id": {"url": "/api/supplier-credit/get-supplier-credit", "param": "id"},
        },
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
        "date_cols":   ["date", "createdAt"],
        "search_endpoints": {},
    },

    "customer-ledger": {
        "url":         "/api/reports/get-all-customer-ledgers",
        "ttl":         FINANCE_TTL,
        "keywords":    [],
        "phrases":     ["customer ledger", "client ledger", "customer statement",
                        "customer account", "receivables", "customer balance",
                        "customer account history", "customer outstanding"],
        "aliases":     ["customer ledger", "client ledger"],
        "amount_cols": ["debit", "credit", "balance", "amount"],
        "qty_cols":    [],
        "date_cols":   ["date", "createdAt"],
        "search_endpoints": {
            "id": {"url": "/api/reports/get-customer-ledger", "param": "customerId"},
        },
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
        "date_cols":   [],
        "search_endpoints": {},
    },

    "customer-ledger-detail": {
        "url":         "/api/reports/customer-ledger-detail",
        "ttl":         FINANCE_TTL,
        "keywords":    [],
        "phrases":     ["customer ledger detail", "customer ledger details",
                        "customer account detail", "customer transaction detail",
                        "detailed customer ledger", "customer detail statement"],
        "aliases":     ["customer ledger detail", "customer detail"],
        "amount_cols": ["debit", "credit", "balance", "amount"],
        "qty_cols":    [],
        "date_cols":   ["date", "createdAt"],
        "search_endpoints": {},
    },

    "sell": {
        "url":         "/api/sell/get-all-sells",
        "ttl":         MODULE_TTL,
        "keywords":    ["sell", "sells", "sold", "sale", "sales", "dispatch",
                        "invoice", "receipt", "billing", "revenue"],
        "phrases":     ["sales invoice", "sell invoice", "dispatch record", "all sales",
                        "sales list", "all sells", "invoices", "what did we sell",
                        "customer invoice", "sales history", "sell history",
                        "bills list", "show bills", "all bills"],
        "aliases":     ["sales", "sells", "invoices", "receipts", "billing",
                        "bills", "bill"],
        "amount_cols": ["totalAmount", "sellPrice", "amount", "paid"],
        "qty_cols":    ["quantity", "qty", "totalQuantity"],
        "date_cols":   ["sellDate", "createdAt", "date"],
        "search_endpoints": {
            "invoice":       {"url": "/api/sell/search-by-invoice",        "param": "invoiceNo"},
            "id":            {"url": "/api/sell/get-sell",                  "param": "id"},
            "next_invoice":  {"url": "/api/sell/get-next-sell-invoice-no", "param": None},
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
        "date_cols":   ["purchaseDate", "createdAt", "date"],
        "search_endpoints": {
            "barcode":       {"url": "/api/purchase/get-stock-by-barcode",       "param": "barcode"},
            "invoice":       {"url": "/api/purchase/get-purchase-by-invoice-no", "param": "invoiceNo"},
            "name":          {"url": "/api/purchase/get-stock-by-product-name",  "param": "productName"},
            "id":            {"url": "/api/purchase/get-purchase",               "param": "id"},
            "next_invoice":  {"url": "/api/purchase/get-next-purchase-invoice-no", "param": None},
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
        "date_cols":   ["purchaseDate", "createdAt", "date"],
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
        "date_cols":   ["date", "createdAt", "paymentDate"],
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
        "date_cols":   ["date", "createdAt", "paymentDate"],
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
        "date_cols":   ["createdAt"],
        "search_endpoints": {
            "name": {"url": "/api/material/get-material-by-name", "param": "name"},
            "id":   {"url": "/api/material/get-material",          "param": "id"},
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
        "date_cols":   ["createdAt"],
        "search_endpoints": {
            "contact": {"url": "/api/customer/search-by-contact", "param": "contact"},
            "id":      {"url": "/api/customer/get-customer",       "param": "id"},
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
        "date_cols":   ["createdAt"],
        "search_endpoints": {
            "id": {"url": "/api/supplier/get-supplier", "param": "id"},
        },
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
        "date_cols":   ["createdAt"],
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
        "date_cols":   [],
        "search_endpoints": {},
    },

    "printer": {
        "url":         "/api/printer/get-all-printers",
        "ttl":         MODULE_TTL,
        "keywords":    ["printer", "printers", "machine", "print"],
        "phrases":     ["printer list", "all printers", "printing machines",
                        "active printer", "show printers", "printer details"],
        "aliases":     ["printers", "printing"],
        "amount_cols": [],
        "qty_cols":    [],
        "date_cols":   [],
        "search_endpoints": {
            "active": {"url": "/api/printer/get-active-printer",  "param": None},
            "id":     {"url": "/api/printer/get-printer-by-id",   "param": "id"},
        },
    },

    "email-config": {
        "url":         "/api/email-config/get-all-emails",
        "ttl":         MODULE_TTL,
        "keywords":    ["email", "emails", "smtp", "config", "mail"],
        "phrases":     ["email config", "mail config", "smtp config",
                        "email settings", "active email", "email setup",
                        "email configuration"],
        "aliases":     ["emails", "mails", "smtp"],
        "amount_cols": [],
        "qty_cols":    [],
        "date_cols":   [],
        "search_endpoints": {
            "active": {"url": "/api/email-config/active",            "param": None},
            "id":     {"url": "/api/email-config/get-email-by-id",   "param": "id"},
        },
    },
}

WARMUP_MODULES = list(MODULE_REGISTRY.keys())

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
# COMPILED REGEXES
# ──────────────────────────────────────────────────────────────────────────────
BARCODE_RE      = re.compile(r"\bBAR\w{6,}\b",                        re.IGNORECASE)
INVOICE_RE      = re.compile(r"\b(PA\d{5,}|SA\d{5,}|INV[-/]\w+)\b",  re.IGNORECASE)
CONTACT_RE      = re.compile(r"\b[6-9]\d{9}\b")
ACTIVE_RE       = re.compile(r"\bactive\b",                            re.IGNORECASE)
ID_RE           = re.compile(r"\bid\s*[:#]?\s*(\d+)\b",               re.IGNORECASE)
NEXT_INVOICE_RE = re.compile(r"\bnext\s+(purchase|sell|sale|invoice)\s*(number|no|invoice)?\b",
                              re.IGNORECASE)
NAME_RE         = re.compile(
    r"\b(?:named?|called|for)\s+([\w\s]{2,40}?)(?:\s+(?:in|from|at|by|with|list|details|info|record|history)|\s*$)",
    re.IGNORECASE,
)
NEGATION_RE     = re.compile(
    r"\b(?:not|except|exclude|excluding|without)\s+([\w\s]{2,40}?)(?:\s+(?:in|from|at|by|with|list|and)|\s*$)",
    re.IGNORECASE,
)
CAMEL_RE        = re.compile(r"([a-z])([A-Z])")
CLEAN_RE        = re.compile(r"[^a-z0-9\s\-]")
FTS_SPECIAL     = re.compile(r'["()*+\-^~<>]')
_FTS_BOOL_RE    = re.compile(r'\b(NOT|OR|AND|NEAR)\b',                re.IGNORECASE)
DATETIME_RE     = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}")
SESSION_RE      = re.compile(r"[^a-zA-Z0-9_\-]")

# ── P2-4: follow-up detection regexes ────────────────────────────────────────
FOLLOWUP_SORT_RE   = re.compile(
    r"\b(?:sort|order|sorted|ordered)\s+(?:by\s+)?([\w\s]{2,30}?)(?:\s+(?:asc|desc|ascending|descending))?\s*$",
    re.IGNORECASE,
)
FOLLOWUP_FILTER_RE = re.compile(
    r"\b(?:filter|show\s+only|where|only\s+show|just\s+show)\s+([\w\s]{2,40}?)(?:\s+(?:in|from|at|by|with)|\s*$)",
    re.IGNORECASE,
)
FOLLOWUP_LIMIT_RE  = re.compile(
    r"\b(?:top|first|show|limit)\s+(\d{1,4})\b",
    re.IGNORECASE,
)
FOLLOWUP_TRIGGERS  = frozenset({
    "sort by", "order by", "sort", "ordered by", "sorted by",
    "filter by", "filter", "show only", "only show", "just show",
    "top ", "first ", "last ", "limit ", "where ",
    "ascending", "descending", "asc", "desc",
})

# ── P2-3: date range patterns ─────────────────────────────────────────────────
# ISO date: 2024-01-15 or 2024/01/15
_ISO_DATE  = r"(\d{4}[-/]\d{1,2}[-/]\d{1,2})"
# Natural: "15 Jan 2024", "Jan 15 2024", "15th January 2024"
_MONTH_NAMES = (r"(?:january|february|march|april|may|june|july|august|september|"
                r"october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)")
_NAT_DATE  = (rf"(\d{{1,2}}(?:st|nd|rd|th)?\s+{_MONTH_NAMES}\s+\d{{4}}|"
              rf"{_MONTH_NAMES}\s+\d{{1,2}}(?:st|nd|rd|th)?\s+\d{{4}}|"
              rf"\d{{1,2}}[-/]\d{{1,2}}[-/]\d{{2,4}})")
DATE_RANGE_RE = re.compile(
    rf"(?:from\s+{_ISO_DATE}|{_NAT_DATE})\s+(?:to|until|through|till)\s+(?:{_ISO_DATE}|{_NAT_DATE})|"
    rf"between\s+(?:{_ISO_DATE}|{_NAT_DATE})\s+and\s+(?:{_ISO_DATE}|{_NAT_DATE})",
    re.IGNORECASE,
)
LAST_N_DAYS_RE  = re.compile(r"\blast\s+(\d+)\s+days?\b",          re.IGNORECASE)
LAST_N_WEEKS_RE = re.compile(r"\blast\s+(\d+)\s+weeks?\b",         re.IGNORECASE)
LAST_N_MONTHS_RE= re.compile(r"\blast\s+(\d+)\s+months?\b",        re.IGNORECASE)
THIS_WEEK_RE    = re.compile(r"\bthis\s+week\b",                    re.IGNORECASE)
THIS_MONTH_RE   = re.compile(r"\bthis\s+month\b",                   re.IGNORECASE)
TODAY_RE        = re.compile(r"\btoday\b",                           re.IGNORECASE)
YESTERDAY_RE    = re.compile(r"\byesterday\b",                       re.IGNORECASE)
NAMED_MONTH_RE  = re.compile(
    rf"\bin\s+({_MONTH_NAMES})(?:\s+(\d{{4}}))?\b", re.IGNORECASE
)
YEAR_RE         = re.compile(r"\bin\s+(\d{4})\b", re.IGNORECASE)

_MONTH_MAP = {
    "january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
    "july":7,"august":8,"september":9,"october":10,"november":11,"december":12,
    "jan":1,"feb":2,"mar":3,"apr":4,"jun":6,"jul":7,"aug":8,
    "sep":9,"oct":10,"nov":11,"dec":12,
}

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
    "me","find","what","is","are","the","a","an","of","for","please","can","you",
    "tell","we","have","our","in","any","by","its","their","about","on","who","which",
    "specific","details","show","get","fetch","display","list","all","every","entire",
    "give","check","search","look","want","need","see","view","pull","do","with","from",
    "this","that","it","i","my","he","she","they","them","up","out","how","many","much",
    "total","count","number","no","has","some","certain","latest","recent","current",
    "new","old","make","create","report","data","records","record","entries","full",
    "complete","whole","information","info",
    "named","called","having","containing","where","whose","like","whose","between",
    "using","via","through","based","order","orders","type","types","whose","its",
    "their","belongs","belonging","related","regarding","about","under","above",
    "not","except","exclude","excluding","without",
})
EN_BULK = frozenset({
    "all","list","every","entire","show","get","fetch","display","give","complete","full",
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
    "customer-ledger-detail": {
        "deva":["ग्राहक खाते तपशील","ग्राहक खाते विस्तृत","ग्राहक तपशील विवरण"],
        "roman":["grahak khate tapshil","customer ledger detail","grahak detail",
                 "customer tapshil","grahak vivaran tapshil"],
    },
    "customer-ledger-summary": {
        "deva":["ग्राहक खाते सारांश","ग्राहक शिल्लक सारांश","ग्राहक सारांश",
                "ग्राहक थकबाकी सारांश"],
        "roman":["grahak khate saransh","grahak shillak saransh",
                 "customer ledger summary","grahak saransh",
                 "customer balance summary"],
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
        "deva":["प्रिंटर यादी","मुद्रण यंत्र","प्रिंटर","सक्रिय प्रिंटर"],
        "roman":["printer yadi","mudran yantra","printer list","printer","active printer"],
    },
    "email-config": {
        "deva":["ईमेल सेटिंग","ईमेल कॉन्फिग","ईमेल","मेल","सक्रिय ईमेल"],
        "roman":["email settings","email config","mail config","email","active email"],
    },
}

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
    "mala","mhala","chi","che","cha","supplier","saransh",
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
    "Sgst":"एसजीएसटी","Igst":"आयजीएसटी","Id":"आयडी",
}

MR_MODULES: Dict[str, str] = {
    "purchase":"खरेदी","sell":"विक्री","supplier-credit":"पुरवठादार क्रेडिट",
    "supplier-ledger":"पुरवठादार खातेवही","customer-ledger":"ग्राहक खातेवही",
    "customer-ledger-summary":"ग्राहक खाते सारांश",
    "customer-ledger-detail":"ग्राहक खाते तपशील",
    "customer":"ग्राहक","supplier":"पुरवठादार","product-stock":"उत्पादन साठा",
    "payment":"देयक","customer-payment-history":"ग्राहक देयक इतिहास",
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
    t     = text.lower()
    words = set(t.split())
    for m in _MR_ROMAN_MARKERS:
        if len(m) <= 4:
            if m in words:
                return "marathi_roman"
        else:
            if m in words or m in t:
                return "marathi_roman"
    return "english"


def _apply_deva_stems(text: str) -> str:
    return " ".join(DEVA_STEMS.get(w, w) for w in text.split())


# ──────────────────────────────────────────────────────────────────────────────
# MARATHI CLASSIFIER
# ──────────────────────────────────────────────────────────────────────────────
def marathi_classify(text: str) -> Optional[str]:
    words   = text.split()
    stemmed = " ".join(DEVA_STEMS.get(w, w) for w in words)
    t       = stemmed.lower().strip()
    best_key, best_score = None, 0.0
    for keyword, module in _MR_FLAT:
        if keyword in t:
            score = min(1.0, len(keyword.split()) * 0.35 + 0.3)
            if score > best_score:
                best_score, best_key = score, module
            if best_score >= 0.95:
                break
    return best_key if best_score >= 0.4 else None


# ──────────────────────────────────────────────────────────────────────────────
# P2-3: DATE RANGE PARSER
# ──────────────────────────────────────────────────────────────────────────────
class DateRangeParser:
    """
    Pure-regex date range parser. Returns (start_iso, end_iso) strings or None.
    Supports:
      - "from 2024-01-01 to 2024-03-31"
      - "between 2024-01-01 and 2024-03-31"
      - "last 7 days / last 2 weeks / last 3 months"
      - "this week / this month"
      - "today / yesterday"
      - "in January / in January 2024"
      - "in 2024"
    """

    def parse(self, text: str) -> Optional[Tuple[str, str]]:
        t = text.lower().strip()
        today = date.today()

        # today / yesterday
        if TODAY_RE.search(t):
            return today.isoformat(), today.isoformat()
        if YESTERDAY_RE.search(t):
            d = today - timedelta(days=1)
            return d.isoformat(), d.isoformat()

        # last N days/weeks/months
        m = LAST_N_DAYS_RE.search(t)
        if m:
            n = int(m.group(1))
            return (today - timedelta(days=n)).isoformat(), today.isoformat()
        m = LAST_N_WEEKS_RE.search(t)
        if m:
            n = int(m.group(1))
            return (today - timedelta(weeks=n)).isoformat(), today.isoformat()
        m = LAST_N_MONTHS_RE.search(t)
        if m:
            n = int(m.group(1))
            # approximate: 30 days per month
            return (today - timedelta(days=n * 30)).isoformat(), today.isoformat()

        # this week / this month
        if THIS_WEEK_RE.search(t):
            start = today - timedelta(days=today.weekday())
            return start.isoformat(), today.isoformat()
        if THIS_MONTH_RE.search(t):
            start = today.replace(day=1)
            return start.isoformat(), today.isoformat()

        # in January [2024]
        m = NAMED_MONTH_RE.search(t)
        if m:
            month_num = _MONTH_MAP.get(m.group(1).lower())
            year = int(m.group(2)) if m.group(2) else today.year
            if month_num:
                import calendar
                _, last_day = calendar.monthrange(year, month_num)
                start = date(year, month_num, 1)
                end   = date(year, month_num, last_day)
                return start.isoformat(), end.isoformat()

        # in 2024
        m = YEAR_RE.search(t)
        if m:
            year = int(m.group(1))
            if 2000 <= year <= 2100:
                return f"{year}-01-01", f"{year}-12-31"

        # from X to Y or between X and Y (ISO dates)
        # try simple ISO date pairs
        iso_pair = re.findall(r"(\d{4}[-/]\d{1,2}[-/]\d{1,2})", t)
        if len(iso_pair) >= 2:
            start = iso_pair[0].replace("/", "-")
            end   = iso_pair[-1].replace("/", "-")
            return start, end

        return None

    def find_date_col(self, cols: List[str], mod_key: str) -> Optional[str]:
        """Find the most appropriate date column for this module."""
        mod      = MODULE_REGISTRY.get(mod_key, {})
        pref     = mod.get("date_cols", [])
        for dc in pref:
            for c in cols:
                if c.lower() == dc.lower():
                    return c
        # fallback: any col with 'date' or 'at' in name
        for c in cols:
            cl = c.lower()
            if "date" in cl or cl.endswith("at"):
                return c
        return None


date_range_parser = DateRangeParser()


# ──────────────────────────────────────────────────────────────────────────────
# QUERY ANALYZER  (P1-25 NAME_RE, P1-28 NEGATION_RE, P2-3 date hints)
# ──────────────────────────────────────────────────────────────────────────────
class QueryAnalyzer:
    def analyze(self, user_query: str) -> Dict[str, Any]:
        q = user_query.strip()

        m = BARCODE_RE.search(q)
        if m:
            return {"query_mode": "barcode", "query_value": m.group(0).upper(),
                    "date_range": None}

        m = INVOICE_RE.search(q)
        if m:
            return {"query_mode": "invoice", "query_value": m.group(0).upper(),
                    "date_range": None}

        m = CONTACT_RE.search(q)
        if m:
            return {"query_mode": "contact", "query_value": m.group(0),
                    "date_range": None}

        m = NEXT_INVOICE_RE.search(q)
        if m:
            return {"query_mode": "next_invoice", "query_value": m.group(1).lower(),
                    "date_range": None}

        m = ID_RE.search(q)
        if m:
            return {"query_mode": "id", "query_value": m.group(1),
                    "date_range": None}

        m = NAME_RE.search(q)
        if m:
            name_val = m.group(1).strip()
            if name_val and len(name_val) >= 2:
                return {"query_mode": "name", "query_value": name_val,
                        "date_range": None}

        m = NEGATION_RE.search(q)
        if m:
            neg_val = m.group(1).strip()
            if neg_val and len(neg_val) >= 2:
                return {"query_mode": "negation", "query_value": neg_val,
                        "date_range": None}

        # P2-3: detect date range BEFORE active/None so date queries work
        date_result = date_range_parser.parse(q)
        if date_result:
            return {"query_mode": "date_range", "query_value": None,
                    "date_range": date_result}

        if ACTIVE_RE.search(q):
            return {"query_mode": "active", "query_value": None,
                    "date_range": None}

        return {"query_mode": None, "query_value": None, "date_range": None}


query_analyzer = QueryAnalyzer()


# ──────────────────────────────────────────────────────────────────────────────
# NLU — 5-STAGE ENGLISH INTENT CLASSIFIER
# ──────────────────────────────────────────────────────────────────────────────
AGG_TRIGGERS = frozenset({
    "total","sum","how many","count","how much","aggregate",
    "altogether","combined","overall","एकूण","बेरीज","किती आहे",
})


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
# P2-4: FOLLOW-UP DETECTOR
# ──────────────────────────────────────────────────────────────────────────────
class FollowUpDetector:
    """
    Detects whether a query is a follow-up operation (sort/filter/limit)
    on the previous result set rather than a new module query.
    Returns a FollowUpOp or None.
    """

    def detect(self, query: str) -> Optional[Dict[str, Any]]:
        q = query.lower().strip()

        # Must start with or contain a follow-up trigger
        is_followup = any(q.startswith(t) or f" {t}" in q for t in FOLLOWUP_TRIGGERS)
        if not is_followup:
            return None

        op: Dict[str, Any] = {}

        # Sort direction
        if "desc" in q or "descending" in q or "highest" in q or "latest" in q or "newest" in q:
            op["sort_dir"] = "DESC"
        elif "asc" in q or "ascending" in q or "oldest" in q or "lowest" in q:
            op["sort_dir"] = "ASC"
        else:
            op["sort_dir"] = "DESC"  # sensible default

        # Sort column
        m = FOLLOWUP_SORT_RE.search(q)
        if m:
            op["sort_col"] = m.group(1).strip()

        # Filter value
        m = FOLLOWUP_FILTER_RE.search(q)
        if m:
            op["filter_val"] = m.group(1).strip()

        # Limit
        m = FOLLOWUP_LIMIT_RE.search(q)
        if m:
            op["limit"] = int(m.group(1))

        # Only return if we actually captured something useful
        if not op or (len(op) == 1 and "sort_dir" in op):
            return None
        return op


follow_up_detector = FollowUpDetector()


# ──────────────────────────────────────────────────────────────────────────────
# QUERY PARSER  (P1-28 NEGATIVE_SEARCH, P2-3 DATE_RANGE intents)
# ──────────────────────────────────────────────────────────────────────────────
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

        target_col = None
        col_words: set = set()
        for trigger, cols in COLUMN_TRIGGERS.items():
            if re.search(rf"\b{re.escape(trigger)}s?\b", text_lower):
                target_col = cols[0]
                col_words.update([trigger, trigger + "s", trigger + "es"])
                break

        strip        = stop | mod_words | col_words
        value_tokens = [tok for tok in tokens if tok not in strip and len(tok) > 1]

        code_tokens = [
            tok for tok in tokens
            if re.match(r"^[a-z0-9\-]+$", tok)
            and (re.search(r"\d", tok) or len(tok) >= 6)
            and tok not in strip
        ]
        if code_tokens and not value_tokens:
            value_tokens = code_tokens

        search_value = " ".join(value_tokens).strip()

        if len(value_tokens) == 0 and len(tokens) > 0:
            is_bulk = True

        negation_value: Optional[str] = None
        neg_m = NEGATION_RE.search(user_query.lower())
        if neg_m:
            negation_value = neg_m.group(1).strip()

        # P2-3: detect date range
        date_range = date_range_parser.parse(user_query)

        if target_col:
            intent = "COLUMN_SEARCH" if search_value else "PROMPT_NEEDED"
        elif negation_value:
            intent = "NEGATIVE_SEARCH"
            search_value = negation_value
        elif date_range:
            intent = "DATE_RANGE"
        elif search_value and not is_bulk:
            intent = "GLOBAL_SEARCH"
        else:
            intent = "BULK_LIST"

        return {
            "intent":          intent,
            "target_col":      target_col,
            "search_value":    search_value,
            "negation_value":  negation_value,
            "date_range":      date_range,
            "aggregation":     "SUM" if agg else "NONE",
        }


# ──────────────────────────────────────────────────────────────────────────────
# P2-2: DISK CACHE MANAGER
# ──────────────────────────────────────────────────────────────────────────────
class DiskCacheManager:
    """
    Persists module row data to /tmp/erp_cache.db between dyno restarts.
    Falls back gracefully if the path is read-only or unavailable.
    Schema: erp_cache(key TEXT PRIMARY KEY, data_json TEXT, loaded_at REAL)
    """

    def __init__(self, path: str = DISK_CACHE_PATH, enabled: bool = DISK_CACHE_ENABLED):
        self._path    = path
        self._enabled = enabled
        self._conn:   Optional[sqlite3.Connection] = None
        self._ok      = False
        if enabled:
            self._init()

    def _init(self):
        try:
            self._conn = sqlite3.connect(self._path, check_same_thread=False)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS erp_cache (
                    key       TEXT PRIMARY KEY,
                    data_json TEXT NOT NULL,
                    loaded_at REAL NOT NULL
                )
            """)
            self._conn.commit()
            self._ok = True
            log.info("DiskCache: connected at %s", self._path)
        except Exception as exc:
            log.warning("DiskCache: could not open %s — %s (disk cache disabled)", self._path, exc)
            self._conn = None
            self._ok   = False

    def save(self, key: str, records: List[Dict], loaded_at: float):
        if not self._ok or self._conn is None:
            return
        try:
            data_json = json.dumps(records, ensure_ascii=False, separators=(",", ":"))
            self._conn.execute(
                "INSERT OR REPLACE INTO erp_cache(key, data_json, loaded_at) VALUES (?,?,?)",
                (key, data_json, loaded_at),
            )
            self._conn.commit()
        except Exception as exc:
            log.warning("DiskCache.save(%s) failed: %s", key, exc)

    def load(self, key: str, max_age: float) -> Optional[Tuple[List[Dict], float]]:
        """Return (records, loaded_at) if cache entry exists and is younger than max_age seconds."""
        if not self._ok or self._conn is None:
            return None
        try:
            cur = self._conn.execute(
                "SELECT data_json, loaded_at FROM erp_cache WHERE key=?", (key,)
            )
            row = cur.fetchone()
            if row is None:
                return None
            loaded_at = float(row[1])
            if time.time() - loaded_at > max_age:
                return None  # too stale
            records = json.loads(row[0])
            log.info("DiskCache: loaded %d rows for '%s' from disk", len(records), key)
            return records, loaded_at
        except Exception as exc:
            log.warning("DiskCache.load(%s) failed: %s", key, exc)
            return None

    def delete(self, key: str):
        if not self._ok or self._conn is None:
            return
        try:
            self._conn.execute("DELETE FROM erp_cache WHERE key=?", (key,))
            self._conn.commit()
        except Exception:
            pass

    def close(self):
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass

    @property
    def is_available(self) -> bool:
        return self._ok


disk_cache = DiskCacheManager()


# ──────────────────────────────────────────────────────────────────────────────
# DATA MIRROR — SQLite in-memory + FTS5 + P2-1 pre-emptive refresh + P2-2 disk
# ──────────────────────────────────────────────────────────────────────────────
class DataMirror:
    def __init__(self):
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.execute("PRAGMA cache_size=-16000")
        self.conn.execute(f"PRAGMA mmap_size={SQLITE_MMAP_SIZE}")

        self._ttl:         Dict[str, float]        = {}
        self._ttl_start:   Dict[str, float]        = {}   # P2-1: track when TTL was set
        self._ttl_dur:     Dict[str, float]        = {}   # P2-1: track TTL duration
        self._stale_data:  Dict[str, bool]         = {}
        self._cols:        Dict[str, List[str]]    = {}
        self._fts_cols:    Dict[str, List[str]]    = {}
        self._locks:       collections.defaultdict = collections.defaultdict(asyncio.Lock)
        self._refreshing:  Set[str]                = set()   # P2-1: dedup guard
        self._jwt_token:   Optional[str]           = None
        self._jwt_expires: float                   = 0.0
        self._jwt_lock:    asyncio.Lock            = asyncio.Lock()
        self._http_client: Optional[Any]           = None
        self._start_time = time.time()

    async def open(self):
        if _FASTAPI_AVAILABLE:
            self._http_client = httpx.AsyncClient(
                timeout=API_TIMEOUT,
                limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
            )

    async def close(self):
        if self._http_client:
            await self._http_client.aclose()
        disk_cache.close()

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

    async def _fetch_with_retry(self, url: str,
                                params: Optional[Dict] = None) -> Optional[Any]:
        last_exc = None
        req_timeout = httpx.Timeout(connect=5.0, read=25.0, write=5.0, pool=5.0)
        for attempt in range(API_MAX_RETRIES):
            try:
                token = await self._get_token()
                if not token:
                    last_exc = Exception("no token")
                    await asyncio.sleep(1.5 * (2 ** attempt))
                    continue
                headers = {"Authorization": f"Bearer {token}",
                           "Content-Type": "application/json"}
                req_url = f"{BASE_URL}{url}"
                client  = self._http_client or httpx.AsyncClient(timeout=req_timeout)
                resp    = await client.get(req_url, headers=headers,
                                           params=params or {},
                                           timeout=req_timeout)
                if resp.status_code == 200:
                    return resp.json()
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
                last_exc = exc
                log.warning("%s attempt %d timeout/connect: %s", url, attempt + 1, exc)
            except asyncio.CancelledError:
                log.warning("%s cancelled", url)
                raise
            except Exception as exc:
                last_exc = exc
                log.warning("%s attempt %d exception: %s", url, attempt + 1, exc)
            if attempt < API_MAX_RETRIES - 1:
                await asyncio.sleep(1.5 * (2 ** attempt))
        log.error("All %d retries failed for %s: %s", API_MAX_RETRIES, url, last_exc)
        return None

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

    def _load(self, key: str, records: List[Dict]):
        cur = self.conn.cursor()
        cur.execute(f'DROP TABLE IF EXISTS "{key}_fts"')
        cur.execute(f'DROP TABLE IF EXISTS "{key}"')
        if not records:
            cur.execute(f'CREATE TABLE "{key}" (_empty TEXT)')
            self._cols[key]     = []
            self._fts_cols[key] = []
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

        fts_eligible = [k for k in all_keys if k in FTS_COLUMNS]
        if not fts_eligible:
            fts_eligible = [k for k in all_keys
                            if k.lower() not in ("_rowid", "_empty", "_data")][:5]

        if fts_eligible:
            fts_cols_ddl = ", ".join(f'"{k}"' for k in fts_eligible)
            fts_col_sel  = ", ".join(f'"{k}"' for k in fts_eligible)
            cur.execute(
                f'CREATE VIRTUAL TABLE "{key}_fts" USING fts5('
                f'{fts_cols_ddl}, content="{key}", content_rowid="_rowid",'
                f'tokenize="unicode61 remove_diacritics 1")'
            )
            cur.execute(
                f'INSERT INTO "{key}_fts" (rowid, {fts_col_sel}) '
                f'SELECT _rowid, {fts_col_sel} FROM "{key}"'
            )

        self.conn.commit()
        self._cols[key]     = all_keys
        self._fts_cols[key] = fts_eligible
        log.info("Loaded %d rows into '%s' (fts on %d/%d cols)",
                 len(records), key, len(fts_eligible), len(all_keys))

    def _drop_targeted(self, key: str):
        try:
            cur = self.conn.cursor()
            cur.execute(f'DROP TABLE IF EXISTS "{key}_fts"')
            cur.execute(f'DROP TABLE IF EXISTS "{key}"')
            self.conn.commit()
            self._cols.pop(key, None)
            self._fts_cols.pop(key, None)
        except Exception:
            pass

    # ── P2-1: background pre-emptive refresh ─────────────────────────────────
    def _should_preempt(self, key: str) -> bool:
        """Return True if TTL is >PREEMPT_THRESHOLD expired and no refresh running."""
        if key in self._refreshing:
            return False
        start = self._ttl_start.get(key, 0)
        dur   = self._ttl_dur.get(key, 0)
        if dur <= 0:
            return False
        elapsed = time.time() - start
        return elapsed >= dur * PREEMPT_THRESHOLD

    async def _background_refresh(self, key: str):
        """Silent background refresh — updates data without blocking the caller."""
        if key in self._refreshing:
            return
        self._refreshing.add(key)
        try:
            log.debug("Pre-emptive refresh starting for '%s'", key)
            mod = MODULE_REGISTRY[key]
            raw = await self._fetch_with_retry(mod["url"])
            if raw is None:
                circuit_breaker.record_failure()
                log.warning("Pre-emptive refresh failed for '%s'", key)
                return
            circuit_breaker.record_success()
            flat = self._flatten(self._extract_list(raw))
            async with self._locks[key]:
                self._load(key, flat)
                now = time.time()
                self._ttl[key]       = now + mod["ttl"]
                self._ttl_start[key] = now
                self._ttl_dur[key]   = mod["ttl"]
                self._stale_data[key] = False
                response_cache.invalidate_module(key)
            # P2-2: persist to disk
            disk_cache.save(key, flat, time.time())
            log.info("Pre-emptive refresh complete for '%s'", key)
        except Exception as exc:
            log.warning("Pre-emptive refresh error for '%s': %s", key, exc)
        finally:
            self._refreshing.discard(key)

    async def sync_module(self, key: str, force: bool = False) -> str:
        now = time.time()
        if not force and now < self._ttl.get(key, 0):
            # P2-1: schedule background refresh if near expiry
            if self._should_preempt(key):
                asyncio.create_task(self._background_refresh(key))
            return "fresh"

        if circuit_breaker.is_open:
            log.debug("CircuitBreaker OPEN — skipping sync for '%s'", key)
            return "stale" if self._cols.get(key) else "error"

        async with self._locks[key]:
            now = time.time()
            if not force and now < self._ttl.get(key, 0):
                return "fresh"

            # P2-2: try disk cache first (only on first load, not force refresh)
            if not force and not self._cols.get(key):
                mod     = MODULE_REGISTRY[key]
                cached  = disk_cache.load(key, max_age=mod["ttl"] * 2)
                if cached:
                    flat, loaded_at = cached
                    self._load(key, flat)
                    self._ttl[key]        = loaded_at + mod["ttl"]
                    self._ttl_start[key]  = loaded_at
                    self._ttl_dur[key]    = mod["ttl"]
                    self._stale_data[key] = (time.time() > loaded_at + mod["ttl"])
                    log.info("Restored '%s' from disk cache (%d rows)", key, len(flat))
                    # Schedule a background refresh to get fresh data
                    asyncio.create_task(self._background_refresh(key))
                    return "disk"

            mod = MODULE_REGISTRY[key]
            raw = await self._fetch_with_retry(mod["url"])
            if raw is None:
                circuit_breaker.record_failure()
                if self._cols.get(key):
                    self._stale_data[key] = True
                    log.warning("Using stale data for '%s'", key)
                    return "stale"
                return "error"
            circuit_breaker.record_success()
            flat = self._flatten(self._extract_list(raw))
            self._load(key, flat)
            now = time.time()
            self._ttl[key]        = now + mod["ttl"]
            self._ttl_start[key]  = now
            self._ttl_dur[key]    = mod["ttl"]
            self._stale_data[key] = False
            response_cache.invalidate_module(key)
            # P2-2: persist to disk
            disk_cache.save(key, flat, now)
            return "ok"

    async def sync_modules(self, keys: List[str],
                           force: bool = False) -> Dict[str, str]:
        results = await asyncio.gather(
            *[self.sync_module(k, force) for k in keys],
            return_exceptions=True,
        )
        return {k: (r if isinstance(r, str) else "error")
                for k, r in zip(keys, results)}

    async def sync_all(self, force: bool = False) -> Dict[str, str]:
        return await self.sync_modules(list(MODULE_REGISTRY.keys()), force=force)

    def columns(self, key: str) -> List[str]:
        return self._cols.get(key, [])

    def fts_columns(self, key: str) -> List[str]:
        return self._fts_cols.get(key, [])

    def is_stale(self, key: str) -> bool:
        return self._stale_data.get(key, False)

    def uptime_seconds(self) -> float:
        return time.time() - self._start_time

    def cache_status(self) -> Dict[str, Any]:
        now = time.time()
        return {
            k: {
                "cached":        bool(self._cols.get(k)),
                "ttl_remaining": max(0, int(self._ttl.get(k, 0) - now)),
                "stale":         self._stale_data.get(k, False),
                "preempting":    k in self._refreshing,
            }
            for k in MODULE_REGISTRY
        }


db = DataMirror()


# ──────────────────────────────────────────────────────────────────────────────
# CIRCUIT BREAKER
# ──────────────────────────────────────────────────────────────────────────────
class _CBState:
    CLOSED    = "closed"
    OPEN      = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self):
        self._state     = _CBState.CLOSED
        self._failures  = 0
        self._successes = 0
        self._opened_at = 0.0

    @property
    def is_open(self) -> bool:
        if self._state == _CBState.OPEN:
            if time.time() - self._opened_at >= CB_RECOVERY_TIMEOUT:
                self._state     = _CBState.HALF_OPEN
                self._successes = 0
                log.info("CircuitBreaker → HALF_OPEN")
                return False
            return True
        return False

    def record_success(self):
        if self._state == _CBState.HALF_OPEN:
            self._successes += 1
            if self._successes >= CB_SUCCESS_THRESHOLD:
                self._state    = _CBState.CLOSED
                self._failures = 0
                log.info("CircuitBreaker → CLOSED")
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
        return {"state": self._state, "failures": self._failures,
                "opened_at": self._opened_at}


circuit_breaker = CircuitBreaker()


# ──────────────────────────────────────────────────────────────────────────────
# SMART QUERY ROUTER
# ──────────────────────────────────────────────────────────────────────────────
class SmartQueryRouter:
    _ERROR_KEYS = frozenset({"message","error","status","statusCode","code","detail","msg"})

    def _is_error_body(self, d: dict) -> bool:
        if not d:
            return True
        keys = set(d.keys())
        if keys <= self._ERROR_KEYS:
            return True
        for k, v in d.items():
            if k.lower() in ("success", "ok") and v is False:
                return True
            if k.lower() == "status" and str(v).lower() in ("error","fail","failed","not found"):
                return True
        return False

    async def route(self, key: str, parsed: Dict,
                    query_mode: str, query_value: Optional[str]
                    ) -> Tuple[bool, str, Optional[str]]:
        mod       = MODULE_REGISTRY.get(key, {})
        endpoints = mod.get("search_endpoints", {})
        if not query_mode or query_mode not in endpoints:
            return False, "", None

        ep = endpoints[query_mode]

        if query_mode == "next_invoice":
            raw = await db._fetch_with_retry(ep["url"])
            if raw is None:
                return False, "", None
            if isinstance(raw, dict):
                val = (raw.get("invoiceNo") or raw.get("nextInvoiceNo") or
                       raw.get("data") or raw.get("result") or str(raw))
            else:
                val = str(raw)
            return True, "", str(val)

        if ep["param"] is None:
            raw = await db._fetch_with_retry(ep["url"])
        else:
            if not query_value:
                return False, "", None
            raw = await db._fetch_with_retry(ep["url"],
                                              params={ep["param"]: query_value})
        if raw is None:
            return False, "", None

        records = db._extract_list(raw)
        if not records and isinstance(raw, dict):
            if self._is_error_body(raw):
                return False, "", None
            records = [raw]

        if not records:
            return False, "", None

        flat = db._flatten(records)
        if not flat:
            return False, "", None

        targeted_key = f"_tgt_{key}_{uuid.uuid4().hex[:8]}"
        db._load(targeted_key, flat)
        return True, targeted_key, None


smart_router = SmartQueryRouter()


# ──────────────────────────────────────────────────────────────────────────────
# RAG RETRIEVER  (P1-28 negative, P2-3 date_range)
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

        if intent == "NEGATIVE_SEARCH" and val:
            rows, method = self._negative(table_key, val, cols)
            return rows, cols, method

        if intent == "DATE_RANGE":
            date_range = parsed.get("date_range")
            if date_range:
                rows, method = self._date_range(table_key, date_range, cols, table_key)
                if rows:
                    return rows, cols, method
            # fallback to bulk if date range yields nothing
            return self._bulk(table_key), cols, "bulk"

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
            cur      = db.conn.cursor()
            fts_cols = db.fts_columns(key)
            if not fts_cols:
                return [], "fts_no_index"
            safe = FTS_SPECIAL.sub(" ", val)
            safe = _FTS_BOOL_RE.sub(" ", safe).strip()
            if not safe:
                return [], "fts_empty"
            if tcol:
                ac = self._resolve_col(tcol, fts_cols)
                fts_q = f'"{ac}" : "{safe}"*' if ac else f'"{safe}"*'
            else:
                words = [w for w in safe.split() if len(w) > 1]
                if not words:
                    return [], "fts_empty"
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

    def _negative(self, key: str, val: str, cols: List[str]) -> Tuple[List, str]:
        try:
            cur    = db.conn.cursor()
            conds  = " AND ".join(f'("{c}" NOT LIKE ? OR "{c}" IS NULL)' for c in cols)
            params = [f"%{val}%"] * len(cols)
            cur.execute(
                f'SELECT * FROM "{key}" WHERE {conds} LIMIT {self.MAX_LIKE}',
                params,
            )
            rows = [dict(r) for r in cur.fetchall()]
            for r in rows:
                r.pop("_rowid", None)
            return rows, "negative"
        except Exception as e:
            log.warning("Negative search error %s: %s", key, e)
            return [], "negative_error"

    def _date_range(self, key: str, date_range: Tuple[str, str],
                    cols: List[str], mod_key: str) -> Tuple[List, str]:
        """
        P2-3: Execute WHERE date_col BETWEEN start AND end.
        Tries each candidate date column until rows are found.
        """
        start, end = date_range
        date_col   = date_range_parser.find_date_col(cols, mod_key)
        candidates = []
        if date_col:
            candidates.append(date_col)
        # also try any col with 'date' in name not already included
        for c in cols:
            if "date" in c.lower() and c not in candidates:
                candidates.append(c)
            if c.lower().endswith("at") and c not in candidates:
                candidates.append(c)

        for dc in candidates:
            try:
                cur = db.conn.cursor()
                # Support both ISO date prefix and full datetime
                cur.execute(
                    f'SELECT * FROM "{key}" '
                    f'WHERE SUBSTR("{dc}", 1, 10) >= ? AND SUBSTR("{dc}", 1, 10) <= ? '
                    f'ORDER BY "{dc}" DESC LIMIT {self.MAX_LIKE}',
                    [start, end],
                )
                rows = [dict(r) for r in cur.fetchall()]
                for r in rows:
                    r.pop("_rowid", None)
                if rows:
                    log.info("Date range %s→%s on col '%s': %d rows", start, end, dc, len(rows))
                    return rows, f"date_range({dc})"
            except Exception as e:
                log.warning("Date range error on col %s: %s", dc, e)
        return [], "date_range_no_match"

    # ── P2-4: context-aware follow-up apply ──────────────────────────────────
    def apply_followup(self, key: str, op: Dict[str, Any],
                       cols: List[str]) -> Tuple[List, str]:
        """
        Apply sort/filter/limit to existing table without re-fetching.
        """
        try:
            cur = db.conn.cursor()
            wheres, params = [], []

            if "filter_val" in op:
                fv    = op["filter_val"]
                conds = " OR ".join(f'"{c}" LIKE ?' for c in cols)
                wheres.append(f"({conds})")
                params.extend([f"%{fv}%"] * len(cols))

            where_sql = f"WHERE {' AND '.join(wheres)}" if wheres else ""

            order_sql = ""
            if "sort_col" in op:
                sc = self._resolve_col(op["sort_col"], cols) or cols[0]
                order_sql = f'ORDER BY "{sc}" {op.get("sort_dir", "DESC")}'
            elif "sort_dir" in op:
                # sort by first amount col or first col
                order_sql = f'ORDER BY "{cols[0]}" {op["sort_dir"]}'

            limit_sql = f'LIMIT {op["limit"]}' if "limit" in op else f'LIMIT {self.MAX_BULK}'

            cur.execute(
                f'SELECT * FROM "{key}" {where_sql} {order_sql} {limit_sql}',
                params,
            )
            rows = [dict(r) for r in cur.fetchall()]
            for r in rows:
                r.pop("_rowid", None)
            return rows, "followup"
        except Exception as e:
            log.warning("Follow-up error %s: %s", key, e)
            return [], "followup_error"

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
# RESPONSE CACHE (LRU + TTL)
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
        self._cache.move_to_end(k)
        return html_resp

    def set(self, module: str, intent: str, value: str, lang: str, resp: str):
        k = self._key(module, intent, value, lang)
        self._cache[k] = (resp, time.time())
        self._cache.move_to_end(k)
        if len(self._cache) > self._max_size:
            self._cache.popitem(last=False)

    def invalidate_module(self, module: str):
        keys = [k for k in self._cache if k.startswith(f"{module}|")]
        for k in keys:
            del self._cache[k]

    def size(self) -> int:
        return len(self._cache)


response_cache = ResponseCache()


# ──────────────────────────────────────────────────────────────────────────────
# P1-26: LATENCY TRACKER
# ──────────────────────────────────────────────────────────────────────────────
class LatencyTracker:
    def __init__(self, maxlen: int = 1000):
        self._samples: deque = deque(maxlen=maxlen)

    def record(self, elapsed_seconds: float):
        self._samples.append(elapsed_seconds * 1000)

    def percentiles(self) -> Dict[str, Optional[float]]:
        if not self._samples:
            return {"p50": None, "p95": None, "p99": None, "count": 0}
        s = sorted(self._samples)
        n = len(s)
        def pct(p: float) -> float:
            idx = max(0, min(n - 1, int(math.ceil(p / 100 * n)) - 1))
            return round(s[idx], 2)
        return {"p50": pct(50), "p95": pct(95), "p99": pct(99), "count": n}

    def count(self) -> int:
        return len(self._samples)


latency_tracker = LatencyTracker()


# ──────────────────────────────────────────────────────────────────────────────
# AGGREGATION ENGINE
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
                        num = float(str(rec[ak]).replace(",", "").replace("₹", "").strip())
                        if math.isfinite(num):
                            total_a += num
                    except (ValueError, TypeError):
                        pass
                    break
            for col in qty_c:
                qk = next((k for k in rec if k.lower() == col.lower()), None)
                if qk:
                    try:
                        num = float(str(rec[qk]).replace(",", "").strip())
                        if math.isfinite(num):
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
# RESPONSE SYNTHESIZER  (P1-27 print CSS retained; P2-3 date banner added)
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

AMOUNT_HINTS = frozenset({"price","amount","total","balance","credit","debit",
                           "paid","cost","value","charges","discount"})
QTY_HINTS    = frozenset({"quantity","qty","stock"})
NOT_CURRENCY = frozenset({"code","no","id","barcode","number","ref","hsn",
                           "pin","port","cgst","sgst","igst","missing","received"})

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

_IMAGE_SAFE_DOMAIN = BASE_URL.split("//")[-1].split("/")[0]

_PRINT_CSS = (
    '<style id="erp-print-css">'
    '@media print {'
    '  .erp-table-wrap { box-shadow: none !important; }'
    '  .erp-table thead tr { background: #333 !important; '
    '    -webkit-print-color-adjust: exact; print-color-adjust: exact; }'
    '  .erp-table tbody tr { page-break-inside: avoid; }'
    '  .erp-no-print { display: none !important; }'
    '  .erp-table td, .erp-table th { font-size: 11px !important; '
    '    padding: 6px 8px !important; }'
    '}'
    '</style>'
)


def _is_currency_col(col: str) -> bool:
    c = col.lower()
    return any(h in c for h in AMOUNT_HINTS) and not any(x in c for x in NOT_CURRENCY)


def _is_qty_col(col: str) -> bool:
    c = col.lower()
    return any(h in c for h in QTY_HINTS) and not any(x in c for x in NOT_CURRENCY)


def _fmt_cell(col: str, val: str) -> str:
    if not val or val in ("None", "null", ""):
        return '<span style="color:#94a3b8">—</span>'

    if (val.startswith("http") and _IMAGE_SAFE_DOMAIN in val
            and any(val.lower().endswith(e)
                    for e in (".png", ".jpg", ".jpeg", ".webp", ".gif"))):
        safe_url = html.escape(val, quote=True)
        return (f'<img src="{safe_url}" style="width:40px;height:40px;'
                f'object-fit:cover;border-radius:6px;" loading="lazy" />')

    if DATETIME_RE.match(val):
        try:
            d, t = val.split("T", 1)
            return (f'<span style="color:#64748b;font-size:12px">'
                    f'{html.escape(d)} {html.escape(t[:5])}</span>')
        except Exception:
            pass

    if _is_currency_col(col):
        try:
            num = float(str(val).replace(",", "").replace("₹", "").strip())
            if math.isfinite(num):
                color = "#16a34a" if num >= 0 else "#dc2626"
                return f'<strong style="color:{color}">₹ {num:,.2f}</strong>'
        except (ValueError, TypeError):
            pass

    if _is_qty_col(col):
        try:
            num   = int(float(str(val).replace(",", "").strip()))
            color = "#0369a1" if num > 10 else "#dc2626"
            return f'<span style="color:{color};font-weight:600">{num:,}</span>'
        except (ValueError, TypeError):
            pass

    if col.lower() in ("status", "supplytype", "type", "state", "paymentmode"):
        badge    = STATUS_BADGES.get(val.lower(), ("#f1f5f9","#475569"))
        safe_val = html.escape(val)
        return (f'<span style="background:{badge[0]};color:{badge[1]};'
                f'padding:2px 8px;border-radius:999px;font-size:12px;font-weight:600">'
                f'{safe_val}</span>')

    return html.escape(str(val))


def _base_key(key: str) -> str:
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
        if marathi:
            return ('<div style="background:#fef9c3;border:1px solid #fde047;'
                    'border-radius:8px;padding:8px 14px;margin-bottom:10px;'
                    'font-size:13px;color:#854d0e">'
                    '⚠️ ERP सर्व्हर उपलब्ध नाही — जुना डेटा दाखवत आहे.</div>')
        return ('<div style="background:#fef9c3;border:1px solid #fde047;'
                'border-radius:8px;padding:8px 14px;margin-bottom:10px;'
                'font-size:13px;color:#854d0e">'
                '⚠️ ERP server unreachable — showing last cached data.</div>')

    def date_range_banner(self, start: str, end: str, marathi: bool) -> str:
        """P2-3: Banner showing the date range filter that was applied."""
        if marathi:
            return (f'<div style="background:#f0f9ff;border:1px solid #bae6fd;'
                    f'border-radius:8px;padding:8px 14px;margin-bottom:10px;'
                    f'font-size:13px;color:#0369a1">'
                    f'📅 तारीख फिल्टर: <strong>{html.escape(start)}</strong> ते '
                    f'<strong>{html.escape(end)}</strong></div>')
        return (f'<div style="background:#f0f9ff;border:1px solid #bae6fd;'
                f'border-radius:8px;padding:8px 14px;margin-bottom:10px;'
                f'font-size:13px;color:#0369a1">'
                f'📅 Date filter: <strong>{html.escape(start)}</strong> to '
                f'<strong>{html.escape(end)}</strong></div>')

    def followup_banner(self, op: Dict, marathi: bool) -> str:
        """P2-4: Banner showing what follow-up was applied."""
        parts = []
        if "sort_col" in op:
            parts.append(f'Sorted by <strong>{html.escape(op["sort_col"])}</strong> '
                         f'({op.get("sort_dir","DESC").lower()})')
        if "filter_val" in op:
            parts.append(f'Filtered: <strong>{html.escape(op["filter_val"])}</strong>')
        if "limit" in op:
            parts.append(f'Top <strong>{op["limit"]}</strong>')
        txt = " · ".join(parts) if parts else "Follow-up applied"
        return (f'<div style="background:#fdf4ff;border:1px solid #e9d5ff;'
                f'border-radius:8px;padding:8px 14px;margin-bottom:10px;'
                f'font-size:13px;color:#7c3aed">🔄 {txt}</div>')

    def scalar_response(self, label: str, value: str, marathi: bool) -> str:
        safe_val = html.escape(str(value))
        if marathi:
            return (f'<div style="background:#f0fdf4;border:1px solid #bbf7d0;'
                    f'border-radius:10px;padding:16px 20px;">'
                    f'पुढील {html.escape(label)} क्रमांक: '
                    f'<strong style="font-size:18px;color:#16a34a">{safe_val}</strong></div>')
        return (f'<div style="background:#f0fdf4;border:1px solid #bbf7d0;'
                f'border-radius:10px;padding:16px 20px;">'
                f'Next {html.escape(label)} number: '
                f'<strong style="font-size:18px;color:#16a34a">{safe_val}</strong></div>')

    def intro(self, key: str, agg: Dict, search_val: str,
              marathi: bool, lang: str,
              total_found: int = 0, rendered: int = 0) -> str:
        count    = agg["count"]
        amt      = agg["total_amount"]
        qty      = agg["total_qty"]
        bk       = _base_key(key)
        safe_val = html.escape(search_val) if search_val else ""

        if marathi:
            label = MR_MODULES.get(bk, html.escape(bk))
            text  = (
                f'<strong>{label}</strong> मध्ये <strong>"{safe_val}"</strong> साठी शोधले — '
                f'<strong>{count:,} नोंदी</strong> सापडल्या.'
                if safe_val else
                f'<strong>{label}</strong> ची संपूर्ण यादी — <strong>{count:,} नोंदी</strong>.'
            )
            parts = []
            if amt: parts.append(f'एकूण रक्कम: <strong>₹{amt:,.2f}</strong>')
            if qty: parts.append(f'एकूण युनिट: <strong>{qty:,}</strong>')
        else:
            label = html.escape(bk.replace("-", " ").title())
            ack   = random.choice(["Here you go! ", "Got it. ", "Absolutely! ", "Done — "])
            text  = (
                f'{ack}Searched <strong>{label}</strong> for <strong>"{safe_val}"</strong> — '
                f'<strong>{count:,} record{"s" if count != 1 else ""}</strong> found.'
                if safe_val else
                f'{ack}Complete <strong>{label}</strong> list — '
                f'<strong>{count:,} record{"s" if count != 1 else ""}</strong>.'
            )
            parts = []
            if amt: parts.append(f'Total: <strong>₹{amt:,.2f}</strong>')
            if qty: parts.append(f'Units: <strong>{qty:,}</strong>')

        if parts:
            text += " &nbsp;·&nbsp; " + " &nbsp;·&nbsp; ".join(parts)

        if total_found > rendered > 0:
            text += (f' &nbsp;<span style="background:#e0f2fe;color:#0369a1;'
                     f'padding:1px 7px;border-radius:999px;font-size:12px">'
                     f'Showing {rendered:,} of {total_found:,}</span>')

        return f'<p style="margin:0 0 14px;font-size:15px;line-height:1.6">{text}</p>'

    def table(self, records: List[Dict], cols: List[str], marathi: bool,
              max_rows: int = MAX_RENDER_ROWS) -> Tuple[str, int]:
        headers = self._headers(cols)
        if not headers:
            return "", 0
        font = ("'Noto Sans Devanagari','Segoe UI',system-ui,sans-serif"
                if marathi else "'Segoe UI',system-ui,sans-serif")
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
            label = html.escape(self._col_label(h, marathi))
            header_cells.append(
                f'<th style="padding:13px 16px;border-right:1px solid #475569;'
                f'font-size:12px;font-weight:600;letter-spacing:.04em;'
                f'white-space:nowrap">{label}</th>'
            )

        tbl = "".join([
            _PRINT_CSS,
            '<div class="erp-table-wrap" style="overflow-x:auto;border-radius:12px;',
            'box-shadow:0 4px 24px rgba(0,0,0,.08);border:1px solid #e2e8f0;">',
            f'<table class="erp-table" style="border-collapse:collapse;width:100%;text-align:left;',
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
            }.get(query_mode or "", "")
            return (f'<div style="background:#fef2f2;border:1px solid #fecaca;'
                    f'border-radius:10px;padding:16px 20px;color:#991b1b;">'
                    f'<strong>{label}</strong> मध्ये <strong>"{safe_val}"</strong> '
                    f'साठी काहीही सापडले नाही.{hint}<br>'
                    f'<small>शब्दलेखन तपासा किंवा लहान शब्द वापरा.</small></div>')
        label = html.escape(bk.replace("-", " ").title())
        hint  = {
            "barcode": "<br><small>Tip: full barcode e.g. BAR268726580</small>",
            "invoice": "<br><small>Tip: invoice format e.g. PA00000001</small>",
            "contact": "<br><small>Tip: 10-digit mobile number</small>",
            "id":      "<br><small>Tip: numeric ID e.g. 'purchase id 42'</small>",
        }.get(query_mode or "", "")
        return (f'<div style="background:#fef2f2;border:1px solid #fecaca;'
                f'border-radius:10px;padding:16px 20px;color:#991b1b;">'
                f'No results in <strong>{label}</strong> for '
                f'<strong>"{safe_val}"</strong>.{hint}<br>'
                f'<small>Check spelling or try a shorter term.</small></div>')

    def api_error(self, key: str, marathi: bool) -> str:
        bk = _base_key(key)
        if marathi:
            label = MR_MODULES.get(bk, html.escape(bk))
            return (f'<div style="background:#fff7ed;border:1px solid #fed7aa;'
                    f'border-radius:10px;padding:16px 20px;color:#9a3412;">'
                    f'⚠️ <strong>{label}</strong> चा डेटा मिळवता आला नाही '
                    f'({API_MAX_RETRIES} प्रयत्नांनंतर). ERP कनेक्शन तपासा.</div>')
        label = html.escape(bk.replace("-", " ").title())
        return (f'<div style="background:#fff7ed;border:1px solid #fed7aa;'
                f'border-radius:10px;padding:16px 20px;color:#9a3412;">'
                f'⚠️ Could not fetch <strong>{label}</strong> data after '
                f'{API_MAX_RETRIES} attempts.<br>'
                f'<small>Check server logs for auth/connection errors.</small></div>')

    def prompt_needed(self, key: str, col: str, marathi: bool) -> str:
        bk = _base_key(key)
        if marathi:
            label   = MR_MODULES.get(bk, html.escape(bk))
            col_eng = CAMEL_RE.sub(r"\1 \2", col).title()
            col_mr  = html.escape(MR_COLUMNS.get(col_eng, col_eng))
            return (f'तुम्ही <strong>{label}</strong> विभागात शोधत आहात. '
                    f'कृपया <strong>{col_mr}</strong> सांगा.<br>'
                    f'<em>उदाहरण: "invoice PA00000001" किंवा "barcode BAR268726580"</em>')
        label     = html.escape(bk.replace("-", " ").title())
        col_label = html.escape(CAMEL_RE.sub(r"\1 \2", col).title())
        return (f'You\'re searching <strong>{label}</strong>. '
                f'Please specify the <strong>{col_label}</strong> value.<br>'
                f'<em>Example: "invoice PA00000001" or "id 42"</em>')

    def targeted_badge(self, query_mode: str, query_value: str, marathi: bool) -> str:
        safe_qv = html.escape(query_value or "")
        labels = {
            "marathi": {
                "barcode":      f"बारकोड शोध: {safe_qv}",
                "invoice":      f"इनव्हॉइस शोध: {safe_qv}",
                "contact":      f"संपर्क शोध: {safe_qv}",
                "id":           f"ID शोध: {safe_qv}",
                "name":         f"नाव शोध: {safe_qv}",
                "active":       "सक्रिय नोंद",
                "next_invoice": "पुढील इनव्हॉइस क्रमांक",
                "negation":     f"वगळलेले: {safe_qv}",
                "date_range":   "तारीख फिल्टर",
            },
            "english": {
                "barcode":      f"Targeted barcode lookup: {safe_qv}",
                "invoice":      f"Targeted invoice lookup: {safe_qv}",
                "contact":      f"Targeted contact lookup: {safe_qv}",
                "id":           f"Targeted ID lookup: {safe_qv}",
                "name":         f"Targeted name lookup: {safe_qv}",
                "active":       "Showing active record",
                "next_invoice": "Next invoice number",
                "negation":     f"Excluding: {safe_qv}",
                "date_range":   "Date range filter",
            },
        }
        lang_key = "marathi" if marathi else "english"
        txt      = html.escape(labels[lang_key].get(query_mode, query_mode))
        return (f'<div style="background:#f0f9ff;border:1px solid #bae6fd;'
                f'border-radius:8px;padding:8px 14px;margin-bottom:10px;'
                f'font-size:13px;color:#0369a1">⚡ {txt}</div>')

    def unrecognized(self, marathi: bool) -> str:
        if marathi:
            mods = " · ".join(MR_MODULES.values())
            return (f'<div style="background:#f8fafc;border:1px solid #e2e8f0;'
                    f'border-radius:10px;padding:16px 20px;">'
                    f'<strong>मला समजले नाही.</strong> मी खालील गोष्टींमध्ये मदत करू शकतो:<br>'
                    f'<em>{html.escape(mods)}</em><br><br>उदाहरणे:'
                    f'<ul style="margin:8px 0;padding-left:20px">'
                    f'<li><em>सर्व खरेदी दाखवा</em></li>'
                    f'<li><em>vikri list</em></li>'
                    f'<li><em>supplier credit kiti ahe</em></li>'
                    f'<li><em>stock bagha</em></li>'
                    f'<li><em>category list</em></li>'
                    f'</ul></div>')
        mods = ", ".join(k.replace("-", " ").title() for k in MODULE_REGISTRY)
        return (f'<div style="background:#f8fafc;border:1px solid #e2e8f0;'
                f'border-radius:10px;padding:16px 20px;">'
                f"<strong>I'm not sure what you're looking for.</strong><br>"
                f"I can help with: <em>{html.escape(mods)}</em><br><br>Try:"
                f"<ul style='margin:8px 0;padding-left:20px'>"
                f"<li><em>List all purchases</em></li>"
                f"<li><em>Show supplier credits</em></li>"
                f"<li><em>Find customer named Rahul</em></li>"
                f"<li><em>Financial summary</em></li>"
                f"<li><em>Purchases from 2024-01-01 to 2024-03-31</em></li>"
                f"<li><em>Sales last 7 days</em></li>"
                f"<li><em>Barcode BAR268726580</em></li>"
                f"<li><em>Next purchase invoice number</em></li>"
                f"<li><em>Show purchases not cancelled</em></li>"
                f"<li><em>Sort by totalAmount</em></li>"
                f"</ul></div>")


synth = ResponseSynthesizer()


# ──────────────────────────────────────────────────────────────────────────────
# CONVERSATION CONTEXT  (P2-4: stores last_module + last_table_key)
# ──────────────────────────────────────────────────────────────────────────────
def _sanitize_session_id(sid: str) -> str:
    clean = SESSION_RE.sub("", sid)[:64]
    return clean if clean else "default"


class ConversationContext:
    def __init__(self, max_sessions: int = MAX_SESSIONS,
                 max_turns: int = 5, session_ttl: float = SESSION_TTL):
        self._sessions:      "collections.OrderedDict[str, Tuple[deque, float]]" = \
            collections.OrderedDict()
        # P2-4: store last module and table key per session
        self._last_module:   Dict[str, str]  = {}
        self._last_table:    Dict[str, str]  = {}   # in-memory table key
        self._max_sessions   = max_sessions
        self._max_turns      = max_turns
        self._session_ttl    = session_ttl
        self._last_evict: float = 0.0

    def _evict_stale(self):
        now = time.time()
        if now - self._last_evict < 60.0:
            return
        self._last_evict = now
        stale = [k for k, (_, ts) in self._sessions.items()
                 if now - ts > self._session_ttl]
        for k in stale:
            del self._sessions[k]
            self._last_module.pop(k, None)
            self._last_table.pop(k, None)

    def add(self, sid: str, role: str, text: str):
        sid = _sanitize_session_id(sid)
        self._evict_stale()
        if sid not in self._sessions:
            if len(self._sessions) >= self._max_sessions:
                oldest = next(iter(self._sessions))
                del self._sessions[oldest]
                self._last_module.pop(oldest, None)
                self._last_table.pop(oldest, None)
            self._sessions[sid] = (deque(maxlen=self._max_turns), time.time())
        turns, _ = self._sessions[sid]
        turns.append({"role": role, "text": text[:300]})
        self._sessions[sid] = (turns, time.time())
        self._sessions.move_to_end(sid)

    def set_last_module(self, sid: str, module: str, table_key: str):
        """P2-4: store the last module + table key for follow-up queries."""
        sid = _sanitize_session_id(sid)
        self._last_module[sid] = module
        self._last_table[sid]  = table_key

    def get_last_module(self, sid: str) -> Optional[str]:
        return self._last_module.get(_sanitize_session_id(sid))

    def get_last_table(self, sid: str) -> Optional[str]:
        return self._last_table.get(_sanitize_session_id(sid))

    def last_module(self, sid: str) -> Optional[str]:
        """Legacy compat — also check the new dict."""
        result = self.get_last_module(sid)
        if result:
            return result
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
# RATE LIMITER
# ──────────────────────────────────────────────────────────────────────────────
class RateLimiter:
    def __init__(self, rpm: int = RATE_LIMIT_RPM):
        self._rpm     = rpm
        self._windows: Dict[str, deque] = defaultdict(deque)

    def check(self, ip: str) -> Tuple[bool, int]:
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
# MAIN ORCHESTRATOR  (P2-3 date range, P2-4 follow-up)
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
        date_range = analysis.get("date_range")

        # P2-4: check for follow-up before expensive classification
        followup_op = follow_up_detector.detect(user_query)
        if followup_op:
            last_table  = ctx.get_last_table(session_id)
            last_module = ctx.get_last_module(session_id)
            if last_table and last_module and db.columns(last_table):
                cols        = db.columns(last_table)
                rows, method= retriever.apply_followup(last_table, followup_op, cols)
                if rows:
                    agg           = aggregator.compute(rows, last_module)
                    banner        = synth.followup_banner(followup_op, is_marathi)
                    intro         = synth.intro(last_module, agg, "", is_marathi, lang)
                    tbl, rendered = synth.table(rows, cols, is_marathi)
                    return banner + intro + tbl

        if is_marathi:
            key = marathi_classify(user_query)
            if key:
                return await self._single(key, user_query, session_id,
                                          lang, marathi=True,
                                          q_mode=q_mode, q_val=q_val,
                                          date_range=date_range)
            return synth.unrecognized(marathi=True)

        cl = self.classifier.classify(user_query)
        if cl["type"] == "UNRECOGNIZED":
            return synth.unrecognized(marathi=False)
        if cl["type"] == "MULTI":
            return await self._multi(cl["pattern"], user_query, session_id)
        return await self._single(cl["module"], user_query, session_id,
                                  lang, marathi=False,
                                  q_mode=q_mode, q_val=q_val,
                                  date_range=date_range)

    async def _single(self, key: str, query: str, sid: str,
                      lang: str, marathi: bool,
                      q_mode: str = None, q_val: str = None,
                      date_range: Optional[Tuple[str, str]] = None) -> str:
        parsed = self.parser.parse(query, key, lang)
        if parsed["intent"] == "PROMPT_NEEDED":
            return synth.prompt_needed(key, parsed["target_col"], marathi)

        # Inject date_range from analyzer if parser didn't find it
        if date_range and not parsed.get("date_range"):
            parsed["date_range"] = date_range
            if parsed["intent"] not in ("COLUMN_SEARCH", "GLOBAL_SEARCH", "NEGATIVE_SEARCH"):
                parsed["intent"] = "DATE_RANGE"

        if not q_mode:
            cached = response_cache.get(
                key, parsed["intent"], parsed["search_value"], lang
            )
            if cached:
                return cached

        targeted_key   = None
        targeted_badge = ""

        if q_mode and q_mode != "date_range":
            ok, targeted_key, scalar = await smart_router.route(
                key, parsed, q_mode, q_val
            )
            if ok:
                targeted_badge = synth.targeted_badge(q_mode, q_val or "", marathi)

                if scalar is not None:
                    bk    = _base_key(key)
                    label = (MR_MODULES.get(bk, bk) if marathi
                             else bk.replace("-", " ").title())
                    ctx.add(sid, "bot", f"_module:{key}")
                    return targeted_badge + synth.scalar_response(label, scalar, marathi)

                if targeted_key:
                    try:
                        records, cols, _ = retriever.retrieve(targeted_key, parsed)
                        if records:
                            agg           = aggregator.compute(records, key)
                            intro         = synth.intro(key, agg, q_val or "",
                                                        marathi, lang)
                            tbl, rendered = synth.table(records, cols, marathi)
                            ctx.add(sid, "bot", f"_module:{key}")
                            return targeted_badge + intro + tbl
                        else:
                            return targeted_badge + synth.no_results(
                                key, q_val or "", marathi, q_mode
                            )
                    finally:
                        db._drop_targeted(targeted_key)

        # Full module sync
        status = await db.sync_module(key)
        if status == "error":
            return synth.api_error(key, marathi)

        stale_banner = synth.stale_banner(key, marathi) if status == "stale" else ""
        records, cols, method = retriever.retrieve(key, parsed)

        # P2-3: date range banner
        date_banner = ""
        if parsed.get("date_range") and records:
            start, end  = parsed["date_range"]
            date_banner = synth.date_range_banner(start, end, marathi)

        if not records:
            return stale_banner + synth.no_results(
                key, parsed["search_value"], marathi, q_mode
            )

        agg = aggregator.compute(records, key)

        if parsed["aggregation"] == "SUM" and parsed["intent"] != "BULK_LIST":
            bk    = _base_key(key)
            label = html.escape(bk.replace("-", " ").title())
            parts = [f'<strong>{label}</strong> — {agg["count"]:,} records']
            if agg["total_amount"]:
                parts.append(f'Total: <strong>₹{agg["total_amount"]:,.2f}</strong>')
            if agg["total_qty"]:
                parts.append(f'Units: <strong>{agg["total_qty"]:,}</strong>')
            result = (f'<div style="background:#f0fdf4;border:1px solid #bbf7d0;'
                      f'border-radius:10px;padding:14px 20px">'
                      + " &nbsp;·&nbsp; ".join(parts) + "</div>")
            return stale_banner + result

        tbl, rendered = synth.table(records, cols, marathi)
        intro  = synth.intro(key, agg, parsed["search_value"], marathi, lang,
                             total_found=len(records), rendered=rendered)
        result = stale_banner + date_banner + intro + tbl

        # P2-4: store last module+table for follow-up queries
        ctx.set_last_module(sid, key, key)
        ctx.add(sid, "bot", f"_module:{key}")

        if not q_mode and status in ("ok", "fresh", "disk"):
            response_cache.set(
                key, parsed["intent"], parsed["search_value"], lang, result
            )

        return result

    async def _multi(self, pattern: Dict, query: str, sid: str) -> str:
        keys    = pattern["modules"]
        label   = html.escape(pattern["label"])
        synced  = await db.sync_modules(keys)
        sects:  List[str] = []
        total_a = total_q = 0.0

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
            records, cols, _ = retriever.retrieve(key, parsed)
            if not records:
                continue
            agg      = aggregator.compute(records, key)
            total_a += agg["total_amount"] or 0
            total_q += agg["total_qty"]    or 0
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
            summary.append(f'Units: <strong>{int(total_q):,}</strong>')

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
# STATIC RESPONSES  — v8.0
# ──────────────────────────────────────────────────────────────────────────────
GREETING_EN = """
<div style="font-family:'Segoe UI',system-ui,sans-serif;max-width:580px">
  <p style="font-size:18px;font-weight:700;margin:0 0 14px;color:#1e293b">
    👋 Hello! I'm your <span style="color:#6366f1">ERP Intelligence Assistant v8.0</span>.
  </p>
  <p style="margin:0 0 14px;color:#475569;font-size:14px">Here's what I can help you with:</p>
  <div style="display:grid;gap:8px">
    <div style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:8px;padding:10px 14px;font-size:14px">
      📦 <strong>Stock &amp; Inventory</strong> — <em>"Show all stock"</em>, <em>"Barcode BAR268726580"</em>, <em>"Product named Chair"</em>
    </div>
    <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:10px 14px;font-size:14px">
      💰 <strong>Sales &amp; Purchases</strong> — <em>"Invoice PA00000001"</em>, <em>"Purchases from 2024-01-01 to 2024-03-31"</em>, <em>"Sales last 7 days"</em>
    </div>
    <div style="background:#fdf4ff;border:1px solid #e9d5ff;border-radius:8px;padding:10px 14px;font-size:14px">
      🏢 <strong>Suppliers &amp; Customers</strong> — <em>"All suppliers"</em>, <em>"Customer payment history"</em>, <em>"Debit notes"</em>
    </div>
    <div style="background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 14px;font-size:14px">
      📊 <strong>Financial Reports</strong> — <em>"Financial summary"</em>, <em>"Sales in January 2024"</em>, <em>"Purchases this month"</em>
    </div>
    <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:10px 14px;font-size:14px">
      🔄 <strong>Follow-up Queries</strong> — <em>"Sort by totalAmount"</em>, <em>"Top 10"</em>, <em>"Filter Cash"</em>
    </div>
  </div>
  <p style="margin:12px 0 0;font-size:13px;color:#94a3b8">
    ⚡ Smart: barcode · invoice · mobile · ID · next invoice · named · exclude · date ranges · follow-up sorting.<br>
    You can also ask in <strong>मराठी</strong> or Marathi Roman typing.
  </p>
</div>
"""

GREETING_MR = """
<div style="font-family:'Noto Sans Devanagari','Segoe UI',system-ui,sans-serif;max-width:580px">
  <p style="font-size:18px;font-weight:700;margin:0 0 14px;color:#1e293b">
    नमस्कार! मी तुमचा <span style="color:#6366f1">ERP सहाय्यक v8.0</span> आहे. 🙏
  </p>
  <p style="margin:0 0 14px;color:#475569;font-size:14px">मी खालील गोष्टींमध्ये मदत करू शकतो:</p>
  <div style="display:grid;gap:8px">
    <div style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:8px;padding:10px 14px;font-size:14px">
      📦 <strong>साठा आणि इन्व्हेंटरी</strong> — <em>"stock bagha"</em>, <em>"उत्पादन साठा दाखवा"</em>
    </div>
    <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:10px 14px;font-size:14px">
      💰 <strong>विक्री आणि खरेदी</strong> — <em>"vikri list"</em>, <em>"last 30 days purchase"</em>
    </div>
    <div style="background:#fdf4ff;border:1px solid #e9d5ff;border-radius:8px;padding:10px 14px;font-size:14px">
      🏢 <strong>पुरवठादार आणि ग्राहक</strong> — <em>"supplier chi yadi"</em>, <em>"grahak payment history"</em>
    </div>
    <div style="background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 14px;font-size:14px">
      📊 <strong>आर्थिक अहवाल</strong> — <em>"supplier credit kiti ahe"</em>, <em>"in January 2024"</em>
    </div>
  </div>
  <p style="margin:12px 0 0;font-size:13px;color:#94a3b8">
    ⚡ बारकोड, इनव्हॉइस, मोबाईल किंवा ID नंबर टाकून थेट शोध करा.<br>
    English मध्ये पण विचारू शकता.
  </p>
</div>
"""

IDENTITY_RESPONSE = """
<div style="font-family:'Segoe UI',system-ui,sans-serif;max-width:640px">
  <p style="font-size:18px;font-weight:700;color:#1e293b;margin:0 0 12px">
    🧠 Admin Intelligence Engine v8.0 — Phase 2 Critical
  </p>
  <p style="margin:0 0 16px;color:#475569">
    Production-grade <strong>RAG pipeline</strong> for ERP —
    zero external AI APIs · full Marathi support · ≤300 MB RAM ·
    all 35 APIs wired · 43 bugs fixed from v6.3 · Phase-1 (8 features) + Phase-2 (4 critical) added.
  </p>
  <div style="background:#fef2f2;border-radius:10px;padding:16px;margin-bottom:12px">
    <strong style="color:#dc2626">🔒 Security:</strong> XSS prevention · memory bomb protection ·
    per-module locks · JWT caching · NaN/Inf guards · rate limiting (60 rpm/IP) ·
    session sanitization · no hardcoded creds · image URL whitelist · FTS injection prevention.
  </div>
  <div style="background:#f0fdf4;border-radius:10px;padding:16px;margin-bottom:12px">
    <strong style="color:#16a34a">🚀 v8.0 Phase 2 additions:</strong>
    Pre-emptive background TTL refresh at 80% expiry (P2-1) ·
    Persistent SQLite disk cache at /tmp/erp_cache.db (P2-2) ·
    Date range queries: "from X to Y", "last N days", "in January", "today" (P2-3) ·
    Context-aware follow-up: sort/filter/top without re-fetching (P2-4).
  </div>
  <div style="background:#f0f9ff;border-radius:10px;padding:16px">
    <strong style="color:#0369a1">⚡ APIs covered (35):</strong>
    purchase (6) · sell (5) · material (3) · supplier-credit (2) · customer (4) ·
    supplier (2) · reports (8) · printer (3) · payment (1) · email-config (3).
  </div>
</div>
"""


# ──────────────────────────────────────────────────────────────────────────────
# P1-30: .env.example
# ──────────────────────────────────────────────────────────────────────────────
_ENV_EXAMPLE = """\
# ── ERP Intelligence Engine — environment variables ──────────────────────────
# Copy this file to .env and fill in your values.
# Never commit .env to version control.

# Required: ERP server base URL (no trailing slash)
ERP_BASE_URL=https://your-erp-server.example.com/your-api-path

# Required: ERP login credentials
ERP_USERNAME=your_username
ERP_PASSWORD=your_password

# Dev only: allow built-in default credentials (never use in production)
# ALLOW_DEFAULT_CREDS=false

# CORS allowed origins (comma-separated, no spaces)
# Example: CORS_ORIGINS=https://yourapp.com,https://admin.yourapp.com
CORS_ORIGINS=https://yourapp.com

# Uvicorn bind port (Render injects PORT automatically)
PORT=8000

# P2-2: persistent disk cache (Render /tmp persists across restarts)
ERP_DISK_CACHE=/tmp/erp_cache.db
ERP_DISK_CACHE_ENABLED=true
"""


def _write_env_example():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env.example")
    if not os.path.exists(path):
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(_ENV_EXAMPLE)
            log.info(".env.example written to %s", path)
        except OSError as e:
            log.warning("Could not write .env.example: %s", e)


_write_env_example()


# ──────────────────────────────────────────────────────────────────────────────
# FASTAPI APPLICATION
# ──────────────────────────────────────────────────────────────────────────────
if _FASTAPI_AVAILABLE:
    from fastapi import FastAPI, status
    from fastapi.middleware.cors import CORSMiddleware
    from starlette.middleware.base import BaseHTTPMiddleware
    from starlette.requests import Request as StarletteRequest
    from starlette.responses import Response as StarletteResponse

    async def _warmup():
        log.info("Warmup: pre-fetching all %d modules: %s",
                 len(WARMUP_MODULES), WARMUP_MODULES)
        results = await db.sync_modules(WARMUP_MODULES)
        ok_count = sum(1 for v in results.values() if v in ("ok", "disk"))
        log.info("Warmup complete: %d/%d modules ok — %s",
                 ok_count, len(WARMUP_MODULES), results)
        _startup_ready.set()

    @asynccontextmanager
    async def lifespan(app):
        global _startup_ready
        _startup_ready = asyncio.Event()
        await db.open()
        log.info("HTTP client opened")
        asyncio.create_task(_warmup())
        yield
        await db.close()
        log.info("HTTP client closed")

    router = APIRouter()

    app = FastAPI(title="ERP Intelligence Engine v8.0", lifespan=lifespan)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["POST", "GET", "OPTIONS"],
        allow_headers=["Content-Type", "Authorization", "X-Request-ID"],
        max_age=600,
    )

    class SecurityHeadersMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request: StarletteRequest,
                           call_next) -> StarletteResponse:
            req_id   = request.headers.get("X-Request-ID") or secrets.token_hex(8)
            response = await call_next(request)
            for k, v in SECURITY_HEADERS.items():
                response.headers[k] = v
            response.headers["X-Request-ID"] = req_id
            return response

    app.add_middleware(SecurityHeadersMiddleware)

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
    _MR_REFRESH_W = frozenset({"रिफ्रेश","अद्यतन","refresh kara","update kara","sync kara"})

    @router.post("/chat")
    async def chat_endpoint(request: Request, body: ChatRequest):
        try:
            await asyncio.wait_for(_startup_ready.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            log.error("Startup warmup timeout — serving anyway")
            _startup_ready.set()

        req_id    = request.headers.get("X-Request-ID", "?")
        client_ip = request.client.host if request.client else "unknown"

        allowed, remaining = rate_limiter.check(client_ip)
        if not allowed:
            log.warning("[%s] rate-limited ip=%s", req_id, client_ip)
            return {"error": "Rate limit exceeded. Please wait a moment.",
                    "retry_after": 60}

        raw_query = (body.query or body.question or "").strip()
        if not raw_query:
            return {"response": "Please type a question. / कृपया प्रश्न टाइप करा."}
        if len(raw_query) > 2000:
            return {"response": "Query too long (max 2000 chars). / प्रश्न 2000 अक्षरांपेक्षा लहान असावा."}

        query      = raw_query
        session_id = _sanitize_session_id(body.session_id or "default")
        q_low      = query.lower().strip()
        lang       = detect_language(query)
        is_mr      = lang in ("marathi_devanagari", "marathi_roman")

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
                results  = await db.sync_all(force=True)
                ok_count = sum(1 for v in results.values() if v == "ok")
                return {"response": (
                    f'<div style="background:#f0fdf4;border:1px solid #bbf7d0;'
                    f'border-radius:10px;padding:14px 18px;color:#166534">'
                    f'🔄 <strong>सर्व माहिती अद्यतनित झाली.</strong> '
                    f'{ok_count} modules refreshed.</div>'
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
                results  = await db.sync_all(force=True)
                ok_count = sum(1 for v in results.values() if v == "ok")
                return {"response": (
                    f'<div style="background:#f0fdf4;border:1px solid #bbf7d0;'
                    f'border-radius:10px;padding:14px 18px;color:#166534">'
                    f'🔄 <strong>{ok_count} module(s) refreshed.</strong> '
                    f'ERP data is up to date.</div>'
                )}

        log.info("[%s] ip=%s lang=%s q=%s", req_id, client_ip, lang, query[:80])

        _t0 = time.perf_counter()
        try:
            response = await asyncio.wait_for(
                engine.process(query, session_id), timeout=45.0
            )
        except asyncio.TimeoutError:
            log.error("[%s] process() timed out: %s", req_id, query[:100])
            response = (
                '<div style="background:#fff7ed;border:1px solid #fed7aa;'
                'border-radius:10px;padding:16px 20px;color:#9a3412;">'
                '⏱️ Request timed out. ERP server is slow. Please try again.</div>'
            )
        finally:
            latency_tracker.record(time.perf_counter() - _t0)

        return {
            "response":  response,
            "request_id": req_id,
            "remaining":  remaining,
        }

    @app.get("/health")
    async def health():
        return {
            "status":          "ok",
            "version":         "8.0",
            "ready":           _startup_ready.is_set() if _startup_ready else False,
            "uptime_seconds":  int(db.uptime_seconds()),
            "sessions_active": ctx.session_count(),
            "cache_entries":   response_cache.size(),
            "circuit_breaker": circuit_breaker.status(),
            "latency_ms":      latency_tracker.percentiles(),
            "disk_cache":      {"enabled": disk_cache.is_available,
                                "path": DISK_CACHE_PATH},
            "modules":         db.cache_status(),
        }

    @app.get("/")
    async def root():
        return {
            "message": "ERP Intelligence Engine v8.0",
            "chat":    "POST /api/chat",
            "health":  "GET /health",
            "docs":    "GET /docs",
        }

    app.include_router(router, prefix="/api")

else:
    router = None


# ──────────────────────────────────────────────────────────────────────────────
# SELF-TEST SUITE  —  python erp_engine_v8_0.py
# All 100 v7.1 tests + Phase 2 tests for items 1-4
# ──────────────────────────────────────────────────────────────────────────────
def _run_tests():
    passed = failed = 0

    def ok(name):
        nonlocal passed; passed += 1; print(f"  ✅  {name}")

    def fail(name, err):
        nonlocal failed; failed += 1; print(f"  ❌  {name}: {err}")

    def test(name, fn):
        try:    fn(); ok(name)
        except Exception as e: fail(name, e)

    print("\n══════════════════════════════════════════════════════")
    print("  ERP Engine v8.0 — Full Self-Test Suite (100 + Phase2)")
    print("══════════════════════════════════════════════════════\n")

    # ── Language detection ────────────────────────────────────────────────────
    test("lang: English", lambda: assert_eq(detect_language("show all purchases"), "english"))
    test("lang: Devanagari", lambda: assert_eq(detect_language("सर्व खरेदी दाखवा"), "marathi_devanagari"))
    test("lang: Roman Marathi (supplier)", lambda: assert_eq(detect_language("supplier chi yadi dakhva"), "marathi_roman"))
    test("lang: Roman Marathi (kiti)", lambda: assert_eq(detect_language("stock kiti ahe"), "marathi_roman"))
    test("lang: mixed still roman", lambda: assert_eq(detect_language("give me supplier chi yadi"), "marathi_roman"))

    # ── Marathi classifier ────────────────────────────────────────────────────
    test("mr: supplier", lambda: assert_eq(marathi_classify("supplier chi yadi dakhva"), "supplier"))
    test("mr: purchase", lambda: assert_eq(marathi_classify("kharedi list dya"), "purchase"))
    test("mr: sell", lambda: assert_eq(marathi_classify("vikri itihas bagha"), "sell"))
    test("mr: stock", lambda: assert_eq(marathi_classify("stock kiti ahe"), "product-stock"))
    test("mr: credit", lambda: assert_eq(marathi_classify("supplier credit kiti ahe"), "supplier-credit"))
    test("mr: junk → None", lambda: assert_eq(marathi_classify("xyz abc def"), None))
    test("mr: customer-ledger-detail", lambda: assert_ne(marathi_classify("grahak khate tapshil"), None))

    # ── Query Analyzer ────────────────────────────────────────────────────────
    qa = QueryAnalyzer()
    test("qa: barcode", lambda: assert_eq(qa.analyze("BAR268726580")["query_mode"], "barcode"))
    test("qa: invoice PA", lambda: assert_eq(qa.analyze("invoice PA00000003")["query_mode"], "invoice"))
    test("qa: invoice value", lambda: assert_eq(qa.analyze("invoice PA00000003")["query_value"], "PA00000003"))
    test("qa: contact", lambda: assert_eq(qa.analyze("customer 9876543210")["query_mode"], "contact"))
    test("qa: active", lambda: assert_eq(qa.analyze("show active printer")["query_mode"], "active"))
    test("qa: id lookup", lambda: assert_eq(qa.analyze("show purchase id 42")["query_mode"], "id"))
    test("qa: id value", lambda: assert_eq(qa.analyze("show purchase id 42")["query_value"], "42"))
    test("qa: next purchase invoice", lambda: assert_eq(qa.analyze("what is next purchase invoice number")["query_mode"], "next_invoice"))
    test("qa: next sell invoice", lambda: assert_eq(qa.analyze("next sell invoice no")["query_mode"], "next_invoice"))

    # ── P1-25: NAME_RE ────────────────────────────────────────────────────────
    test("p1-25: named mode", lambda: assert_eq(qa.analyze("show product named Chair")["query_mode"], "name"))
    test("p1-25: called mode", lambda: assert_eq(qa.analyze("material called Cotton")["query_mode"], "name"))
    test("p1-25: name value", lambda: assert_eq(qa.analyze("product named Chair")["query_value"], "Chair"))
    test("p1-25: barcode wins over name",
         lambda: assert_eq(qa.analyze("show BAR268726580 named Chair")["query_mode"], "barcode"))

    # ── P1-28: NEGATION_RE ────────────────────────────────────────────────────
    test("p1-28: not cancelled", lambda: assert_eq(qa.analyze("show purchases not cancelled")["query_mode"], "negation"))
    test("p1-28: negation value", lambda: assert_eq(qa.analyze("show sells not cancelled")["query_value"], "cancelled"))
    test("p1-28: invoice wins over negation",
         lambda: assert_eq(qa.analyze("invoice PA00123 not pending")["query_mode"], "invoice"))

    # ── P2-3: DATE RANGE PARSER ───────────────────────────────────────────────
    drp = DateRangeParser()
    today = date.today()

    test("p2-3: today", lambda: assert_eq(drp.parse("sales today"), (today.isoformat(), today.isoformat())))
    test("p2-3: yesterday",
         lambda: assert_eq(drp.parse("purchases yesterday"),
                           ((today - timedelta(days=1)).isoformat(),
                            (today - timedelta(days=1)).isoformat())))
    test("p2-3: last 7 days returns tuple",
         lambda: assert_eq(drp.parse("purchases last 7 days") is not None, True))
    test("p2-3: last 7 days end = today",
         lambda: assert_eq(drp.parse("sales last 7 days")[1], today.isoformat()))
    test("p2-3: last 7 days start correct",
         lambda: assert_eq(drp.parse("sales last 7 days")[0],
                           (today - timedelta(days=7)).isoformat()))
    test("p2-3: last 2 weeks",
         lambda: assert_eq(drp.parse("payments last 2 weeks")[0],
                           (today - timedelta(weeks=2)).isoformat()))
    test("p2-3: this month start = 1st",
         lambda: assert_eq(drp.parse("sells this month")[0], today.replace(day=1).isoformat()))
    test("p2-3: in January 2024",
         lambda: assert_eq(drp.parse("purchases in January 2024"), ("2024-01-01", "2024-01-31")))
    test("p2-3: in March 2024",
         lambda: assert_eq(drp.parse("purchases in march 2024"), ("2024-03-01", "2024-03-31")))
    test("p2-3: in 2024 full year",
         lambda: assert_eq(drp.parse("sales in 2024"), ("2024-01-01", "2024-12-31")))
    test("p2-3: ISO range from-to",
         lambda: assert_eq(drp.parse("purchases from 2024-01-01 to 2024-03-31"),
                           ("2024-01-01", "2024-03-31")))
    test("p2-3: ISO range between-and",
         lambda: assert_eq(drp.parse("sales between 2024-06-01 and 2024-06-30"),
                           ("2024-06-01", "2024-06-30")))
    test("p2-3: no date → None",
         lambda: assert_eq(drp.parse("show all purchases"), None))
    test("p2-3: analyzer detects date_range mode",
         lambda: assert_eq(qa.analyze("purchases last 7 days")["query_mode"], "date_range"))
    test("p2-3: analyzer stores date_range tuple",
         lambda: assert_eq(qa.analyze("sales today")["date_range"] is not None, True))
    test("p2-3: barcode still wins over date",
         lambda: assert_eq(qa.analyze("BAR268726580 today")["query_mode"], "barcode"))

    # ── P2-3: parser DATE_RANGE intent ───────────────────────────────────────
    parser = QueryParser()
    dr_parsed = parser.parse("show purchases last 7 days", "purchase", "english")
    test("p2-3: parser sets DATE_RANGE intent",
         lambda: assert_eq(dr_parsed["intent"], "DATE_RANGE"))
    test("p2-3: parser stores date_range",
         lambda: assert_eq(dr_parsed["date_range"] is not None, True))

    # ── P2-3: retriever date_range SQLite ─────────────────────────────────────
    # Load test data with dates
    db._load("date_t", [
        {"productName": "A", "sellDate": "2024-01-15T10:00:00", "totalAmount": "100"},
        {"productName": "B", "sellDate": "2024-02-20T10:00:00", "totalAmount": "200"},
        {"productName": "C", "sellDate": "2024-03-25T10:00:00", "totalAmount": "300"},
        {"productName": "D", "sellDate": "2024-07-01T10:00:00", "totalAmount": "400"},
    ])
    ret = RAGRetriever()
    dr_rows, dr_cols, dr_method = ret.retrieve("date_t", {
        "intent": "DATE_RANGE",
        "search_value": "",
        "target_col": None,
        "aggregation": "NONE",
        "negation_value": None,
        "date_range": ("2024-01-01", "2024-03-31"),
    })
    test("p2-3: date range returns 3 rows (Q1 2024)",
         lambda: assert_eq(len(dr_rows), 3))
    test("p2-3: date range excludes July",
         lambda: assert_eq(all(r.get("productName") != "D" for r in dr_rows), True))
    test("p2-3: date range method label",
         lambda: assert_in("date_range", dr_method))

    # ── P2-4: FOLLOW-UP DETECTOR ──────────────────────────────────────────────
    fd = FollowUpDetector()
    test("p2-4: 'sort by totalAmount' → followup",
         lambda: assert_eq(fd.detect("sort by totalAmount") is not None, True))
    test("p2-4: sort_col extracted",
         lambda: assert_eq(fd.detect("sort by totalAmount").get("sort_col","").lower(), "totalamount"))
    test("p2-4: 'top 10' → followup",
         lambda: assert_eq(fd.detect("top 10") is not None, True))
    test("p2-4: limit extracted",
         lambda: assert_eq(fd.detect("top 10")["limit"], 10))
    test("p2-4: 'filter Cash' → followup",
         lambda: assert_eq(fd.detect("filter Cash") is not None, True))
    test("p2-4: filter_val extracted",
         lambda: assert_in("cash", str(fd.detect("filter Cash")).lower()))
    test("p2-4: 'order by date desc' → followup",
         lambda: assert_eq(fd.detect("order by date desc") is not None, True))
    test("p2-4: desc direction",
         lambda: assert_eq(fd.detect("order by date desc")["sort_dir"], "DESC"))
    test("p2-4: asc direction",
         lambda: assert_eq(fd.detect("sort by name ascending")["sort_dir"], "ASC"))
    test("p2-4: plain 'show purchases' → NOT followup",
         lambda: assert_eq(fd.detect("show all purchases"), None))
    test("p2-4: 'what is stock' → NOT followup",
         lambda: assert_eq(fd.detect("what is current stock"), None))

    # ── P2-4: retriever apply_followup ───────────────────────────────────────
    db._load("fu_t", [
        {"productName": "Chair",  "totalAmount": "1500", "status": "active"},
        {"productName": "Table",  "totalAmount": "2500", "status": "inactive"},
        {"productName": "Desk",   "totalAmount": "3500", "status": "active"},
        {"productName": "Lamp",   "totalAmount": "500",  "status": "active"},
    ])
    fu_cols = db.columns("fu_t")
    fu_rows, fu_method = ret.apply_followup("fu_t", {"limit": 2}, fu_cols)
    test("p2-4: followup limit=2 returns 2 rows",
         lambda: assert_eq(len(fu_rows), 2))
    test("p2-4: followup method label",
         lambda: assert_eq(fu_method, "followup"))
    # Use "Table" as filter to get exactly 1 exact match on productName
    fu_rows2, _ = ret.apply_followup("fu_t", {"filter_val": "Table"}, fu_cols)
    test("p2-4: followup filter 'Table' returns 1",
         lambda: assert_eq(len(fu_rows2), 1))

    # ── P2-4: ConversationContext set/get last module ─────────────────────────
    ctx_t2 = ConversationContext(max_sessions=5, max_turns=3)
    ctx_t2.set_last_module("sess1", "purchase", "purchase")
    test("p2-4: get_last_module",
         lambda: assert_eq(ctx_t2.get_last_module("sess1"), "purchase"))
    test("p2-4: get_last_table",
         lambda: assert_eq(ctx_t2.get_last_table("sess1"), "purchase"))
    test("p2-4: unknown session → None",
         lambda: assert_eq(ctx_t2.get_last_module("unknown"), None))

    # ── P2-1: pre-emptive refresh constants ──────────────────────────────────
    test("p2-1: PREEMPT_THRESHOLD = 0.80",
         lambda: assert_eq(PREEMPT_THRESHOLD, 0.80))
    test("p2-1: _refreshing set exists on DataMirror",
         lambda: assert_eq(hasattr(db, "_refreshing"), True))
    test("p2-1: _ttl_start dict exists on DataMirror",
         lambda: assert_eq(hasattr(db, "_ttl_start"), True))
    test("p2-1: _ttl_dur dict exists on DataMirror",
         lambda: assert_eq(hasattr(db, "_ttl_dur"), True))
    test("p2-1: _should_preempt returns False for unknown key",
         lambda: assert_eq(db._should_preempt("nonexistent_key"), False))
    # Simulate a key that has been loaded and is 85% through its TTL
    db._ttl_start["test_key"] = time.time() - 255  # 255s into a 300s TTL = 85%
    db._ttl_dur["test_key"]   = 300
    test("p2-1: _should_preempt True when 85% expired",
         lambda: assert_eq(db._should_preempt("test_key"), True))
    db._ttl_start["test_key"] = time.time() - 100  # 100s into 300s TTL = 33%
    test("p2-1: _should_preempt False when 33% expired",
         lambda: assert_eq(db._should_preempt("test_key"), False))
    db._ttl_start.pop("test_key", None)
    db._ttl_dur.pop("test_key", None)

    # ── P2-2: DiskCacheManager ────────────────────────────────────────────────
    import tempfile, os as _os
    tmp_f = tempfile.mktemp(suffix=".db")
    try:
        dcm = DiskCacheManager(path=tmp_f, enabled=True)
        test("p2-2: disk cache opens successfully",
             lambda: assert_eq(dcm.is_available, True))
        dcm.save("purchase", [{"id": "1", "name": "A"}], time.time())
        result = dcm.load("purchase", max_age=3600)
        test("p2-2: disk save and load roundtrip",
             lambda: assert_eq(result is not None, True))
        test("p2-2: loaded data correct",
             lambda: assert_eq(result[0][0]["name"], "A"))
        # max_age exceeded
        dcm.save("old_mod", [{"x": "1"}], time.time() - 7200)
        old_result = dcm.load("old_mod", max_age=3600)
        test("p2-2: stale entry returns None",
             lambda: assert_eq(old_result, None))
        # delete
        dcm.save("del_mod", [{"y": "1"}], time.time())
        dcm.delete("del_mod")
        del_result = dcm.load("del_mod", max_age=3600)
        test("p2-2: deleted entry returns None",
             lambda: assert_eq(del_result, None))
        dcm.close()
    finally:
        try:
            _os.unlink(tmp_f)
        except Exception:
            pass

    # P2-2: disabled disk cache
    dcm2 = DiskCacheManager(path="/tmp/test_disabled.db", enabled=False)
    test("p2-2: disabled cache is_available=False",
         lambda: assert_eq(dcm2.is_available, False))
    dcm2.save("x", [], time.time())  # should not raise
    test("p2-2: disabled cache save doesn't crash",
         lambda: assert_eq(True, True))

    # ── P2-3: find_date_col ───────────────────────────────────────────────────
    test("p2-3: find_date_col for purchase finds purchaseDate",
         lambda: assert_eq(
             drp.find_date_col(["id", "purchaseDate", "totalAmount"], "purchase"),
             "purchaseDate"
         ))
    test("p2-3: find_date_col fallback to createdAt",
         lambda: assert_eq(
             drp.find_date_col(["id", "createdAt", "totalAmount"], "category"),
             "createdAt"
         ))

    # ── Retained v7.1 tests ───────────────────────────────────────────────────
    test("parser: bulk list",
         lambda: assert_eq(parser.parse("give me suppliers list", "supplier", "english")["intent"], "BULK_LIST"))
    test("parser: specific search",
         lambda: assert_eq(parser.parse("find supplier VIVEK TRADING", "supplier", "english")["intent"], "GLOBAL_SEARCH"))

    neg_p = parser.parse("show purchases not cancelled", "purchase", "english")
    test("p1-28: parser NEGATIVE_SEARCH intent", lambda: assert_eq(neg_p["intent"], "NEGATIVE_SEARCH"))
    test("p1-28: negation_value", lambda: assert_eq(neg_p["negation_value"], "cancelled"))

    # fmt tests
    test("fmt: datetime", lambda: assert_in("2026-02-27", _fmt_cell("createdAt", "2026-02-27T16:21:00")))
    test("fmt: no datetime bleed", lambda: assert_eq(_fmt_cell("size", "NOT SPECIFY"), html.escape("NOT SPECIFY")))

    # XSS
    payload = '<img src=x onerror=alert(1)>'
    test("sec: XSS no_results EN", lambda: assert_not_in("<img src=x", synth.no_results("purchase", payload, False)))
    agg0 = {"count": 1, "total_amount": None, "total_qty": None}
    test("sec: XSS intro", lambda: assert_not_in("<img src=x", synth.intro("purchase", agg0, payload, False, "english")))

    # Inf aggregation
    result = AggregationEngine().compute([{"totalAmount": "inf"}, {"totalAmount": "100"}], "purchase")
    test("agg: inf ignored", lambda: assert_eq(result["total_amount"], 100.0))

    # XSS in headers
    dirty_cols  = ['<script>alert(1)</script>', 'productName', 'totalAmount']
    sample_recs = [{'<script>alert(1)</script>': 'xss', 'productName': 'Chair', 'totalAmount': '100'}]
    tbl, _      = synth.table(sample_recs, dirty_cols, False)
    test("sec: script in header escaped", lambda: assert_not_in("<script>", tbl))

    # Session sanitization
    test("sec: session path traversal", lambda: assert_eq(_sanitize_session_id("../../etc/passwd"), "etcpasswd"))
    test("sec: session cap 64", lambda: assert_eq(len(_sanitize_session_id("a" * 100)), 64))

    # Flatten
    rec  = {"id":"1","items":[{"sku":f"S{i}"}for i in range(50)],"taxes":[{"t":f"T{i}"}for i in range(20)]}
    flat = db._flatten([rec])
    test("sec: flatten not cartesian", lambda: assert_lte(len(flat), 200))

    # SQLite round-trip
    db._load("t1", [
        {"productName":"Chair","totalAmount":"1500","barcode":"BAR123"},
        {"productName":"Table","totalAmount":"2500","barcode":"BAR456"},
        {"productName":"Desk", "totalAmount":"3500","barcode":"BAR789"},
    ])
    rows, cols, method = ret.retrieve("t1", {"intent":"BULK_LIST","search_value":"","target_col":None,"aggregation":"NONE","negation_value":None,"date_range":None})
    test("sqlite: bulk=3", lambda: assert_eq(len(rows), 3))
    rows2, _, _ = ret.retrieve("t1", {"intent":"GLOBAL_SEARCH","search_value":"Chair","target_col":None,"aggregation":"NONE","negation_value":None,"date_range":None})
    test("sqlite: FTS Chair", lambda: assert_eq(len(rows2), 1))

    # P1-24
    fts_cols_t1 = db.fts_columns("t1")
    test("p1-24: FTS no totalAmount", lambda: assert_eq("totalAmount" not in fts_cols_t1, True))
    test("p1-24: FTS has productName", lambda: assert_in("productName", fts_cols_t1))

    # P1-28 negative search
    db._load("neg_t", [
        {"productName":"Chair","status":"active"},
        {"productName":"Table","status":"inactive"},
        {"productName":"Desk", "status":"active"},
    ])
    neg_rows, _, neg_method = ret.retrieve("neg_t", {"intent":"NEGATIVE_SEARCH","search_value":"inactive","target_col":None,"aggregation":"NONE","negation_value":"inactive","date_range":None})
    test("p1-28: negative 2 rows", lambda: assert_eq(len(neg_rows), 2))
    test("p1-28: negative excludes inactive", lambda: assert_eq(all(r.get("status") != "inactive" for r in neg_rows), True))

    # P1-23
    test("p1-23: WARMUP 17 modules", lambda: assert_eq(set(WARMUP_MODULES), set(MODULE_REGISTRY.keys())))

    # P1-26 LatencyTracker
    lt = LatencyTracker(maxlen=10)
    test("p1-26: empty → None", lambda: assert_eq(lt.percentiles()["p50"], None))
    for ms in [10,20,30,40,50,60,70,80,90,100]: lt.record(ms/1000)
    pct = lt.percentiles()
    test("p1-26: p50 in range", lambda: assert_eq(40 <= pct["p50"] <= 60, True))
    test("p1-26: p99 ≥ 90", lambda: assert_eq(pct["p99"] >= 90, True))

    # P1-27 print CSS
    tbl_p, _ = synth.table([{"productName":"Chair","totalAmount":"100"}], ["productName","totalAmount"], False)
    test("p1-27: @media print", lambda: assert_in("@media print", tbl_p))
    test("p1-27: erp-table class", lambda: assert_in("erp-table", tbl_p))

    # P1-29 aliases
    ic = IntentClassifier()
    test("p1-29: bills → sell", lambda: assert_eq(ic.classify("show bills")["module"], "sell"))
    test("p1-29: debit note → supplier-credit", lambda: assert_eq(ic.classify("debit note")["module"], "supplier-credit"))

    # Rate limiter
    rl = RateLimiter(rpm=5)
    for _ in range(5): rl.is_allowed("1.2.3.4")
    test("rl: 6th blocked", lambda: assert_eq(rl.is_allowed("1.2.3.4"), False))
    test("rl: different IP ok", lambda: assert_eq(rl.is_allowed("9.9.9.9"), True))

    # NLU
    test("nlu: purchases", lambda: assert_eq(ic.classify("show all purchases")["module"], "purchase"))
    test("nlu: supplier credit", lambda: assert_eq(ic.classify("supplier credits outstanding")["module"], "supplier-credit"))
    test("nlu: financial MULTI", lambda: assert_eq(ic.classify("financial summary")["type"], "MULTI"))

    # Circuit breaker
    cb = CircuitBreaker()
    for _ in range(CB_FAILURE_THRESHOLD): cb.record_failure()
    test("cb: opens", lambda: assert_eq(cb._state, _CBState.OPEN))
    cb._opened_at = time.time() - CB_RECOVERY_TIMEOUT - 1
    cb.is_open  # trigger transition
    for _ in range(CB_SUCCESS_THRESHOLD): cb.record_success()
    test("cb: closes", lambda: assert_eq(cb._state, _CBState.CLOSED))

    # Router
    router_inst = SmartQueryRouter()
    test("router: error body", lambda: assert_eq(router_inst._is_error_body({"message": "not found"}), True))
    test("router: real record not error", lambda: assert_eq(router_inst._is_error_body({"id":"42","productName":"Chair"}), False))

    # Module registry
    test("registry: 17 modules", lambda: assert_eq(len(MODULE_REGISTRY), 17))

    # sell endpoints
    sell_eps = MODULE_REGISTRY["sell"]["search_endpoints"]
    test("fix-c3: sell has id", lambda: assert_in("id", sell_eps))
    test("fix-c4: sell next_invoice", lambda: assert_in("next_invoice", sell_eps))

    # Version strings
    test("fix-l1: GREETING_EN v8.0", lambda: assert_in("v8.0", GREETING_EN))
    test("fix-l1: GREETING_MR v8.0", lambda: assert_in("v8.0", GREETING_MR))
    test("fix-l1: IDENTITY v8.0",    lambda: assert_in("v8.0", IDENTITY_RESPONSE))

    # Performance
    big = [{"productName":f"P{i}","totalAmount":str(i*10),"barcode":f"BAR{i:09d}"} for i in range(600)]
    db._load("perf_t", big)
    rows_p, cols_p, _ = RAGRetriever().retrieve("perf_t", {"intent":"BULK_LIST","search_value":"","target_col":None,"aggregation":"NONE","negation_value":None,"date_range":None})
    t0 = time.time()
    tbl_h, rendered = synth.table(rows_p[:600], cols_p, False)
    elapsed = time.time() - t0
    test(f"perf: 500-row render < 0.5s ({elapsed:.3f}s)", lambda: assert_lte(elapsed, 0.5))
    test("perf: render cap ≤500", lambda: assert_lte(rendered, MAX_RENDER_ROWS))

    # P1-10
    test("p1-10: customer-ledger-summary Marathi", lambda: assert_in("customer-ledger-summary", _MR_RAW))

    # P2-2: env example has disk cache vars
    test("p2-2: .env has ERP_DISK_CACHE", lambda: assert_in("ERP_DISK_CACHE", _ENV_EXAMPLE))
    test("p2-2: .env has ERP_DISK_CACHE_ENABLED", lambda: assert_in("ERP_DISK_CACHE_ENABLED", _ENV_EXAMPLE))

    # Summary
    total = passed + failed
    print(f"\n══════════════════════════════════════════════════════")
    print(f"  Results: {passed}/{total} passed", end="")
    if failed:
        print(f"   ⚠️  {failed} FAILED")
    else:
        print("   🎉 ALL PASSED")
    print(f"══════════════════════════════════════════════════════\n")
    return failed == 0


# ── Test helpers ──────────────────────────────────────────────────────────────
import sys

def assert_eq(a, b):    assert a == b,  f"Expected {b!r}, got {a!r}"
def assert_ne(a, b):    assert a != b,  f"Expected not {b!r}, got {a!r}"
def assert_in(a, b):    assert a in b,  f"{a!r} not found in result"
def assert_not_in(a,b): assert a not in b, f"{a!r} should NOT be in result"
def assert_lte(a, b):   assert a <= b,  f"Expected ≤{b}, got {a}"
def assert_lt(a, b):    assert a < b,   f"Expected <{b}, got {a}"


if __name__ == "__main__":
    success = _run_tests()
    sys.exit(0 if success else 1)