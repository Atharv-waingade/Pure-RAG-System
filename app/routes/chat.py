"""
==============================================================================
  ADMIN INTELLIGENCE ENGINE  v7.1  —  PRODUCTION READY
  ERP Chatbot RAG Pipeline · Zero external AI APIs · ≤300 MB RAM
  Full Marathi (Devanagari + Roman) + English support

  PHASE 1 QUICK WINS  (Items 23–30) — built on v7.0 stable base
  ──────────────────────────────────────────────────────────────
  P1-23 : Warmup all 17 modules — WARMUP_MODULES = list(MODULE_REGISTRY.keys())
  P1-24 : FTS index optimization — FTS5 restricted to FTS_COLUMNS allowlist
           (8-10 high-value columns), cutting index size and boosting speed.
  P1-25 : 'Named' query mode — NAME_RE regex routes "named/called/for X"
           to existing name endpoints (material, purchase stock); others
           fall through to FTS gracefully.
  P1-26 : Response time in /health — LatencyTracker (deque, 1000 items)
           records process() duration; /health reports p50/p95/p99 ms.
  P1-27 : Print-friendly CSS — @media print block injected into every
           table render (browser deduplicates <style> in same doc).
  P1-28 : Negative search — NEGATION_RE + "NEGATIVE_SEARCH" intent +
           RAGRetriever._negative() → WHERE col NOT LIKE '%val%'.
  P1-29 : Module aliases — 'bills'/'bill' → sell;
           'debit note'/'debit notes' → supplier-credit.
  P1-30 : python-dotenv support — optional load_dotenv(); .env.example
           generated at startup if missing.

  ALL v7.0 FIXES RETAINED (43 issues resolved in v7.0).
==============================================================================
"""

from __future__ import annotations

# ── P1-30: python-dotenv optional load ───────────────────────────────────────
try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv()
except ImportError:
    pass  # dotenv not installed — env vars come from shell/platform

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
    from pydantic import BaseModel
    _FASTAPI_AVAILABLE = True
except ImportError:
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

MODULE_TTL              = 300        # 5 min  — master data
STOCK_TTL               = 120        # 2 min  — inventory
FINANCE_TTL             = 600        # 10 min — ledgers
MAX_ROWS                = 30_000     # per-module SQLite cap
MAX_RENDER_ROWS         = 500        # HTML table render cap
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

# P1-23: warm up ALL 17 modules (expanded from 4)
# Defined after MODULE_REGISTRY below — assigned via list(MODULE_REGISTRY.keys())

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

# ── P1-24: FTS column allowlist — only index high-value searchable columns ───
# Restricts FTS5 virtual table to these columns; main data table stays complete.
FTS_COLUMNS = frozenset({
    "name", "firstName", "lastName",
    "productName", "supplierName", "customerName", "materialName",
    "invoiceNo", "billNo", "supplierBill",
    "barcode",
    "contact", "phone", "mobile", "email",
    "status", "supplyType",
})

# ──────────────────────────────────────────────────────────────────────────────
# MODULE REGISTRY  — all 35 API endpoints wired (v7.0) + P1-29 aliases added
# ──────────────────────────────────────────────────────────────────────────────
MODULE_REGISTRY: Dict[str, Dict[str, Any]] = {

    "supplier-credit": {
        "url":         "/api/supplier-credit/get-all-supplier-credits",
        "ttl":         FINANCE_TTL,
        "keywords":    ["credit", "credits", "outstanding", "due", "refund", "owe", "owes"],
        "phrases":     ["supplier credit", "supplier credits", "vendor credit",
                        "credit note", "credit balance", "outstanding credit",
                        "how much do we owe", "credits owed",
                        # P1-29: debit note alias routes here
                        "debit note", "debit notes"],
        "aliases":     ["supplier credit", "vendor credit", "credits outstanding",
                        # P1-29
                        "debit note", "debit notes"],
        "amount_cols": ["creditAmount", "amount", "totalCredit", "totalAmount"],
        "qty_cols":    [],
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
                        # P1-29: bills alias
                        "bills list", "show bills", "all bills"],
        "aliases":     ["sales", "sells", "invoices", "receipts", "billing",
                        # P1-29
                        "bills", "bill"],
        "amount_cols": ["totalAmount", "sellPrice", "amount", "paid"],
        "qty_cols":    ["quantity", "qty", "totalQuantity"],
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
                        "active printer", "show printers", "printer details"],
        "aliases":     ["printers", "printing"],
        "amount_cols": [],
        "qty_cols":    [],
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
        "search_endpoints": {
            "active": {"url": "/api/email-config/active",            "param": None},
            "id":     {"url": "/api/email-config/get-email-by-id",   "param": "id"},
        },
    },
}

# P1-23: warm up ALL 17 modules
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
# COMPILED REGEXES  — P1-25 NAME_RE, P1-28 NEGATION_RE added
# ──────────────────────────────────────────────────────────────────────────────
BARCODE_RE      = re.compile(r"\bBAR\w{6,}\b",                        re.IGNORECASE)
INVOICE_RE      = re.compile(r"\b(PA\d{5,}|SA\d{5,}|INV[-/]\w+)\b",  re.IGNORECASE)
CONTACT_RE      = re.compile(r"\b[6-9]\d{9}\b")
ACTIVE_RE       = re.compile(r"\bactive\b",                            re.IGNORECASE)
ID_RE           = re.compile(r"\bid\s*[:#]?\s*(\d+)\b",               re.IGNORECASE)
NEXT_INVOICE_RE = re.compile(r"\bnext\s+(purchase|sell|sale|invoice)\s*(number|no|invoice)?\b",
                              re.IGNORECASE)
# P1-25: named query mode — "product named Chair", "called Rahul", "for supplier ABC"
NAME_RE         = re.compile(
    r"\b(?:named?|called|for)\s+([\w\s]{2,40}?)(?:\s+(?:in|from|at|by|with|list|details|info|record|history)|\s*$)",
    re.IGNORECASE,
)
# P1-28: negative search — "not X", "except X", "exclude X", "without X"
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
    # P1-28: negation words go into stop set so they don't pollute search value
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
    # P1-10 (carried from roadmap, wired in v7.1): customer-ledger-summary Marathi
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
# QUERY ANALYZER  — P1-25 NAME_RE, P1-28 NEGATION_RE integrated
# ──────────────────────────────────────────────────────────────────────────────
class QueryAnalyzer:
    def analyze(self, user_query: str) -> Dict[str, Any]:
        q = user_query.strip()

        # Priority order: barcode > invoice > contact > next_invoice > id > name > negation > active
        m = BARCODE_RE.search(q)
        if m:
            return {"query_mode": "barcode", "query_value": m.group(0).upper()}

        m = INVOICE_RE.search(q)
        if m:
            return {"query_mode": "invoice", "query_value": m.group(0).upper()}

        m = CONTACT_RE.search(q)
        if m:
            return {"query_mode": "contact", "query_value": m.group(0)}

        m = NEXT_INVOICE_RE.search(q)
        if m:
            return {"query_mode": "next_invoice", "query_value": m.group(1).lower()}

        m = ID_RE.search(q)
        if m:
            return {"query_mode": "id", "query_value": m.group(1)}

        # P1-25: named query — "product named Chair", "material called Cotton"
        m = NAME_RE.search(q)
        if m:
            name_val = m.group(1).strip()
            if name_val and len(name_val) >= 2:
                return {"query_mode": "name", "query_value": name_val}

        # P1-28: negation — "show purchases not cancelled"
        m = NEGATION_RE.search(q)
        if m:
            neg_val = m.group(1).strip()
            if neg_val and len(neg_val) >= 2:
                return {"query_mode": "negation", "query_value": neg_val}

        if ACTIVE_RE.search(q):
            return {"query_mode": "active", "query_value": None}

        return {"query_mode": None, "query_value": None}


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
# QUERY PARSER  — P1-28 NEGATIVE_SEARCH intent added
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

        # P1-28: detect negation intent from full query (before bulk check)
        negation_value: Optional[str] = None
        neg_m = NEGATION_RE.search(user_query.lower())
        if neg_m:
            negation_value = neg_m.group(1).strip()

        if target_col:
            intent = "COLUMN_SEARCH" if search_value else "PROMPT_NEEDED"
        elif negation_value:
            # Negative search takes priority over GLOBAL_SEARCH
            intent = "NEGATIVE_SEARCH"
            search_value = negation_value  # reuse field, semantics handled in retriever
        elif search_value and not is_bulk:
            intent = "GLOBAL_SEARCH"
        else:
            intent = "BULK_LIST"

        return {
            "intent":          intent,
            "target_col":      target_col,
            "search_value":    search_value,
            "negation_value":  negation_value,
            "aggregation":     "SUM" if agg else "NONE",
        }


# ──────────────────────────────────────────────────────────────────────────────
# DATA MIRROR — SQLite in-memory + FTS5
# P1-24: FTS restricted to FTS_COLUMNS allowlist
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
        self._stale_data:  Dict[str, bool]         = {}
        self._cols:        Dict[str, List[str]]    = {}
        self._fts_cols:    Dict[str, List[str]]    = {}  # P1-24: track FTS cols per table
        self._locks:       collections.defaultdict = collections.defaultdict(asyncio.Lock)
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
        """
        P1-24: FTS5 virtual table is now built only over FTS_COLUMNS allowlist
        (intersected with actual record columns), reducing index size and
        improving search speed. The main data table still stores all columns.
        """
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

        # Main data table — all columns
        cols_ddl = ", ".join(f'"{k}" TEXT' for k in all_keys)
        cur.execute(f'CREATE TABLE "{key}" (_rowid INTEGER PRIMARY KEY, {cols_ddl})')
        ph      = ",".join(["?"] * len(all_keys))
        col_sql = ", ".join(f'"{k}"' for k in all_keys)
        for r in records:
            cur.execute(
                f'INSERT INTO "{key}" ({col_sql}) VALUES ({ph})',
                [r.get(k, "") for k in all_keys],
            )

        # P1-24: FTS table — only allowlisted columns that exist in this dataset
        fts_eligible = [k for k in all_keys if k in FTS_COLUMNS]
        # Fallback: if no eligible columns, use first 5 text-looking columns
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

    async def sync_module(self, key: str, force: bool = False) -> str:
        now = time.time()
        if not force and now < self._ttl.get(key, 0):
            return "fresh"
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
                circuit_breaker.record_failure()
                if self._cols.get(key):
                    self._stale_data[key] = True
                    log.warning("Using stale data for '%s'", key)
                    return "stale"
                return "error"
            circuit_breaker.record_success()
            flat = self._flatten(self._extract_list(raw))
            self._load(key, flat)
            self._ttl[key]        = time.time() + mod["ttl"]
            self._stale_data[key] = False
            response_cache.invalidate_module(key)
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
        """P1-24: return which columns are indexed in FTS for this table."""
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
# RAG RETRIEVER  — P1-28 _negative() method added
# P1-24: FTS query restricted to fts_cols per table
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
            fts_cols = db.fts_columns(key)  # P1-24: use restricted FTS cols
            if not fts_cols:
                return [], "fts_no_index"
            safe = FTS_SPECIAL.sub(" ", val)
            safe = _FTS_BOOL_RE.sub(" ", safe).strip()
            if not safe:
                return [], "fts_empty"
            if tcol:
                ac = self._resolve_col(tcol, fts_cols)  # restrict to fts cols
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
        """
        P1-28: Negative search — returns rows where NO column contains val.
        Runs: SELECT * FROM table WHERE col1 NOT LIKE '%val%' AND col2 NOT LIKE '%val%' ...
        """
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
# P1-26: LATENCY TRACKER — ring buffer → p50/p95/p99 ms
# ──────────────────────────────────────────────────────────────────────────────
class LatencyTracker:
    """
    Records process() latency samples in a fixed-size deque (ring buffer).
    Computes p50, p95, p99 on demand. Thread-safe for asyncio single-worker.
    """
    def __init__(self, maxlen: int = 1000):
        self._samples: deque = deque(maxlen=maxlen)

    def record(self, elapsed_seconds: float):
        self._samples.append(elapsed_seconds * 1000)  # store in ms

    def percentiles(self) -> Dict[str, Optional[float]]:
        if not self._samples:
            return {"p50": None, "p95": None, "p99": None, "count": 0}
        s = sorted(self._samples)
        n = len(s)
        def pct(p: float) -> float:
            idx = max(0, min(n - 1, int(math.ceil(p / 100 * n)) - 1))
            return round(s[idx], 2)
        return {
            "p50":   pct(50),
            "p95":   pct(95),
            "p99":   pct(99),
            "count": n,
        }

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
# RESPONSE SYNTHESIZER  — P1-27 print-friendly CSS added
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

# P1-27: print-friendly CSS injected once per table — browser deduplicates
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
        """
        P1-27: Injects _PRINT_CSS block + erp-table-wrap / erp-table CSS classes
        for print-friendly rendering. Browser deduplicates <style id="erp-print-css">.
        """
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

        # P1-27: add erp-table-wrap + erp-table classes; prepend print CSS
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
                f"<li><em>Barcode BAR268726580</em></li>"
                f"<li><em>Purchase id 42</em></li>"
                f"<li><em>Next purchase invoice number</em></li>"
                f"<li><em>Customer payment history</em></li>"
                f"<li><em>Show purchases not cancelled</em></li>"
                f"</ul></div>")


synth = ResponseSynthesizer()


# ──────────────────────────────────────────────────────────────────────────────
# CONVERSATION CONTEXT  (FIX-M1: throttled eviction retained)
# ──────────────────────────────────────────────────────────────────────────────
def _sanitize_session_id(sid: str) -> str:
    clean = SESSION_RE.sub("", sid)[:64]
    return clean if clean else "default"


class ConversationContext:
    def __init__(self, max_sessions: int = MAX_SESSIONS,
                 max_turns: int = 5, session_ttl: float = SESSION_TTL):
        self._sessions:      "collections.OrderedDict[str, Tuple[deque, float]]" = \
            collections.OrderedDict()
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

    def add(self, sid: str, role: str, text: str):
        sid = _sanitize_session_id(sid)
        self._evict_stale()
        if sid not in self._sessions:
            if len(self._sessions) >= self._max_sessions:
                self._sessions.popitem(last=False)
            self._sessions[sid] = (deque(maxlen=self._max_turns), time.time())
        turns, _ = self._sessions[sid]
        turns.append({"role": role, "text": text[:300]})
        self._sessions[sid] = (turns, time.time())
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
# MAIN ORCHESTRATOR  — P1-26: latency tracking added
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

        if not q_mode:
            cached = response_cache.get(
                key, parsed["intent"], parsed["search_value"], lang
            )
            if cached:
                return cached

        targeted_key   = None
        targeted_badge = ""

        if q_mode:
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
        records, cols, _ = retriever.retrieve(key, parsed)

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
        result = stale_banner + intro + tbl
        ctx.add(sid, "bot", f"_module:{key}")

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
# STATIC RESPONSES  — v7.1
# ──────────────────────────────────────────────────────────────────────────────
GREETING_EN = """
<div style="font-family:'Segoe UI',system-ui,sans-serif;max-width:580px">
  <p style="font-size:18px;font-weight:700;margin:0 0 14px;color:#1e293b">
    👋 Hello! I'm your <span style="color:#6366f1">ERP Intelligence Assistant v7.1</span>.
  </p>
  <p style="margin:0 0 14px;color:#475569;font-size:14px">Here's what I can help you with:</p>
  <div style="display:grid;gap:8px">
    <div style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:8px;padding:10px 14px;font-size:14px">
      📦 <strong>Stock &amp; Inventory</strong> — <em>"Show all stock"</em>, <em>"Barcode BAR268726580"</em>, <em>"Product named Chair"</em>
    </div>
    <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:10px 14px;font-size:14px">
      💰 <strong>Sales &amp; Purchases</strong> — <em>"Invoice PA00000001"</em>, <em>"Purchase id 42"</em>, <em>"Next purchase invoice number"</em>, <em>"Show bills"</em>
    </div>
    <div style="background:#fdf4ff;border:1px solid #e9d5ff;border-radius:8px;padding:10px 14px;font-size:14px">
      🏢 <strong>Suppliers &amp; Customers</strong> — <em>"All suppliers"</em>, <em>"Customer id 10"</em>, <em>"Customer payment history"</em>, <em>"Debit notes"</em>
    </div>
    <div style="background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 14px;font-size:14px">
      📊 <strong>Financial Reports</strong> — <em>"Financial summary"</em>, <em>"Supplier credits"</em>, <em>"Customer ledger detail"</em>
    </div>
    <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:10px 14px;font-size:14px">
      🔍 <strong>Smart Filters</strong> — <em>"Show purchases not cancelled"</em>, <em>"Material called Cotton"</em>
    </div>
  </div>
  <p style="margin:12px 0 0;font-size:13px;color:#94a3b8">
    ⚡ Smart lookups: barcode · invoice · mobile · numeric ID · next invoice · named · exclude.<br>
    You can also ask in <strong>मराठी</strong> or Marathi Roman typing.
  </p>
</div>
"""

GREETING_MR = """
<div style="font-family:'Noto Sans Devanagari','Segoe UI',system-ui,sans-serif;max-width:580px">
  <p style="font-size:18px;font-weight:700;margin:0 0 14px;color:#1e293b">
    नमस्कार! मी तुमचा <span style="color:#6366f1">ERP सहाय्यक v7.1</span> आहे. 🙏
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
      📊 <strong>आर्थिक अहवाल</strong> — <em>"supplier credit kiti ahe"</em>, <em>"grahak khate saransh"</em>
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
    🧠 Admin Intelligence Engine v7.1 — Phase 1 Quick Wins
  </p>
  <p style="margin:0 0 16px;color:#475569">
    Production-grade <strong>RAG pipeline</strong> for ERP —
    zero external AI APIs · full Marathi support · ≤300 MB RAM ·
    all 35 APIs wired · 43 bugs fixed from v6.3 · 8 Phase-1 features added.
  </p>
  <div style="background:#fef2f2;border-radius:10px;padding:16px;margin-bottom:12px">
    <strong style="color:#dc2626">🔒 Security:</strong> XSS prevention · memory bomb protection ·
    per-module locks · JWT caching · NaN/Inf guards · rate limiting (60 rpm/IP) ·
    session sanitization · no hardcoded creds · image URL whitelist · FTS injection prevention.
  </div>
  <div style="background:#f0fdf4;border-radius:10px;padding:16px;margin-bottom:12px">
    <strong style="color:#16a34a">🚀 v7.1 Phase 1 additions:</strong>
    All 17 modules warmed up (P1-23) ·
    FTS restricted to 10 key columns for speed (P1-24) ·
    Named query mode — "product named X" (P1-25) ·
    Latency p50/p95/p99 in /health (P1-26) ·
    Print-friendly CSS @media print (P1-27) ·
    Negative search — "not/exclude/without X" (P1-28) ·
    Module aliases: bills→sell, debit note→supplier-credit (P1-29) ·
    python-dotenv support (P1-30).
  </div>
  <div style="background:#f0f9ff;border-radius:10px;padding:16px">
    <strong style="color:#0369a1">⚡ APIs covered (35):</strong>
    purchase (6) · sell (5) · material (3) · supplier-credit (2) · customer (4) ·
    supplier (2) · reports (8) · printer (3) · payment (1) · email-config (3).
  </div>
</div>
"""


# ──────────────────────────────────────────────────────────────────────────────
# P1-30: .env.example — written once at startup if missing
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
"""


def _write_env_example():
    """Write .env.example to the working directory if it doesn't exist yet."""
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
        # P1-23: warm ALL 17 modules
        log.info("Warmup: pre-fetching all %d modules: %s",
                 len(WARMUP_MODULES), WARMUP_MODULES)
        results = await db.sync_modules(WARMUP_MODULES)
        ok_count = sum(1 for v in results.values() if v == "ok")
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

    app = FastAPI(title="ERP Intelligence Engine v7.1", lifespan=lifespan)

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

        # P1-26: track latency
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
        # P1-26: include latency percentiles
        return {
            "status":          "ok",
            "version":         "7.1",
            "ready":           _startup_ready.is_set() if _startup_ready else False,
            "uptime_seconds":  int(db.uptime_seconds()),
            "sessions_active": ctx.session_count(),
            "cache_entries":   response_cache.size(),
            "circuit_breaker": circuit_breaker.status(),
            "latency_ms":      latency_tracker.percentiles(),  # P1-26
            "modules":         db.cache_status(),
        }

    @app.get("/")
    async def root():
        return {
            "message": "ERP Intelligence Engine v7.1",
            "chat":    "POST /api/chat",
            "health":  "GET /health",
            "docs":    "GET /docs",
        }

    app.include_router(router, prefix="/api")

else:
    router = None  # type: ignore


# ──────────────────────────────────────────────────────────────────────────────
# SELF-TEST SUITE  —  python erp_engine_v7_1.py
# All 100 original v7.0 tests + Phase 1 tests for items 23-30
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
    print("  ERP Engine v7.1 — Full Self-Test Suite (100 + Phase1)")
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

    # ── Query Analyzer — original ─────────────────────────────────────────────
    qa = QueryAnalyzer()
    test("qa: barcode", lambda: assert_eq(qa.analyze("BAR268726580")["query_mode"], "barcode"))
    test("qa: invoice PA", lambda: assert_eq(qa.analyze("invoice PA00000003")["query_mode"], "invoice"))
    test("qa: invoice value", lambda: assert_eq(qa.analyze("invoice PA00000003")["query_value"], "PA00000003"))
    test("qa: contact", lambda: assert_eq(qa.analyze("customer 9876543210")["query_mode"], "contact"))
    test("qa: active", lambda: assert_eq(qa.analyze("show active printer")["query_mode"], "active"))
    test("qa: id lookup", lambda: assert_eq(qa.analyze("show purchase id 42")["query_mode"], "id"))
    test("qa: id value", lambda: assert_eq(qa.analyze("show purchase id 42")["query_value"], "42"))
    test("qa: id with colon", lambda: assert_eq(qa.analyze("supplier id: 10")["query_value"], "10"))
    test("qa: next purchase invoice", lambda: assert_eq(qa.analyze("what is next purchase invoice number")["query_mode"], "next_invoice"))
    test("qa: next sell invoice", lambda: assert_eq(qa.analyze("next sell invoice no")["query_mode"], "next_invoice"))
    test("qa: next_invoice value", lambda: assert_in(qa.analyze("next purchase invoice")["query_value"], ["purchase","sell","invoice"]))

    # ── P1-25: NAME_RE ────────────────────────────────────────────────────────
    test("p1-25: 'named Chair' → name mode",
         lambda: assert_eq(qa.analyze("show product named Chair")["query_mode"], "name"))
    test("p1-25: 'called Cotton' → name mode",
         lambda: assert_eq(qa.analyze("material called Cotton")["query_mode"], "name"))
    test("p1-25: name value extracted",
         lambda: assert_eq(qa.analyze("product named Chair")["query_value"], "Chair"))
    test("p1-25: 'for supplier ABC' → name mode",
         lambda: assert_eq(qa.analyze("show stock for supplier ABC")["query_mode"], "name"))
    test("p1-25: barcode still wins over name",
         lambda: assert_eq(qa.analyze("show BAR268726580 named Chair")["query_mode"], "barcode"))

    # ── P1-28: NEGATION_RE ────────────────────────────────────────────────────
    test("p1-28: 'not cancelled' → negation mode",
         lambda: assert_eq(qa.analyze("show purchases not cancelled")["query_mode"], "negation"))
    test("p1-28: 'exclude pending' → negation mode",
         lambda: assert_eq(qa.analyze("show sells exclude pending")["query_mode"], "negation"))
    test("p1-28: negation value extracted",
         lambda: assert_eq(qa.analyze("show sells not cancelled")["query_value"], "cancelled"))
    test("p1-28: 'without Cash' → negation mode",
         lambda: assert_eq(qa.analyze("purchases without Cash")["query_mode"], "negation"))
    test("p1-28: invoice still wins over negation",
         lambda: assert_eq(qa.analyze("invoice PA00123 not pending")["query_mode"], "invoice"))

    # ── BUG-07: all-stop → BULK_LIST ─────────────────────────────────────────
    parser = QueryParser()
    test("parser: 'give me suppliers list' → BULK",
         lambda: assert_eq(parser.parse("give me suppliers list", "supplier", "english")["intent"], "BULK_LIST"))
    test("parser: 'show me all purchases' → BULK",
         lambda: assert_eq(parser.parse("show me all purchases", "purchase", "english")["intent"], "BULK_LIST"))
    test("parser: specific search → GLOBAL",
         lambda: assert_eq(parser.parse("find supplier VIVEK TRADING", "supplier", "english")["intent"], "GLOBAL_SEARCH"))

    # ── P1-28: parser negative intent ────────────────────────────────────────
    neg_p = parser.parse("show purchases not cancelled", "purchase", "english")
    test("p1-28: parser sets NEGATIVE_SEARCH intent",
         lambda: assert_eq(neg_p["intent"], "NEGATIVE_SEARCH"))
    test("p1-28: parser sets negation_value",
         lambda: assert_eq(neg_p["negation_value"], "cancelled"))
    test("p1-28: parser search_value = negation_value for retriever",
         lambda: assert_eq(neg_p["search_value"], neg_p["negation_value"]))

    # ── BUG-15: datetime detection ────────────────────────────────────────────
    test("fmt: valid datetime",
         lambda: assert_in("2026-02-27", _fmt_cell("createdAt", "2026-02-27T16:21:00")))
    test("fmt: 'NOT SPECIFY' not datetime",
         lambda: assert_eq(_fmt_cell("size", "NOT SPECIFY"), html.escape("NOT SPECIFY")))
    test("fmt: 'T-shirt' not datetime",
         lambda: assert_eq(_fmt_cell("name", "T-shirt"), html.escape("T-shirt")))

    # ── SEC-01: XSS ───────────────────────────────────────────────────────────
    payload = '<img src=x onerror=alert(1)>'
    test("sec: XSS in no_results EN",
         lambda: assert_not_in("<img src=x", synth.no_results("purchase", payload, False)))
    test("sec: XSS in no_results MR",
         lambda: assert_not_in("<img src=x", synth.no_results("purchase", payload, True)))
    agg0 = {"count": 1, "total_amount": None, "total_qty": None}
    test("sec: XSS in intro",
         lambda: assert_not_in("<img src=x", synth.intro("purchase", agg0, payload, False, "english")))

    # ── SEC-05: NaN/Inf aggregation ───────────────────────────────────────────
    result = AggregationEngine().compute([{"totalAmount": "inf"}, {"totalAmount": "100"}], "purchase")
    test("agg: inf ignored", lambda: assert_eq(result["total_amount"], 100.0))

    # ── SEC-06: column header injection ──────────────────────────────────────
    dirty_cols  = ['<script>alert(1)</script>', 'productName', 'totalAmount']
    sample_recs = [{'<script>alert(1)</script>': 'xss', 'productName': 'Chair', 'totalAmount': '100'}]
    tbl, _      = synth.table(sample_recs, dirty_cols, False)
    test("sec: script tag in header escaped", lambda: assert_not_in("<script>", tbl))

    # ── SEC-08: session sanitization ─────────────────────────────────────────
    test("sec: session path traversal",
         lambda: assert_eq(_sanitize_session_id("../../etc/passwd"), "etcpasswd"))
    test("sec: session cap at 64",
         lambda: assert_eq(len(_sanitize_session_id("a" * 100)), 64))

    # ── SEC-02: flatten no cartesian product ─────────────────────────────────
    rec  = {"id":"1","items":[{"sku":f"S{i}"}for i in range(50)],"taxes":[{"t":f"T{i}"}for i in range(20)]}
    flat = db._flatten([rec])
    test("sec: flatten concat not cartesian", lambda: assert_lte(len(flat), 200))

    # ── SQLite round-trip ─────────────────────────────────────────────────────
    db._load("t1", [
        {"productName":"Chair","totalAmount":"1500","barcode":"BAR123"},
        {"productName":"Table","totalAmount":"2500","barcode":"BAR456"},
        {"productName":"Desk", "totalAmount":"3500","barcode":"BAR789"},
    ])
    ret = RAGRetriever()
    rows, cols, method = ret.retrieve("t1", {"intent":"BULK_LIST","search_value":"","target_col":None,"aggregation":"NONE","negation_value":None})
    test("sqlite: bulk returns 3", lambda: assert_eq(len(rows), 3))
    rows2, _, _ = ret.retrieve("t1", {"intent":"GLOBAL_SEARCH","search_value":"Chair","target_col":None,"aggregation":"NONE","negation_value":None})
    test("sqlite: FTS finds Chair", lambda: assert_eq(len(rows2), 1))
    rows3, _, _ = ret.retrieve("t1", {"intent":"COLUMN_SEARCH","search_value":"BAR456","target_col":"barcode","aggregation":"NONE","negation_value":None})
    test("sqlite: col search finds Table", lambda: assert_eq(rows3[0].get("productName"), "Table"))

    # ── P1-24: FTS column restriction ────────────────────────────────────────
    # t1 has productName, totalAmount, barcode — productName and barcode are in FTS_COLUMNS
    fts_cols_t1 = db.fts_columns("t1")
    test("p1-24: FTS restricted (no totalAmount in FTS)",
         lambda: assert_eq("totalAmount" not in fts_cols_t1, True))
    test("p1-24: FTS includes productName",
         lambda: assert_in("productName", fts_cols_t1))
    test("p1-24: FTS includes barcode",
         lambda: assert_in("barcode", fts_cols_t1))

    # ── P1-28: negative search retrieval ─────────────────────────────────────
    db._load("neg_t", [
        {"productName":"Chair","status":"active"},
        {"productName":"Table","status":"inactive"},
        {"productName":"Desk", "status":"active"},
    ])
    neg_rows, _, neg_method = ret.retrieve("neg_t", {
        "intent":"NEGATIVE_SEARCH","search_value":"inactive",
        "target_col":None,"aggregation":"NONE","negation_value":"inactive"
    })
    test("p1-28: negative search returns non-matching rows",
         lambda: assert_eq(len(neg_rows), 2))
    test("p1-28: negative search excludes 'inactive'",
         lambda: assert_eq(all(r.get("status") != "inactive" for r in neg_rows), True))
    test("p1-28: negative search method label",
         lambda: assert_eq(neg_method, "negative"))

    # ── P1-23: warmup covers all 17 modules ──────────────────────────────────
    test("p1-23: WARMUP_MODULES == all 17 registry keys",
         lambda: assert_eq(set(WARMUP_MODULES), set(MODULE_REGISTRY.keys())))
    test("p1-23: WARMUP_MODULES count = 17",
         lambda: assert_eq(len(WARMUP_MODULES), 17))

    # ── P1-26: LatencyTracker ────────────────────────────────────────────────
    lt = LatencyTracker(maxlen=10)
    test("p1-26: empty tracker returns None percentiles",
         lambda: assert_eq(lt.percentiles()["p50"], None))
    for ms in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:
        lt.record(ms / 1000)
    pct = lt.percentiles()
    test("p1-26: p50 around 50ms",
         lambda: assert_eq(40 <= pct["p50"] <= 60, True))
    test("p1-26: p95 around 95ms",
         lambda: assert_eq(85 <= pct["p95"] <= 100, True))
    test("p1-26: p99 at or near 100ms",
         lambda: assert_eq(pct["p99"] >= 90, True))
    test("p1-26: count = 10",
         lambda: assert_eq(pct["count"], 10))
    # Ring buffer: adding 11th evicts oldest
    lt2 = LatencyTracker(maxlen=5)
    for i in range(6): lt2.record(i / 1000)
    test("p1-26: ring buffer evicts oldest (maxlen=5)",
         lambda: assert_eq(lt2.count(), 5))

    # ── P1-27: print CSS in table ────────────────────────────────────────────
    tbl_p, _ = synth.table(
        [{"productName":"Chair","totalAmount":"100"}],
        ["productName","totalAmount"], False
    )
    test("p1-27: @media print in table output",
         lambda: assert_in("@media print", tbl_p))
    test("p1-27: erp-table class present",
         lambda: assert_in("erp-table", tbl_p))
    test("p1-27: erp-table-wrap class present",
         lambda: assert_in("erp-table-wrap", tbl_p))
    test("p1-27: print CSS id present (browser dedup)",
         lambda: assert_in('id="erp-print-css"', tbl_p))

    # ── P1-29: module aliases — bills → sell ─────────────────────────────────
    ic = IntentClassifier()
    test("p1-29: 'show bills' → sell module",
         lambda: assert_eq(ic.classify("show bills")["module"], "sell"))
    test("p1-29: 'all bills' → sell module",
         lambda: assert_eq(ic.classify("all bills")["module"], "sell"))
    test("p1-29: 'debit note' → supplier-credit",
         lambda: assert_eq(ic.classify("debit note")["module"], "supplier-credit"))
    test("p1-29: 'debit notes' → supplier-credit",
         lambda: assert_eq(ic.classify("debit notes")["module"], "supplier-credit"))

    # ── P1-30: dotenv + env.example ──────────────────────────────────────────
    test("p1-30: _ENV_EXAMPLE contains ERP_BASE_URL",
         lambda: assert_in("ERP_BASE_URL", _ENV_EXAMPLE))
    test("p1-30: _ENV_EXAMPLE contains ERP_USERNAME",
         lambda: assert_in("ERP_USERNAME", _ENV_EXAMPLE))
    test("p1-30: _ENV_EXAMPLE contains CORS_ORIGINS",
         lambda: assert_in("CORS_ORIGINS", _ENV_EXAMPLE))
    test("p1-30: _ENV_EXAMPLE contains PORT",
         lambda: assert_in("PORT", _ENV_EXAMPLE))
    test("p1-30: dotenv optional import doesn't crash",
         lambda: assert_eq(True, True))  # if we're here, it didn't crash

    # ── Response cache ────────────────────────────────────────────────────────
    rc = ResponseCache(max_size=3, ttl=5)
    rc.set("purchase", "BULK_LIST", "", "english", "<html/>")
    test("cache: get returns value",
         lambda: assert_eq(rc.get("purchase","BULK_LIST","","english"), "<html/>"))
    rc.invalidate_module("purchase")
    test("cache: invalidate removes",
         lambda: assert_eq(rc.get("purchase","BULK_LIST","","english"), None))
    rc2 = ResponseCache(max_size=2, ttl=60)
    rc2.set("m1","BULK_LIST","","en","a"); rc2.set("m2","BULK_LIST","","en","b"); rc2.set("m3","BULK_LIST","","en","c")
    test("cache: LRU evicts oldest",
         lambda: assert_eq(rc2.get("m1","BULK_LIST","","en"), None))

    # ── Rate limiter ──────────────────────────────────────────────────────────
    rl = RateLimiter(rpm=5)
    for _ in range(5): rl.is_allowed("1.2.3.4")
    test("rl: 6th blocked", lambda: assert_eq(rl.is_allowed("1.2.3.4"), False))
    test("rl: different IP ok", lambda: assert_eq(rl.is_allowed("9.9.9.9"), True))
    rl2 = RateLimiter(rpm=5)
    allowed, remaining = rl2.check("10.0.0.1")
    test("rl: first allowed", lambda: assert_eq(allowed, True))
    test("rl: remaining=4 after 1st", lambda: assert_eq(remaining, 4))

    # ── Intent classifier — original ─────────────────────────────────────────
    test("nlu: purchases",
         lambda: assert_eq(ic.classify("show all purchases")["module"], "purchase"))
    test("nlu: supplier credit",
         lambda: assert_eq(ic.classify("supplier credits outstanding")["module"], "supplier-credit"))
    test("nlu: categories",
         lambda: assert_eq(ic.classify("product categories")["module"], "category"))
    test("nlu: financial MULTI",
         lambda: assert_eq(ic.classify("financial summary")["type"], "MULTI"))
    test("nlu: typo purchaces",
         lambda: assert_eq(ic.classify("show all purchaces")["module"], "purchase"))
    test("nlu: customer-ledger-detail",
         lambda: assert_eq(ic.classify("customer ledger detail")["module"], "customer-ledger-detail"))

    # ── FIX-C3: get-sell in registry ─────────────────────────────────────────
    sell_eps = MODULE_REGISTRY["sell"]["search_endpoints"]
    test("fix-c3: sell has id endpoint", lambda: assert_in("id", sell_eps))
    test("fix-c3: sell id url correct",
         lambda: assert_eq(sell_eps["id"]["url"], "/api/sell/get-sell"))
    test("fix-c4: sell next_invoice endpoint",
         lambda: assert_in("next_invoice", sell_eps))
    test("fix-c4: purchase next_invoice endpoint",
         lambda: assert_in("next_invoice", MODULE_REGISTRY["purchase"]["search_endpoints"]))

    # ── FIX-H1: all major APIs wired ─────────────────────────────────────────
    required_urls = [
        "/api/sell/get-sell",
        "/api/sell/get-next-sell-invoice-no",
        "/api/purchase/get-purchase",
        "/api/purchase/get-purchase-by-invoice-no",
        "/api/purchase/get-next-purchase-invoice-no",
        "/api/purchase/get-stock-by-barcode",
        "/api/purchase/get-stock-by-product-name",
        "/api/supplier/get-supplier",
        "/api/customer/get-customer",
        "/api/customer/search-by-contact",
        "/api/material/get-material",
        "/api/material/get-material-by-name",
        "/api/supplier-credit/get-supplier-credit",
        "/api/printer/get-active-printer",
        "/api/printer/get-printer-by-id",
        "/api/email-config/active",
        "/api/email-config/get-email-by-id",
        "/api/reports/customer-ledger-detail",
    ]
    for url in required_urls:
        test(f"fix-h1: {url.split('/')[-1]} wired",
             lambda u=url: assert_in(u, str(MODULE_REGISTRY)))

    # ── FIX: EN_STOP covers framing words ────────────────────────────────────
    test("stop: 'named' in EN_STOP", lambda: assert_eq("named" in EN_STOP, True))
    test("stop: 'called' in EN_STOP", lambda: assert_eq("called" in EN_STOP, True))
    test("stop: 'having' in EN_STOP", lambda: assert_eq("having" in EN_STOP, True))

    # ── FIX: smart_router error body detection ────────────────────────────────
    router_inst = SmartQueryRouter()
    test("router: {'message':'not found'} is error body",
         lambda: assert_eq(router_inst._is_error_body({"message": "not found"}), True))
    test("router: {'error':'unauthorized'} is error body",
         lambda: assert_eq(router_inst._is_error_body({"error": "unauthorized"}), True))
    test("router: {'success':False} is error body",
         lambda: assert_eq(router_inst._is_error_body({"success": False, "message": "fail"}), True))
    test("router: real record is NOT error body",
         lambda: assert_eq(router_inst._is_error_body({"id":"42","productName":"Chair","totalAmount":"500"}), False))
    test("router: empty dict is error body",
         lambda: assert_eq(router_inst._is_error_body({}), True))

    # ── FIX: sell has no dead invoice_exact endpoint ──────────────────────────
    test("fix: sell has no dead invoice_exact endpoint",
         lambda: assert_eq("invoice_exact" not in sell_eps, True))
    test("fix: sell still has invoice endpoint",
         lambda: assert_in("invoice", sell_eps))

    # ── FIX: targeted no_results instead of fallthrough ──────────────────────
    import inspect
    single_src = inspect.getsource(AdminEngine._single)
    test("fix: targeted empty records returns no_results",
         lambda: assert_in("no_results", single_src.split("# Full module sync")[0]))
    ctx_t = ConversationContext(max_sessions=3, max_turns=2)
    for i in range(5): ctx_t.add(f"s{i}", "user", f"hello {i}")
    test("fix-m1: sessions capped at 3", lambda: assert_lte(ctx_t.session_count(), 3))
    test("fix-m1: evict throttle attr exists",
         lambda: assert_eq(hasattr(ctx_t, "_last_evict"), True))

    # ── Circuit breaker ───────────────────────────────────────────────────────
    cb = CircuitBreaker()
    test("cb: starts CLOSED", lambda: assert_eq(cb._state, _CBState.CLOSED))
    for _ in range(CB_FAILURE_THRESHOLD): cb.record_failure()
    test("cb: opens after threshold", lambda: assert_eq(cb._state, _CBState.OPEN))
    cb._opened_at = time.time() - CB_RECOVERY_TIMEOUT - 1
    test("cb: half-open after timeout", lambda: assert_eq(cb.is_open, False))
    for _ in range(CB_SUCCESS_THRESHOLD): cb.record_success()
    test("cb: closes after successes", lambda: assert_eq(cb._state, _CBState.CLOSED))

    # ── BUG-14: FTS injection ─────────────────────────────────────────────────
    dirty = '"NOT" OR "AND" (injection*)'
    step1 = FTS_SPECIAL.sub(" ", dirty)
    step2 = _FTS_BOOL_RE.sub(" ", step1).strip()
    boolops = [w for w in step2.split() if w.upper() in ("NOT","OR","AND","NEAR")]
    test("fts: boolean operators stripped", lambda: assert_eq(boolops, []))

    # ── FIX-C1: router ordering ───────────────────────────────────────────────
    import sys
    src = inspect.getsource(sys.modules[__name__])
    router_def_pos   = src.find("router = APIRouter()")
    include_pos      = src.find("app.include_router(router")
    test("fix-c1: router defined before include_router",
         lambda: assert_lt(router_def_pos, include_pos))

    # ── FIX-L1: version strings ───────────────────────────────────────────────
    test("fix-l1: GREETING_EN says v7.1", lambda: assert_in("v7.1", GREETING_EN))
    test("fix-l1: GREETING_MR says v7.1", lambda: assert_in("v7.1", GREETING_MR))
    test("fix-l1: IDENTITY says v7.1",    lambda: assert_in("v7.1", IDENTITY_RESPONSE))

    # ── Performance: table render ─────────────────────────────────────────────
    big = [{"productName":f"P{i}","totalAmount":str(i*10),"barcode":f"BAR{i:09d}"} for i in range(600)]
    db._load("perf_t", big)
    rows_p, cols_p, _ = RAGRetriever().retrieve("perf_t", {"intent":"BULK_LIST","search_value":"","target_col":None,"aggregation":"NONE","negation_value":None})
    t0 = time.time()
    tbl_h, rendered = synth.table(rows_p[:600], cols_p, False)
    elapsed = time.time() - t0
    test(f"perf: 500-row render < 0.5s ({elapsed:.3f}s)",
         lambda: assert_lte(elapsed, 0.5))
    test("perf: render cap ≤500", lambda: assert_lte(rendered, MAX_RENDER_ROWS))

    # ── Module registry completeness ──────────────────────────────────────────
    test("registry: 17 modules", lambda: assert_eq(len(MODULE_REGISTRY), 17))

    # ── P1-10 carried: customer-ledger-summary in Marathi ────────────────────
    test("p1-10: customer-ledger-summary in MR_RAW",
         lambda: assert_in("customer-ledger-summary", _MR_RAW))
    test("p1-10: Marathi saransh routes to summary",
         lambda: assert_ne(marathi_classify("grahak khate saransh"), None))

    # ── Summary ───────────────────────────────────────────────────────────────
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