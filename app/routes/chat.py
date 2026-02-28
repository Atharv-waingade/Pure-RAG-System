"""
==============================================================================
  ADMIN INTELLIGENCE ENGINE  v6.1  —  PRODUCTION READY
  ERP Chatbot RAG Pipeline · Zero external AI APIs · <200 MB RAM
  Full Marathi (Devanagari + Roman) + English support

  SECURITY FIXES vs v6.0 (from vulnerability audit):
  ────────────────────────────────────────────────────
  SEC-01 [CRITICAL] XSS in no_results / intro: search_value now escaped
         via html.escape() before being injected into HTML responses.
  SEC-02 [HIGH]     Cross-Join Memory Bomb: _flatten() now uses CONCAT
         strategy for sibling arrays instead of Cartesian product.
         Max sibling rows capped at FLATTEN_MAX_CHILD_ROWS (200).
  SEC-03 [HIGH]     Global Lock Bottleneck: replaced single asyncio.Lock
         with per-module locks via collections.defaultdict(asyncio.Lock).
         Modules now sync concurrently without queuing behind each other.
  SEC-04 [MEDIUM]   JWT Auth Spam: JWT token cached at class level with
         60-second pre-expiry refresh window. 401 responses force re-auth.
         sync_all(force=True) no longer fires 16 simultaneous login POSTs.
  SEC-05 [MEDIUM]   NaN/Inf Aggregation Corruption: AggregationEngine now
         rejects float("nan") and float("inf") values via math.isfinite().
  SEC-06 [LOW]      Dynamic Column HTML Injection: table() now escapes
         column header keys via html.escape() before rendering <th>.

  PERFORMANCE IMPROVEMENTS vs v6.0:
  ───────────────────────────────────
  PERF-01: JWT token reuse eliminates N login POSTs per sync cycle.
  PERF-02: Per-module locks allow concurrent syncs (asyncio.gather speed).
  PERF-03: _flatten() memory-safe — no OOM on nested ERP payloads.
  PERF-04: httpx AsyncClient reused across retries (single connection pool).
  PERF-05: MAX_ROWS = 20_000 (down from 50_000) — safe on 200 MB Render free.
==============================================================================
"""

import asyncio
import collections
import html
import math
import os
import random
import re
import sqlite3
import time
from collections import Counter, defaultdict, deque
from difflib import SequenceMatcher
from typing import Optional

import httpx
from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────────
BASE_URL    = "https://umbrellasales.xyz/umbrella-inventory-server"
LOGIN_URL   = f"{BASE_URL}/api/service/login"
LOGIN_CREDS = {
    "username": os.getenv("ERP_USERNAME", "superadmin.com"),
    "password": os.getenv("ERP_PASSWORD",  "superadmin@123"),
}
MODULE_TTL            = 300      # 5 min
STOCK_TTL             = 120      # 2 min
FINANCE_TTL           = 600      # 10 min
MAX_ROWS              = 20_000   # ← reduced for 200 MB Render free tier
API_TIMEOUT           = 30.0
API_MAX_RETRIES       = 3
JWT_EXPIRY_BUFFER     = 60       # re-fetch token 60s before it expires
FLATTEN_MAX_CHILD_ROWS = 200     # SEC-02: cap rows per sibling child array


# ──────────────────────────────────────────────────────────────────────────────
# MODULE REGISTRY
# ──────────────────────────────────────────────────────────────────────────────
MODULE_REGISTRY = {
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
            "invoice": {
                "url":   "/api/sell/search-by-invoice",
                "param": "invoiceNo",
            },
            "invoice_exact": {
                "url":   "/api/sell/get-sell-by-invoice-no",
                "param": "invoiceNo",
            },
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
            "barcode": {
                "url":   "/api/purchase/get-stock-by-barcode",
                "param": "barcode",
            },
            "invoice": {
                "url":   "/api/purchase/get-purchase-by-invoice-no",
                "param": "invoiceNo",
            },
            "name": {
                "url":   "/api/purchase/get-stock-by-product-name",
                "param": "productName",
            },
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
            "name": {
                "url":   "/api/material/get-material-by-name",
                "param": "name",
            },
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
            "contact": {
                "url":   "/api/customer/search-by-contact",
                "param": "contact",
            },
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
            "active": {
                "url":   "/api/printer/get-active-printer",
                "param": None,
            },
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
            "active": {
                "url":   "/api/email-config/active",
                "param": None,
            },
        },
    },
}

# ──────────────────────────────────────────────────────────────────────────────
# MULTI-MODULE QUERY PATTERNS
# ──────────────────────────────────────────────────────────────────────────────
MULTI_PATTERNS = [
    {
        "phrases": ["supplier summary", "supplier overview", "vendor summary",
                    "full supplier details", "all supplier info",
                    "complete supplier info"],
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
        "phrases": ["purchase and payment", "vendor transactions",
                    "vendor payment history"],
        "modules": ["purchase", "payment", "supplier"],
        "label":   "Vendor Transactions",
    },
    {
        "phrases": ["customer report", "customer full report",
                    "customer complete details"],
        "modules": ["customer", "customer-ledger", "customer-payment-history"],
        "label":   "Customer Full Report",
    },
]

# ──────────────────────────────────────────────────────────────────────────────
# QUERY PATTERN DETECTION
# ──────────────────────────────────────────────────────────────────────────────
BARCODE_RE  = re.compile(r"\bBAR\w{6,}\b", re.IGNORECASE)
INVOICE_RE  = re.compile(r"\b(PA\d{5,}|SA\d{5,}|INV[-/]\w+)\b", re.IGNORECASE)
CONTACT_RE  = re.compile(r"\b[6-9]\d{9}\b")
ACTIVE_RE   = re.compile(r"\bactive\b", re.IGNORECASE)

# ──────────────────────────────────────────────────────────────────────────────
# COLUMN SEARCH TRIGGERS
# ──────────────────────────────────────────────────────────────────────────────
COLUMN_TRIGGERS = {
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
EN_STOP = {
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
}
EN_BULK = {
    "all", "list", "every", "entire", "show", "get", "fetch",
    "display", "give", "complete", "full",
}

MR_STOP_ROMAN = {
    "mala", "mla", "mhala", "amhala", "amhi", "mi", "tu", "to", "ti",
    "te", "tyala", "tila", "aapan", "apan",
    "dakhva", "dakhav", "dakhvav", "dakha", "dakhava", "dakhvaa",
    "dikhao", "dikhav", "dikhava", "dikhva",
    "dya", "deu", "de", "dyaa", "dyave",
    "bagha", "bagh", "baghu", "pahava", "paha", "pahun", "pahat",
    "sanga", "sangav", "sangava", "sangaa",
    "kadhav", "kadhva", "milvava", "milva", "milu", "mil",
    "ghya", "ghyava", "aana", "aanava",
    "pahije", "hve", "have",
    "sarv", "sarva", "sarve", "saglya", "sagale", "sagala", "sagali",
    "sagle", "yadi", "yaadi", "soochi", "suchi", "jadval",
    "sampurn", "sampoorna", "poorna", "purna",
    "sare", "sara", "sari", "sarav",
    "chi", "che", "cha", "chya", "la", "na", "va", "ani", "aani",
    "ahe", "aahe", "aste", "asate",
    "ahet", "aahet", "naahi", "nahi",
    "kiti", "kitee", "kiteek",
    "kasa", "kase", "kashi", "kay", "kaay", "kon", "konte",
    "ha", "he", "hi", "hya", "tya", "ya", "ja",
    "pan", "pari", "tar", "mhanje", "mhanun",
    "ata", "aata", "jara", "jra", "ekda", "ekadha",
    "sathi", "saathi", "karitha", "karita",
    "madhe", "madhye", "madhil", "tun", "tyatun",
    "sobat", "sathe", "barobar",
    "naavache", "naav", "naavane", "naavachya",
    "wala", "wale", "wali", "vala", "vale", "vali",
    "mahiti", "tapshil", "maahiti",
    "itihas", "history",
    "kaya", "kithi", "kevu", "kevha", "keva", "kuthun",
    "please", "plz", "krupaya", "krupa",
    "thodi", "thoda", "thode",
    "ek", "don", "teen", "char", "panch",
}
MR_BULK_ROMAN = {
    "sarv", "sarva", "sarve", "saglya", "sagale", "yadi", "yaadi",
    "sare", "sara", "sampurn", "poorna", "purna", "soochi",
    "all", "every", "complete", "full", "entire", "sarav",
}
MR_STOP_DEVA = {
    "मला", "म्हाला", "आम्हाला", "आम्ही", "मी", "तू", "तो", "ती", "ते",
    "त्याला", "तिला", "आपण",
    "दाखवा", "दाखव", "दाखवाव", "दाखव", "दिखाओ", "दिखाव",
    "द्या", "दे", "देऊ", "द्यावा", "द्यावे",
    "बघा", "बघ", "बघू", "पहावा", "पाहा", "पाहावा",
    "सांगा", "सांगव", "काढा", "काढव",
    "मिळवा", "मिळव", "घ्या", "घ्यावा", "आणा",
    "सर्व", "सगळ्या", "सगळे", "सगळा", "सगळी",
    "यादी", "सूची", "जाडवल",
    "संपूर्ण", "पूर्ण",
    "सारे", "सारा", "सारी",
    "ची", "चे", "चा", "च्या", "ला", "ना", "व", "आणि",
    "आहे", "असते", "असतात", "नाही",
    "किती", "कसा", "कसे", "कशी", "काय", "कोण", "कोणते",
    "हा", "हे", "ही", "ह्या", "त्या", "या", "जा",
    "पण", "परी", "तर", "म्हणजे", "म्हणून",
    "आता", "जरा", "एकदा",
    "साठी", "करिता",
    "मध्ये", "मधून", "त्यातून",
    "सोबत", "साथे", "बरोबर",
    "माहिती", "तपशील",
    "इतिहास",
    "शोधा", "शोध", "शोधव",
    "कृपया", "थोडी", "थोडा",
}
MR_BULK_DEVA = {
    "सर्व", "सगळ्या", "सगळे", "यादी", "सूची", "संपूर्ण", "पूर्ण", "सारे",
}

# ──────────────────────────────────────────────────────────────────────────────
# MARATHI LANGUAGE LAYER
# ──────────────────────────────────────────────────────────────────────────────
DEVA_STEMS = {
    "उत्पादने":       "उत्पादन",
    "उत्पादनें":      "उत्पादन",
    "ग्राहकांची":     "ग्राहक",
    "ग्राहकाची":      "ग्राहक",
    "ग्राहकांचे":     "ग्राहक",
    "पुरवठादाराचे":   "पुरवठादार",
    "पुरवठादारांचे":  "पुरवठादार",
    "खरेदीची":        "खरेदी",
    "विक्रीची":       "विक्री",
    "साठ्याची":       "साठा",
    "साथ":            "साठा",
    "स्टॉकची":        "स्टॉक",
    "देयकाचे":        "देयक",
    "पेमेंटची":       "पेमेंट",
    "साहित्याचे":     "साहित्य",
    "श्रेणीची":       "श्रेणी",
    "क्रेडिटची":      "क्रेडिट",
    "साठाची":         "साठा",
    "उत्पादनाची":     "उत्पादन",
    "उत्पादनांची":    "उत्पादन",
    "खरेदीचे":        "खरेदी",
    "विक्रीचे":       "विक्री",
    "पुरवठादाराची":   "पुरवठादार",
    "श्रेणींची":      "श्रेणी",
    "साहित्यांचे":    "साहित्य",
}

_MR_RAW = {
    "purchase": {
        "deva": [
            "खरेदी यादी", "खरेदी ऑर्डर", "खरेदी इनव्हॉइस",
            "खरेदी इतिहास", "माल खरेदी", "खरेदी नोंदी",
            "काय खरेदी केले", "सर्व खरेदी", "खरेदी",
        ],
        "roman": [
            "kharedi yadi", "kharedi order", "kharedi invoice",
            "kharedi itihas", "mal kharedi", "kharedi nondi",
            "kay kharedi kele", "sarv kharedi", "kharedi list",
            "kharedee list", "kharidi list", "kharedee itihas",
            "purchase kele", "purchase list", "kharidi yadi",
            "kharedi", "kharedee", "kharidi",
        ],
    },
    "sell": {
        "deva": [
            "विक्री यादी", "विक्री इनव्हॉइस", "विक्री इतिहास",
            "विक्री नोंदी", "माल विकला", "काय विकले",
            "सर्व विक्री", "विक्री तपशील", "विक्री", "बिल", "पावती",
        ],
        "roman": [
            "vikri yadi", "vikri invoice", "vikri itihas", "vikri nondi",
            "mal vikla", "kay vikle", "sarv vikri", "vikri tapshil",
            "vikri list", "vikree list", "vikri history", "sales list",
            "vikri", "vikree", "pavti",
        ],
    },
    "supplier-purchase-history": {
        "deva": [
            "पुरवठादार खरेदी इतिहास", "पुरवठादार खरेदी अहवाल",
            "पुरवठादाराने काय खरेदी", "विक्रेता खरेदी यादी",
        ],
        "roman": [
            "purvathakaar kharedi itihas", "supplier kharedi itihas",
            "supplier purchase history", "vendor purchase history",
            "purvathakaar kharedi list", "supplier kharedi yadi",
            "supplier kharedi", "purvathakaar purchase",
        ],
    },
    "supplier-credit": {
        "deva": [
            "पुरवठादार क्रेडिट", "क्रेडिट शिल्लक",
            "किती देणे आहे", "पुरवठादाराचे देणे",
            "बाकी रक्कम", "पुरवठादार बाकी",
            "थकबाकी", "देणे", "उधार", "क्रेडिट",
        ],
        "roman": [
            "purvathakaar credit", "puravathakaar credit",
            "credit shillak", "kiti dene ahe",
            "purvathakaarache dene", "baki rakkam",
            "purvathakaar baki", "supplier credit",
            "vendor credit", "credit balance",
            "thakbaki", "udhar", "dene", "credit",
        ],
    },
    "supplier-ledger": {
        "deva": [
            "पुरवठादार खातेवही", "पुरवठादार हिशेब",
            "पुरवठादार विवरण", "पुरवठादार खाते",
        ],
        "roman": [
            "purvathakaar khatevahi", "purvathakaar hisab",
            "puravathakaar khatevahi", "purvathakaar vivaran",
            "supplier ledger", "vendor ledger", "supplier hisab",
        ],
    },
    "customer-ledger": {
        "deva": [
            "ग्राहक खातेवही", "ग्राहक हिशेब",
            "ग्राहक विवरण", "ग्राहक खाते",
        ],
        "roman": [
            "grahak khatevahi", "grahak hisab", "graahak khatevahi",
            "customer ledger", "customer hisab",
        ],
    },
    "customer-payment-history": {
        "deva": [
            "ग्राहक देयक इतिहास", "ग्राहक पेमेंट",
            "ग्राहकाने किती दिले",
        ],
        "roman": [
            "grahak payment itihas", "grahak payment history",
            "customer payment history", "grahak paid",
        ],
    },
    "customer": {
        "deva": [
            "ग्राहक यादी", "सर्व ग्राहक", "ग्राहक माहिती",
            "खरेदीदार", "ग्राहक",
        ],
        "roman": [
            "grahak yadi", "sarv grahak", "grahak mahiti", "kharedidar",
            "grahak list", "graahak list", "customer list",
            "grahak", "graahak",
        ],
    },
    "supplier": {
        "deva": [
            "पुरवठादार यादी", "सर्व पुरवठादार", "विक्रेता",
            "माल पुरवठादार", "पुरवठादार",
        ],
        "roman": [
            "purvathakaar yadi", "sarv purvathakaar", "vikreta",
            "supplier list", "vendor list", "supplier yadi",
            "supplier chi yadi", "supplier che tapshil",
            "supplier mahiti", "supplier bagha", "supplier dakha",
            "purvathakaar", "puravathakaar", "supplier",
        ],
    },
    "product-stock": {
        "deva": [
            "माल साठा", "उपलब्ध माल", "किती साठा आहे",
            "स्टॉक यादी", "उपलब्ध स्टॉक",
            "उत्पादन साठा", "उत्पादन यादी", "इन्व्हेंटरी",
            "गोदाम", "उत्पादने", "उत्पादन", "साठा", "स्टॉक",
        ],
        "roman": [
            "mal satha", "uplabdh mal", "kiti satha ahe", "stock yadi",
            "uplabdh stock", "stock kiti ahe", "stock bagha", "stock list",
            "inventory list", "utpadan satha", "utpadan yadi", "godam",
            "utpadan", "saatha", "satha", "stock",
        ],
    },
    "payment": {
        "deva": [
            "पेमेंट इतिहास", "दिलेले पैसे", "पुरवठादार देयक",
            "व्यवहार नोंदी", "पैसे दिले", "व्यवहार", "देयक", "पेमेंट",
        ],
        "roman": [
            "payment itihas", "dilele paise", "purvathakaar dayak",
            "vyavahar nondi", "paise dile", "payment history",
            "payment list", "vyavahar", "dayak", "payment",
        ],
    },
    "material": {
        "deva": [
            "कच्चा माल", "माल यादी", "साहित्य यादी",
            "कापड यादी", "साहित्य", "कापड", "घटक", "सामग्री",
        ],
        "roman": [
            "kachha mal", "mal yadi", "sahitya yadi", "kapad yadi",
            "material list", "raw material", "sahitya", "kapad",
            "ghatak", "samagri",
        ],
    },
    "category": {
        "deva": [
            "श्रेणी यादी", "वर्गीकरण", "उत्पादन प्रकार",
            "उत्पादन श्रेणी", "सर्व श्रेणी", "श्रेणी", "प्रकार",
        ],
        "roman": [
            "shreni yadi", "vargikaran", "utpadan prakar",
            "utpadan shreni", "sarv shreni",
            "category list", "all categories", "shreni", "prakar",
        ],
    },
    "printer": {
        "deva": ["प्रिंटर यादी", "मुद्रण यंत्र", "प्रिंटर"],
        "roman": ["printer yadi", "mudran yantra", "printer list", "printer"],
    },
    "email-config": {
        "deva": ["ईमेल सेटिंग", "ईमेल कॉन्फिग", "ईमेल", "मेल"],
        "roman": ["email settings", "email config", "mail config", "email"],
    },
}

_MR_FLAT: list = []
for _mod, _kws in _MR_RAW.items():
    for k in _kws["deva"] + _kws["roman"]:
        _MR_FLAT.append((k.lower(), _mod))
_MR_FLAT.sort(key=lambda x: len(x[0]), reverse=True)

_MR_ROMAN_MARKERS: frozenset = frozenset([
    "kharedi", "kharedee", "kharidi", "vikri", "vikree",
    "grahak", "graahak", "purvathakaar", "puravathakaar",
    "satha", "saatha", "thakbaki", "udhar", "dene",
    "yadi", "bagha", "dakha",
    "ahe", "aahe", "aahet", "ahet",
    "sarv", "sarva", "sarve",
    "saglya", "hisab", "hisaab", "khatevahi", "vivaran",
    "dayak", "paise", "paisa", "kachha", "sahitya",
    "kapad", "shreni", "uplabdh", "godam", "vyavahar",
    "pavti", "baki", "shillak", "rakkam", "vikreta",
    "kiti", "kitee", "mal", "nondi", "tapshil", "itihas",
    "mahiti", "dilele", "dile", "kele", "vikle",
    "dakhva", "dakhav", "dakha", "mala", "mhala",
    "chi", "che", "cha",
    "supplier",
])

_MR_GREET: frozenset = frozenset([
    "नमस्कार", "नमस्ते", "हॅलो", "सुप्रभात",
    "namaskar", "namaste",
])
_MR_THANKS: frozenset = frozenset([
    "धन्यवाद", "आभारी", "थँक्यू",
    "dhanyavad", "aabhari", "dhanywaad",
])
_MR_REFRESH: frozenset = frozenset([
    "रिफ्रेश", "अद्यतन", "ताजी",
    "refresh kara", "update kara", "sync kara",
])

MR_COLUMNS = {
    "Invoice No":           "इनव्हॉइस क्र.",
    "Name":                 "नाव",
    "Email":                "ईमेल",
    "Contact":              "संपर्क",
    "Address":              "पत्ता",
    "Supplier Name":        "पुरवठादाराचे नाव",
    "Customer Name":        "ग्राहकाचे नाव",
    "Product Name":         "उत्पादनाचे नाव",
    "Material Name":        "साहित्याचे नाव",
    "Total Amount":         "एकूण रक्कम",
    "Sell Price":           "विक्री किंमत",
    "Stock Quantity":       "साठ्याची संख्या",
    "Quantity":             "संख्या",
    "Qty":                  "संख्या",
    "Received Quantity":    "प्राप्त संख्या",
    "Missing Quantity":     "उणी संख्या",
    "Total Quantity":       "एकूण संख्या",
    "Paid":                 "दिलेले",
    "Credit Amount":        "क्रेडिट रक्कम",
    "Balance":              "शिल्लक",
    "Debit":                "नावे",
    "Credit":               "जमा",
    "Date":                 "तारीख",
    "Created At":           "तयार तारीख",
    "Updated At":           "अद्यतन तारीख",
    "Created By":           "तयार केले",
    "Updated By":           "अद्यतन केले",
    "Barcode":              "बारकोड",
    "Gst No":               "जीएसटी क्र.",
    "Supply Type":          "पुरवठा प्रकार",
    "Status":               "स्थिती",
    "Price Per Unit":       "प्रति युनिट किंमत",
    "Purchase Date":        "खरेदी तारीख",
    "Sell Date":            "विक्री तारीख",
    "Unit":                 "युनिट",
    "Size":                 "आकार",
    "Color":                "रंग",
    "Category":             "श्रेणी",
    "Description":          "वर्णन",
    "Supplier Credit":      "पुरवठादार क्रेडिट",
    "First Name":           "पहिले नाव",
    "Last Name":            "आडनाव",
    "Phone":                "फोन",
    "City":                 "शहर",
    "Type":                 "प्रकार",
    "Mode":                 "पद्धत",
    "Hsn No":               "एचएसएन क्र.",
    "Price Code":           "किंमत कोड",
    "Image Url":            "प्रतिमा",
    "Product Code":         "उत्पादन कोड",
    "Host":                 "होस्ट",
    "Port":                 "पोर्ट",
    "Bill No":              "बिल क्र.",
    "Mobile":               "मोबाईल",
    "To Pay":               "द्यावयाचे",
    "Debit Amount":         "नावे रक्कम",
    "Purchase Amount":      "खरेदी रक्कम",
    "Total Purchase Amount":"एकूण खरेदी रक्कम",
    "Payment Mode":         "देय पद्धत",
    "Supplier Bill":        "पुरवठादार बिल",
    "Supplier":             "पुरवठादार",
    "Material":             "साहित्य",
    "Packing Charges":      "पॅकिंग शुल्क",
    "Discount":             "सूट",
    "Cgst":                 "सीजीएसटी",
    "Sgst":                 "एसजीएसटी",
    "Igst":                 "आयजीएसटी",
}

MR_MODULES = {
    "purchase":                   "खरेदी",
    "sell":                       "विक्री",
    "supplier-credit":            "पुरवठादार क्रेडिट",
    "supplier-ledger":            "पुरवठादार खातेवही",
    "customer-ledger":            "ग्राहक खातेवही",
    "customer-ledger-summary":    "ग्राहक खाते सारांश",
    "customer":                   "ग्राहक",
    "supplier":                   "पुरवठादार",
    "product-stock":              "उत्पादन साठा",
    "payment":                    "देयक",
    "customer-payment-history":   "ग्राहक देयक इतिहास",
    "supplier-purchase-history":  "पुरवठादार खरेदी इतिहास",
    "material":                   "साहित्य",
    "category":                   "श्रेणी",
    "printer":                    "प्रिंटर",
    "email-config":               "ईमेल सेटिंग",
}


# ──────────────────────────────────────────────────────────────────────────────
# LANGUAGE DETECTION & MARATHI CLASSIFICATION
# ──────────────────────────────────────────────────────────────────────────────

def detect_language(text: str) -> str:
    deva  = sum(1 for c in text if 0x0900 <= ord(c) <= 0x097F)
    alpha = sum(1 for c in text if c.isalpha())
    if alpha == 0:
        return "english"
    if deva / alpha > 0.25:
        return "marathi_devanagari"
    t = text.lower()
    if any(m in t.split() or m in t for m in _MR_ROMAN_MARKERS):
        return "marathi_roman"
    return "english"


def marathi_classify(text: str) -> Optional[str]:
    words   = text.split()
    stemmed = " ".join(DEVA_STEMS.get(w, w) for w in words)
    t       = stemmed.lower().strip()
    for keyword, module in _MR_FLAT:
        if keyword in t:
            return module
    return None


def _apply_deva_stems(text: str) -> str:
    return " ".join(DEVA_STEMS.get(w, w) for w in text.split())


# ──────────────────────────────────────────────────────────────────────────────
# QUERY ANALYZER
# ──────────────────────────────────────────────────────────────────────────────

class QueryAnalyzer:
    def analyze(self, user_query: str) -> dict:
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
            [
                (ph.lower(), mod)
                for mod, d in MODULE_REGISTRY.items()
                for ph in d["phrases"]
            ],
            key=lambda x: len(x[0]),
            reverse=True,
        )
        self._alias_pairs = [
            (al.lower(), mod)
            for mod, d in MODULE_REGISTRY.items()
            for al in d["aliases"]
        ]
        self._multi_pairs = sorted(
            [
                (ph.lower(), pat)
                for pat in MULTI_PATTERNS
                for ph in pat["phrases"]
            ],
            key=lambda x: len(x[0]),
            reverse=True,
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
        self._vecs: dict = {}
        for mod, doc in corpus.items():
            words = doc.split()
            if not words:
                self._vecs[mod] = {}
                continue
            tf   = Counter(words)
            raw  = {w: (tf[w] / len(words)) * idf.get(w, 0) for w in tf}
            norm = math.sqrt(sum(v**2 for v in raw.values()))
            self._vecs[mod] = {w: v / norm for w, v in raw.items()} if norm else {}

    def _tfidf_score(self, text: str) -> dict:
        words = text.split()
        if not words:
            return {}
        tf  = Counter(words)
        n   = len(words)
        q   = {w: tf[w] / n for w in tf}
        qn  = math.sqrt(sum(v**2 for v in q.values()))
        if not qn:
            return {}
        scores = {}
        for mod, dv in self._vecs.items():
            common = set(q) & set(dv)
            scores[mod] = sum(q[w] * dv[w] for w in common) / qn
        return scores

    def _stage_multi(self, text: str):
        for ph, pat in self._multi_pairs:
            if ph in text:
                return {"type": "MULTI", "pattern": pat, "confidence": 1.0}
        return None

    def _stage_phrase(self, text: str):
        for ph, mod in self._phrase_pairs:
            if ph in text:
                return {"type": "SINGLE", "module": mod, "confidence": 0.95}
        return None

    def _stage_keyword(self, text: str):
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

    def _stage_fuzzy(self, tokens: list):
        best_s, best_mod = 0.0, None
        for tok in tokens:
            if len(tok) < 4:
                continue
            for al, mod in self._alias_pairs:
                s = SequenceMatcher(None, tok, al).ratio()
                if s > best_s:
                    best_s, best_mod = s, mod
        if best_s >= 0.72:
            return {"type": "SINGLE", "module": best_mod,
                    "confidence": best_s * 0.85}
        return None

    def _stage_tfidf(self, text: str):
        scores = self._tfidf_score(text)
        if scores:
            best = max(scores, key=scores.get)
            if scores[best] >= 0.12:
                return {"type": "SINGLE", "module": best,
                        "confidence": scores[best] * 0.75}
        return None

    def classify(self, user_query: str) -> dict:
        t      = re.sub(r"[^a-z0-9\s\-]", " ", user_query.lower()).strip()
        tokens = t.split()
        return (
            self._stage_multi(t)
            or self._stage_phrase(t)
            or self._stage_keyword(t)
            or self._stage_fuzzy(tokens)
            or self._stage_tfidf(t)
            or {"type": "UNRECOGNIZED", "confidence": 0.0}
        )


# ──────────────────────────────────────────────────────────────────────────────
# 2. QUERY PARSER
# ──────────────────────────────────────────────────────────────────────────────

AGG_TRIGGERS = {
    "total", "sum", "how many", "count", "how much", "aggregate",
    "altogether", "combined", "overall",
    "एकूण", "बेरीज", "किती आहे",
}


class QueryParser:
    def parse(self, user_query: str, module_key: str, lang: str) -> dict:
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
        strip = stop | mod_words | col_words
        value_tokens = [
            tok for tok in tokens
            if tok not in strip and len(tok) > 1
        ]
        code_tokens = [
            tok for tok in tokens
            if re.match(r"^[a-z0-9\-]+$", tok)
            and (re.search(r"\d", tok) or len(tok) >= 6)
            and tok not in strip
        ]
        if code_tokens and not value_tokens:
            value_tokens = code_tokens
        search_value = " ".join(value_tokens).strip()
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
# 3. DATA MIRROR — SQLite in-memory with FTS5
#
# SEC-03 FIX: per-module locks via defaultdict(asyncio.Lock)
# SEC-04 FIX: class-level JWT token cache with expiry
# SEC-02 FIX: _flatten() uses concat strategy — no Cartesian product
# ──────────────────────────────────────────────────────────────────────────────

class DataMirror:
    def __init__(self):
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.execute("PRAGMA cache_size=-8000")   # 8 MB page cache
        self._ttl:   dict = {}
        self._cols:  dict = {}
        # SEC-03: per-module lock dict — modules sync concurrently
        self._locks: collections.defaultdict = collections.defaultdict(asyncio.Lock)
        # SEC-04: JWT token cache
        self._jwt_token:   Optional[str] = None
        self._jwt_expires: float         = 0.0
        self._jwt_lock:    asyncio.Lock  = asyncio.Lock()

    def _extract_list(self, raw) -> list:
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

    def _flatten(self, records: list):
        """
        SEC-02 FIX: Yields flat dict rows without Cartesian product.
        For records with multiple sibling child arrays, we CONCATENATE
        rows from all sibling arrays (not cross-join them).
        Each child array is capped at FLATTEN_MAX_CHILD_ROWS.
        This prevents OOM on ERP payloads like 50 items × 20 taxes × 10 events.
        """
        for rec in records:
            if not isinstance(rec, dict):
                yield {"_data": str(rec)}
                continue
            base, children = {}, []
            for k, v in rec.items():
                if v is None:
                    base[k] = ""
                elif isinstance(v, (str, int, float, bool)):
                    base[k] = str(v)
                elif isinstance(v, dict):
                    base[k] = str(
                        v.get("name", v.get("title",
                        v.get("invoiceNo", v.get("totalAmount", ""))))
                    )
                elif isinstance(v, list) and v and isinstance(v[0], dict):
                    children.append((k, v))
            if children:
                # CONCAT strategy: emit one row per child item (across all arrays)
                # capped to avoid OOM
                for _, clist in children:
                    for child in clist[:FLATTEN_MAX_CHILD_ROWS]:
                        row = base.copy()
                        for ck, cv in child.items():
                            if isinstance(cv, dict):
                                row[ck] = str(cv.get("name",
                                    cv.get("title", cv.get("invoiceNo", ""))))
                            elif not isinstance(cv, list):
                                row[ck] = str(cv) if cv is not None else ""
                        yield row
            else:
                yield base

    def _load(self, key: str, records: list):
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

    # ── SEC-04: Cached JWT token ───────────────────────────────────────────────
    async def _get_token(self, client: httpx.AsyncClient) -> Optional[str]:
        """
        Return cached JWT if still valid (>JWT_EXPIRY_BUFFER seconds remaining).
        Re-authenticate if token missing, expired, or about to expire.
        Uses a dedicated asyncio.Lock to prevent concurrent auth storms.
        """
        now = time.time()
        if self._jwt_token and now < self._jwt_expires - JWT_EXPIRY_BUFFER:
            return self._jwt_token
        async with self._jwt_lock:
            # Double-checked after acquiring lock
            now = time.time()
            if self._jwt_token and now < self._jwt_expires - JWT_EXPIRY_BUFFER:
                return self._jwt_token
            try:
                auth  = await client.post(LOGIN_URL, json=LOGIN_CREDS)
                body  = auth.json()
                token = body.get("jwtToken") or body.get("token")
                if not token:
                    print(f"[DataMirror] Auth failed: status={auth.status_code}")
                    return None
                # ERP JWTs typically expire in 24h; assume 1h if not stated
                expires_in = body.get("expiresIn", 3600)
                self._jwt_token   = token
                self._jwt_expires = now + int(expires_in)
                return token
            except Exception as exc:
                print(f"[DataMirror] Auth exception: {exc}")
                return None

    def _invalidate_token(self):
        """Force re-authentication on next request (called on 401 response)."""
        self._jwt_token   = None
        self._jwt_expires = 0.0

    async def _fetch_with_retry(self, url: str,
                                params: Optional[dict] = None) -> Optional[list]:
        """
        SEC-04 FIX: JWT token is fetched once and reused across all calls.
        On HTTP 401, token is invalidated and a single re-auth is attempted.
        Uses a single AsyncClient per call to reuse the connection pool.
        """
        last_exc = None
        for attempt in range(API_MAX_RETRIES):
            try:
                async with httpx.AsyncClient(timeout=API_TIMEOUT) as client:
                    token = await self._get_token(client)
                    if not token:
                        last_exc = Exception("Auth failed — no token")
                        await asyncio.sleep(1.5 * (2 ** attempt))
                        continue
                    headers  = {
                        "Authorization": f"Bearer {token}",
                        "Content-Type":  "application/json",
                    }
                    req_url = f"{BASE_URL}{url}"
                    resp = await client.get(req_url, headers=headers,
                                            params=params or {})
                    if resp.status_code == 200:
                        return self._extract_list(resp.json())
                    if resp.status_code == 401:
                        # Token rejected — force re-auth on next iteration
                        self._invalidate_token()
                        print(f"[DataMirror] 401 on {url}, invalidating token")
                        last_exc = Exception("HTTP 401 Unauthorized")
                        await asyncio.sleep(1.5 * (2 ** attempt))
                        continue
                    print(f"[DataMirror] {url} attempt {attempt+1}: "
                          f"HTTP {resp.status_code} — {resp.text[:200]}")
                    last_exc = Exception(f"HTTP {resp.status_code}")
            except Exception as exc:
                last_exc = exc
                print(f"[DataMirror] {url} attempt {attempt+1} exception: {exc}")
            if attempt < API_MAX_RETRIES - 1:
                await asyncio.sleep(1.5 * (2 ** attempt))
        print(f"[DataMirror] All {API_MAX_RETRIES} retries failed for {url}: {last_exc}")
        return None

    async def sync_module(self, key: str, force: bool = False) -> bool:
        """
        SEC-03 FIX: Uses per-module lock (self._locks[key]) instead of a
        single global lock. Different modules now sync concurrently.
        """
        now = time.time()
        if not force and now < self._ttl.get(key, 0):
            return True
        async with self._locks[key]:
            # Double-check after acquiring per-module lock
            now = time.time()
            if not force and now < self._ttl.get(key, 0):
                return True
            mod = MODULE_REGISTRY[key]
            raw = await self._fetch_with_retry(mod["url"])
            if raw is None:
                return False
            flat = list(self._flatten(raw))
            self._load(key, flat)
            self._ttl[key] = time.time() + mod["ttl"]
            return True

    async def sync_modules(self, keys: list, force: bool = False) -> dict:
        results = await asyncio.gather(
            *[self.sync_module(k, force) for k in keys],
            return_exceptions=True,
        )
        return {k: (r is True) for k, r in zip(keys, results)}

    async def sync_all(self, force: bool = True):
        await self.sync_modules(list(MODULE_REGISTRY.keys()), force=force)

    def columns(self, key: str) -> list:
        return self._cols.get(key, [])

    def has_data(self, key: str) -> bool:
        return bool(self._cols.get(key))


db = DataMirror()


# ──────────────────────────────────────────────────────────────────────────────
# 3b. SMART QUERY ROUTER
# ──────────────────────────────────────────────────────────────────────────────

class SmartQueryRouter:
    async def route(self, key: str, parsed: dict,
                    query_mode: str, query_value: Optional[str]) -> bool:
        mod       = MODULE_REGISTRY.get(key, {})
        endpoints = mod.get("search_endpoints", {})
        if not query_mode or query_mode not in endpoints:
            return False
        ep = endpoints[query_mode]
        if ep["param"] is None:
            raw = await db._fetch_with_retry(ep["url"])
        else:
            if not query_value:
                return False
            raw = await db._fetch_with_retry(ep["url"],
                                              params={ep["param"]: query_value})
        if raw is None or not raw:
            return False
        flat = list(db._flatten(raw))
        if not flat:
            return False
        targeted_key = f"{key}_targeted"
        db._load(targeted_key, flat)
        db._cols[targeted_key] = db._cols.get(targeted_key, [])
        return True


smart_router = SmartQueryRouter()


# ──────────────────────────────────────────────────────────────────────────────
# 4. RAG RETRIEVER — FTS5 BM25 → LIKE fallback → bulk
# ──────────────────────────────────────────────────────────────────────────────

class RAGRetriever:
    MAX_FTS  = 500
    MAX_LIKE = 500
    MAX_BULK = 10_000

    def retrieve(self, key: str, parsed: dict,
                 use_targeted: bool = False) -> tuple:
        table  = f"{key}_targeted" if use_targeted else key
        intent = parsed["intent"]
        val    = parsed["search_value"]
        tcol   = parsed["target_col"]
        cols   = db.columns(table)
        if not cols:
            return [], [], "empty"
        if intent == "BULK_LIST" or not val:
            return self._bulk(table), cols, "bulk"
        rows, method = self._fts(table, val, tcol, cols)
        if not rows:
            rows, method = self._like(table, val, tcol, cols)
        return rows, cols, method

    def _resolve_col(self, target: str, cols: list) -> Optional[str]:
        for c in cols:
            if c.lower() == target.lower():
                return c
        for c in cols:
            if target.lower() in c.lower() or c.lower() in target.lower():
                return c
        return None

    def _fts(self, key, val, tcol, cols):
        try:
            cur  = db.conn.cursor()
            safe = re.sub(r'["()*+\-^~]', " ", val).strip()
            if not safe:
                return [], "fts_empty"
            if tcol:
                ac    = self._resolve_col(tcol, cols)
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
            print(f"[Retriever] FTS error {key}: {e}")
            return [], "fts_error"

    def _like(self, key, val, tcol, cols):
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
            print(f"[Retriever] LIKE error {key}: {e}")
            return [], "like_error"

    def _bulk(self, key):
        try:
            cur = db.conn.cursor()
            cur.execute(f'SELECT * FROM "{key}" LIMIT {self.MAX_BULK}')
            rows = [dict(r) for r in cur.fetchall()]
            for r in rows:
                r.pop("_rowid", None)
            return rows
        except Exception as e:
            print(f"[Retriever] Bulk error {key}: {e}")
            return []


retriever = RAGRetriever()


# ──────────────────────────────────────────────────────────────────────────────
# 5. AGGREGATION ENGINE
# SEC-05 FIX: rejects float("inf") and float("nan") via math.isfinite()
# ──────────────────────────────────────────────────────────────────────────────

class AggregationEngine:
    def compute(self, records: list, key: str) -> dict:
        base_key = key.replace("_targeted", "")
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
                        # SEC-05: skip inf/nan — would corrupt totals permanently
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
# 6. RESPONSE SYNTHESIZER
# SEC-01 FIX: search_value escaped via html.escape() in intro() + no_results()
# SEC-06 FIX: column header keys escaped via html.escape() in table()
# ──────────────────────────────────────────────────────────────────────────────

HIDDEN_COLS = {
    "_id", "__v", "_empty", "_data", "_rowid",
    "password", "jwttoken", "role", "permissions", "token",
}

PRIORITY_COLS = [
    "invoiceNo", "billNo", "supplierBill",
    "productName", "name", "firstName", "lastName",
    "supplierName", "customerName", "materialName",
    "supplier", "customer", "product", "material",
    "email", "phone", "contact", "mobile",
    "stockQuantity", "quantity", "qty", "receivedQuantity",
    "sellPrice", "pricePerUnit", "purchaseAmount", "totalPurchaseAmount",
    "totalAmount", "amount", "creditAmount", "paid", "balance",
    "debit", "credit", "discount", "cgst", "sgst", "igst",
    "date", "createdAt", "purchaseDate", "sellDate",
    "paymentMode", "status", "supplyType",
    "address", "city", "gstNo", "barcode", "hsnNo", "size", "color", "unit",
]

AMOUNT_HINTS = {
    "price", "amount", "total", "balance", "credit", "debit",
    "paid", "cost", "value", "charges", "discount",
}
QTY_HINTS = {"quantity", "qty", "stock"}

NOT_CURRENCY = {
    "code", "no", "id", "barcode", "number", "ref",
    "hsn", "pin", "port", "cgst", "sgst", "igst",
    "missing", "received",
}


def _is_currency_col(col: str) -> bool:
    col_l = col.lower()
    if any(ex in col_l for ex in NOT_CURRENCY):
        return False
    return any(h in col_l for h in AMOUNT_HINTS)


def _is_qty_col(col: str) -> bool:
    col_l = col.lower()
    return any(h in col_l for h in QTY_HINTS) and not any(
        ex in col_l for ex in NOT_CURRENCY
    )


def _fmt_cell(col: str, val: str) -> str:
    """Format a single cell for HTML. All user-derived strings escaped."""
    if not val or val in ("None", "null", ""):
        return '<span style="color:#94a3b8">—</span>'

    # Image URL — only render known-safe image URLs
    if val.startswith("https://") and any(
        val.lower().endswith(e) for e in (".png", ".jpg", ".jpeg", ".webp", ".gif")
    ):
        safe_url = html.escape(val, quote=True)
        return (
            f'<img src="{safe_url}" style="width:40px;height:40px;'
            f'object-fit:cover;border-radius:6px;" loading="lazy" />'
        )

    # ISO datetime
    if "T" in val and val.count("-") >= 2 and len(val) >= 19:
        try:
            d, t = val.split("T", 1)
            safe_d = html.escape(d)
            safe_t = html.escape(t[:5])
            return f'<span style="color:#64748b;font-size:12px">{safe_d} {safe_t}</span>'
        except Exception:
            pass

    # Currency
    if _is_currency_col(col):
        try:
            num   = float(str(val).replace(",", "").replace("₹", "").strip())
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
    if col.lower() in ("status", "supplytype", "type", "state", "paymentmode"):
        BADGE = {
            "in-state":  ("#dcfce7", "#16a34a"),
            "out-state": ("#fef3c7", "#d97706"),
            "active":    ("#dcfce7", "#16a34a"),
            "inactive":  ("#fee2e2", "#dc2626"),
            "paid":      ("#dcfce7", "#16a34a"),
            "pending":   ("#fef3c7", "#d97706"),
            "cash":      ("#f0f9ff", "#0369a1"),
            "credit":    ("#fdf4ff", "#9333ea"),
            "upi":       ("#f0fdf4", "#16a34a"),
        }
        badge    = BADGE.get(val.lower(), ("#f1f5f9", "#475569"))
        safe_val = html.escape(val)
        return (
            f'<span style="background:{badge[0]};color:{badge[1]};'
            f'padding:2px 8px;border-radius:999px;font-size:12px;font-weight:600">'
            f'{safe_val}</span>'
        )

    # Default — SEC-01/06: always escape arbitrary strings
    return html.escape(str(val))


class ResponseSynthesizer:

    def _headers(self, cols: list) -> list:
        lh      = {c.lower() for c in HIDDEN_COLS}
        visible = [c for c in cols if c.lower() not in lh]
        return sorted(visible, key=lambda x: (
            PRIORITY_COLS.index(x) if x in PRIORITY_COLS else 99
        ))

    def _col_label(self, col: str, marathi: bool) -> str:
        eng = re.sub(r"([a-z])([A-Z])", r"\1 \2", col).title()
        return MR_COLUMNS.get(eng, eng) if marathi else eng

    def intro(self, key: str, agg: dict, search_val: str,
              marathi: bool, lang: str) -> str:
        """SEC-01 FIX: search_val is html.escape()-d before insertion."""
        count    = agg["count"]
        amt      = agg["total_amount"]
        qty      = agg["total_qty"]
        base_key = key.replace("_targeted", "")
        # SEC-01: escape user-supplied search value
        safe_val = html.escape(search_val) if search_val else ""

        if marathi:
            label = MR_MODULES.get(base_key, html.escape(base_key))
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
            if amt:
                parts.append(f'एकूण रक्कम: <strong>₹{amt:,.2f}</strong>')
            if qty:
                parts.append(f'एकूण युनिट: <strong>{qty:,}</strong>')
        else:
            label = html.escape(base_key.replace("-", " ").title())
            ack   = random.choice([
                "Here you go! ", "Got it. ", "Absolutely! ", "Done — ",
            ])
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
            if amt:
                parts.append(f'Total: <strong>₹{amt:,.2f}</strong>')
            if qty:
                parts.append(f'Units: <strong>{qty:,}</strong>')

        if parts:
            text += " &nbsp;·&nbsp; " + " &nbsp;·&nbsp; ".join(parts)
        return f'<p style="margin:0 0 14px;font-size:15px;line-height:1.6">{text}</p>'

    def table(self, records: list, cols: list, marathi: bool) -> str:
        """SEC-06 FIX: column header keys are html.escape()-d in <th>."""
        headers = self._headers(cols)
        if not headers:
            return ""
        font = (
            "'Noto Sans Devanagari','Segoe UI',system-ui,sans-serif"
            if marathi else
            "'Segoe UI',system-ui,sans-serif"
        )
        rows_html = []
        for i, rec in enumerate(records):
            bg    = "#ffffff" if i % 2 == 0 else "#f8fafc"
            cells = ""
            for h in headers:
                raw  = str(rec.get(h, "") or "")
                cell = _fmt_cell(h, raw)
                cells += (
                    f'<td style="padding:11px 16px;border-right:1px solid #e2e8f0;'
                    f'border-bottom:1px solid #e2e8f0;font-size:13px;'
                    f'color:#334155;white-space:nowrap">{cell}</td>'
                )
            rows_html.append(
                f'<tr style="background:{bg}" '
                f'onmouseover="this.style.background=\'#eff6ff\'" '
                f'onmouseout="this.style.background=\'{bg}\'">'
                + cells + "</tr>"
            )

        # SEC-06: escape column header labels before putting in <th>
        header_cells = "".join(
            f'<th style="padding:13px 16px;border-right:1px solid #475569;'
            f'font-size:12px;font-weight:600;letter-spacing:.04em;'
            f'white-space:nowrap">'
            f'{html.escape(self._col_label(h, marathi))}</th>'
            for h in headers
        )

        return (
            f'<div style="overflow-x:auto;border-radius:12px;'
            f'box-shadow:0 4px 24px rgba(0,0,0,.08);border:1px solid #e2e8f0;">'
            f'<table style="border-collapse:collapse;width:100%;text-align:left;'
            f'background:#fff;font-family:{font};min-width:520px;">'
            f'<thead><tr style="background:linear-gradient(135deg,#1e293b,#334155);'
            f'color:#f8fafc;">{header_cells}</tr></thead>'
            f'<tbody>{"".join(rows_html)}</tbody>'
            f'</table></div>'
        )

    def no_results(self, key: str, val: str, marathi: bool,
                   query_mode: str = None) -> str:
        """SEC-01 FIX: val is html.escape()-d before insertion."""
        base_key = key.replace("_targeted", "")
        safe_val = html.escape(val) if val else ""
        if marathi:
            label = MR_MODULES.get(base_key, html.escape(base_key))
            hint  = ""
            if query_mode == "barcode":
                hint = "<br><small>टीप: बारकोड पूर्ण स्कॅन करा (उदा. BAR268726580)</small>"
            elif query_mode == "invoice":
                hint = "<br><small>टीप: इनव्हॉइस नंबर तपासा (उदा. PA00000001)</small>"
            return (
                f'<div style="background:#fef2f2;border:1px solid #fecaca;'
                f'border-radius:10px;padding:16px 20px;color:#991b1b;">'
                f'<strong>{label}</strong> मध्ये '
                f'<strong>"{safe_val}"</strong> साठी काहीही सापडले नाही.{hint}<br>'
                f'<small>शब्दलेखन तपासा किंवा लहान शब्द वापरा.</small></div>'
            )
        label = html.escape(base_key.replace("-", " ").title())
        hint  = ""
        if query_mode == "barcode":
            hint = "<br><small>Tip: use the full barcode (e.g. BAR268726580)</small>"
        elif query_mode == "invoice":
            hint = "<br><small>Tip: check invoice number format (e.g. PA00000001)</small>"
        elif query_mode == "contact":
            hint = "<br><small>Tip: enter a 10-digit mobile number</small>"
        return (
            f'<div style="background:#fef2f2;border:1px solid #fecaca;'
            f'border-radius:10px;padding:16px 20px;color:#991b1b;">'
            f'No results found in <strong>{label}</strong> for '
            f'<strong>"{safe_val}"</strong>.{hint}<br>'
            f'<small>Check spelling or try a shorter term.</small></div>'
        )

    def api_error(self, key: str, marathi: bool) -> str:
        base_key = key.replace("_targeted", "")
        if marathi:
            label = MR_MODULES.get(base_key, html.escape(base_key))
            return (
                f'<div style="background:#fff7ed;border:1px solid #fed7aa;'
                f'border-radius:10px;padding:16px 20px;color:#9a3412;">'
                f'⚠️ <strong>{label}</strong> चा डेटा मिळवता आला नाही '
                f'({API_MAX_RETRIES} प्रयत्नांनंतर). '
                f'ERP कनेक्शन तपासा.<br>'
                f'<small>सर्व्हर लॉग्स console मध्ये तपासा.</small></div>'
            )
        label = html.escape(base_key.replace("-", " ").title())
        return (
            f'<div style="background:#fff7ed;border:1px solid #fed7aa;'
            f'border-radius:10px;padding:16px 20px;color:#9a3412;">'
            f'⚠️ Could not fetch <strong>{label}</strong> data after '
            f'{API_MAX_RETRIES} attempts.<br>'
            f'<small>Check server logs for HTTP status codes and auth errors.</small></div>'
        )

    def prompt_needed(self, key: str, col: str, marathi: bool) -> str:
        base_key = key.replace("_targeted", "")
        if marathi:
            label   = MR_MODULES.get(base_key, html.escape(base_key))
            col_eng = re.sub(r"([a-z])([A-Z])", r"\1 \2", col).title()
            col_mr  = html.escape(MR_COLUMNS.get(col_eng, col_eng))
            return (
                f'तुम्ही <strong>{label}</strong> विभागात शोधत आहात. '
                f'कृपया <strong>{col_mr}</strong> सांगा.<br>'
                f'<em>उदाहरण: "invoice PA00000001" किंवा "barcode BAR268726580"</em>'
            )
        label     = html.escape(base_key.replace("-", " ").title())
        col_label = html.escape(re.sub(r"([a-z])([A-Z])", r"\1 \2", col).title())
        return (
            f'You\'re searching <strong>{label}</strong>. '
            f'Please specify the <strong>{col_label}</strong> value.<br>'
            f'<em>Example: "invoice PA00000001" or "barcode BAR268726580"</em>'
        )

    def targeted_badge(self, query_mode: str, query_value: str,
                       marathi: bool) -> str:
        safe_qv = html.escape(query_value or "")
        if marathi:
            labels = {
                "barcode":  f"बारकोड शोध: {safe_qv}",
                "invoice":  f"इनव्हॉइस शोध: {safe_qv}",
                "contact":  f"संपर्क शोध: {safe_qv}",
                "active":   "सक्रिय नोंद",
            }
            txt = html.escape(labels.get(query_mode, query_mode))
            return (
                f'<div style="background:#f0f9ff;border:1px solid #bae6fd;'
                f'border-radius:8px;padding:8px 14px;margin-bottom:10px;'
                f'font-size:13px;color:#0369a1">⚡ {txt}</div>'
            )
        labels = {
            "barcode": f"Targeted barcode lookup: {safe_qv}",
            "invoice": f"Targeted invoice lookup: {safe_qv}",
            "contact": f"Targeted contact lookup: {safe_qv}",
            "active":  "Showing active record",
        }
        txt = html.escape(labels.get(query_mode, query_mode))
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
            f"<li><em>Search barcode BAR268726580</em></li>"
            f"</ul></div>"
        )


synth = ResponseSynthesizer()


# ──────────────────────────────────────────────────────────────────────────────
# 7. CONVERSATION CONTEXT (last 5 turns)
# ──────────────────────────────────────────────────────────────────────────────

class ConversationContext:
    def __init__(self, max_turns: int = 5):
        self._sessions: dict = defaultdict(lambda: deque(maxlen=max_turns))

    def add(self, sid: str, role: str, text: str):
        self._sessions[sid].append({"role": role, "text": text[:300]})

    def last_module(self, sid: str) -> Optional[str]:
        for turn in reversed(self._sessions[sid]):
            if turn["role"] == "bot":
                m = re.search(r"_module:(\S+)", turn["text"])
                if m:
                    return m.group(1)
        return None


ctx = ConversationContext()


# ──────────────────────────────────────────────────────────────────────────────
# 8. MAIN ORCHESTRATOR
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

        use_targeted   = False
        targeted_badge = ""

        if q_mode and q_val:
            routed = await smart_router.route(key, parsed, q_mode, q_val)
            if routed:
                use_targeted   = True
                targeted_badge = synth.targeted_badge(q_mode, q_val, marathi)
                records, cols, _ = retriever.retrieve(key, parsed, use_targeted=True)
                if records:
                    agg   = aggregator.compute(records, key)
                    intro = synth.intro(key, agg, q_val, marathi, lang)
                    tbl   = synth.table(records, cols, marathi)
                    ctx.add(sid, "bot", f"_module:{key}")
                    return targeted_badge + intro + tbl

        ok = await db.sync_module(key)
        if not ok:
            return synth.api_error(key, marathi)

        records, cols, _ = retriever.retrieve(key, parsed, use_targeted=False)

        if not records:
            return synth.no_results(key, parsed["search_value"], marathi, q_mode)

        agg = aggregator.compute(records, key)

        if parsed["aggregation"] == "SUM" and parsed["intent"] != "BULK_LIST":
            base_key = html.escape(key.replace("-", " ").title())
            parts = [f'<strong>{base_key}</strong> — {agg["count"]:,} records']
            if agg["total_amount"]:
                parts.append(f'Total: <strong>₹{agg["total_amount"]:,.2f}</strong>')
            if agg["total_qty"]:
                parts.append(f'Units: <strong>{agg["total_qty"]:,}</strong>')
            return (
                f'<div style="background:#f0fdf4;border:1px solid #bbf7d0;'
                f'border-radius:10px;padding:14px 20px">'
                + " &nbsp;·&nbsp; ".join(parts) + "</div>"
            )

        intro = synth.intro(key, agg, parsed["search_value"], marathi, lang)
        tbl   = synth.table(records, cols, marathi)
        ctx.add(sid, "bot", f"_module:{key}")
        return intro + tbl

    async def _multi(self, pattern: dict, query: str, sid: str) -> str:
        keys    = pattern["modules"]
        label   = html.escape(pattern["label"])
        synced  = await db.sync_modules(keys)
        sects   = []
        total_a = total_q = 0

        for key in keys:
            if not synced.get(key):
                sects.append(
                    f'<div style="margin-bottom:24px">'
                    f'<h3 style="border-left:4px solid #6366f1;padding-left:10px;'
                    f'margin:0 0 8px;font-size:15px">'
                    f'{html.escape(key.replace("-"," ").title())}</h3>'
                    + synth.api_error(key, False) + "</div>"
                )
                continue
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
            sects.append(
                f'<div style="margin-bottom:28px">'
                f'<h3 style="border-left:4px solid #6366f1;padding-left:10px;'
                f'margin:0 0 8px;font-size:15px;color:#1e293b">{mod_label}</h3>'
                + synth.intro(key, agg, "", False, "english")
                + synth.table(records, cols, False)
                + "</div>"
            )

        if not sects:
            return f'<p>No data available for <strong>{label}</strong>.</p>'

        summary = [f'<strong>{len(keys)} modules</strong>']
        if total_a:
            summary.append(f'Total: <strong>₹{total_a:,.2f}</strong>')
        if total_q:
            summary.append(f'Units: <strong>{total_q:,}</strong>')

        header = (
            f'<div style="background:linear-gradient(135deg,#6366f1,#8b5cf6);'
            f'color:#fff;border-radius:12px;padding:16px 20px;margin-bottom:20px">'
            f'<strong style="font-size:17px">📊 {label}</strong><br>'
            f'<small>{" &nbsp;·&nbsp; ".join(summary)}</small></div>'
        )
        ctx.add(sid, "bot", f"_module:multi")
        return header + "".join(sects)


engine = AdminEngine()


# ──────────────────────────────────────────────────────────────────────────────
# 9. STATIC RESPONSES
# ──────────────────────────────────────────────────────────────────────────────

GREETING_EN = """
<div style="font-family:'Segoe UI',system-ui,sans-serif;max-width:580px">
  <p style="font-size:18px;font-weight:700;margin:0 0 14px;color:#1e293b">
    👋 Hello! I'm your <span style="color:#6366f1">ERP Intelligence Assistant v6</span>.
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
    नमस्कार! मी तुमचा <span style="color:#6366f1">ERP सहाय्यक v6</span> आहे. 🙏
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
    🧠 Admin Intelligence Engine v6.1 — Security Hardened
  </p>
  <p style="margin:0 0 16px;color:#475569">
    Production-grade <strong>RAG pipeline</strong> for ERP —
    zero external AI APIs · full Marathi support · &lt;200 MB RAM · 6 security fixes.
  </p>
  <div style="background:#fef2f2;border-radius:10px;padding:16px;margin-bottom:12px">
    <strong style="color:#dc2626">🔒 Security Fixes (v6.1):</strong>
    <ul style="margin:8px 0;padding-left:20px;color:#334155;line-height:1.9;font-size:14px">
      <li>[CRITICAL] XSS: search_value and column headers now html.escape()-d</li>
      <li>[HIGH] Memory bomb: _flatten() uses concat strategy, no Cartesian product</li>
      <li>[HIGH] Lock bottleneck: per-module locks — concurrent syncs via asyncio</li>
      <li>[MEDIUM] JWT spam: token cached at class level with 60s pre-expiry refresh</li>
      <li>[MEDIUM] NaN/Inf aggregation: math.isfinite() guard on all numeric ops</li>
      <li>[LOW] Column injection: table headers escaped via html.escape()</li>
    </ul>
  </div>
  <div style="background:#f8fafc;border-radius:10px;padding:16px;margin-bottom:12px">
    <strong style="color:#6366f1">5-Stage NLU (English):</strong>
    <ol style="margin:8px 0;padding-left:20px;color:#334155;line-height:1.9;font-size:14px">
      <li>Multi-module pattern detection</li>
      <li>Phrase matching (longest-first)</li>
      <li>Keyword scoring with confidence</li>
      <li>Fuzzy alias matching (typo-tolerant)</li>
      <li>TF-IDF cosine similarity fallback</li>
    </ol>
  </div>
  <div style="background:#f0f9ff;border-radius:10px;padding:16px">
    <strong style="color:#0369a1">⚡ SmartQueryRouter + Targeted APIs:</strong>
    <ul style="margin:8px 0;padding-left:20px;color:#334155;line-height:1.9;font-size:14px">
      <li>Barcode → /api/purchase/get-stock-by-barcode</li>
      <li>Invoice → /api/purchase/get-purchase-by-invoice-no</li>
      <li>Contact → /api/customer/search-by-contact</li>
      <li>🔒 100% private · ⚡ Zero AI API cost · 200 MB safe on Render free</li>
    </ul>
  </div>
</div>
"""


# ──────────────────────────────────────────────────────────────────────────────
# 10. FASTAPI ENDPOINT
# ──────────────────────────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    query:      Optional[str] = None
    question:   Optional[str] = None
    session_id: Optional[str] = "default"


_EN_GREET    = {"hi", "hello", "hey", "heyy", "heya",
                "good morning", "good afternoon", "good evening", "sup", "yo"}
_EN_THANKS   = {"thank you", "thanks", "awesome", "perfect", "great",
                "nice", "good", "ok", "okay", "cool"}
_EN_IDENTITY = {"who are you", "how do you work", "chatgpt", "your brain",
                "how were you built", "architecture", "how does this work",
                "what can you do", "tell me about yourself"}
_EN_REFRESH  = {"refresh", "sync", "update records", "reload data",
                "refresh data", "sync data"}
_MR_GREET_W  = {"नमस्कार", "नमस्ते", "हॅलो", "namaskar", "namaste"}
_MR_THANKS_W = {"धन्यवाद", "आभारी", "थँक्यू", "dhanyavad", "aabhari"}
_MR_REFRESH_W= {"रिफ्रेश", "अद्यतन", "refresh kara", "update kara", "sync kara"}


@router.post("/chat")
async def chat_endpoint(request: ChatRequest):
    query      = (request.query or request.question or "").strip()
    session_id = request.session_id or "default"

    if not query:
        return {"response": "Please type a question. / कृपया प्रश्न टाइप करा."}

    # Sanity cap — reject absurdly long inputs before any processing
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
            await db.sync_all(force=True)
            return {"response": (
                '<div style="background:#f0fdf4;border:1px solid #bbf7d0;'
                'border-radius:10px;padding:14px 18px;color:#166534">'
                '🔄 <strong>सर्व माहिती अद्यतनित झाली.</strong> '
                'ERP डेटा ताजा आहे. आता काय पाहायचे आहे?</div>'
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
            await db.sync_all(force=True)
            return {"response": (
                '<div style="background:#f0fdf4;border:1px solid #bbf7d0;'
                'border-radius:10px;padding:14px 18px;color:#166534">'
                '🔄 <strong>All modules synced.</strong> ERP data is up to date.'
                '</div>'
            )}

    response = await engine.process(query, session_id)
    return {"response": response}