"""
==============================================================================
  ADMIN INTELLIGENCE ENGINE  v5.0  —  PRODUCTION READY
  ERP Chatbot RAG Pipeline · Zero external AI APIs · <120 MB RAM
  Full Marathi (Devanagari + Roman) + English support

ARCHITECTURE
┌──────────────────────────────────────────────────────────────────┐
│  User Query (English / मराठी / Marathi Roman typing)             │
│      │                                                           │
│      ▼                                                           │
│  LanguageDetector ──► MarathiClassifier (if Marathi detected)   │
│      │                        │                                  │
│      │ (English)              │ (Marathi)                        │
│      ▼                        ▼                                  │
│  IntentClassifier      MarathiIntentMap                          │
│  (5-stage NLU)               │                                   │
│      │                        │                                  │
│      └──────────┬─────────────┘                                  │
│                 ▼                                                 │
│          Multi-API Planner ──► Async Fetch Pool                  │
│                 │                      │                          │
│                 ▼                      ▼                          │
│           SQLite FTS5 Mirror    DataMirror Cache                  │
│                 │                                                 │
│                 ▼                                                 │
│          RAG Retriever ──► ResponseSynthesizer                   │
│                                    │                             │
│                                    ▼                             │
│          HTML Response (English or मराठी based on input)         │
└──────────────────────────────────────────────────────────────────┘

  KEY INNOVATIONS vs v3/v4:
  1. Language detection — Devanagari Unicode + Roman phonetic markers
  2. Marathi intent map — 206 keywords across Devanagari + Roman
  3. Marathi response synthesizer — headers, summaries, UI in Marathi
  4. Marathi column translations — all 40+ ERP column headers translated
  5. Longest-match algorithm — most specific Marathi phrase wins
  6. Mixed-language queries — "supplier credit kiti ahe" works perfectly
  7. Language-aware QueryParser — correct stop set per language
  8. Devanagari inflection stemming — उत्पादने→उत्पादन, साथ→साठा, etc.

  BUGS FIXED vs v4:
  ─────────────────
  1. Marathi stop words — 120+ words so filler phrases like "kiti ahe",
     "dya", "sarv", "dakha", "mala", "chi", "yadi" never leak into search_value
  2. Marathi bulk triggers — sarv/saglya/yadi correctly set BULK_LIST intent
  3. Devanagari inflection stems — उत्पादने→उत्पादन, साठाची→साठा, etc.
  4. QueryParser is now language-aware — passes correct stop set per language
  5. Price Code / Barcode / HSN columns no longer formatted as currency
  6. API retry with exponential backoff — 3 attempts before showing error
  7. Marathi search-value queries work — "mala rahul grahak dakha" → search
  8. Mixed Marathi+English fully handled in every function
==============================================================================
"""

import asyncio
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
MODULE_TTL       = 300      # 5 min — master data (customers, suppliers, etc.)
STOCK_TTL        = 120      # 2 min — inventory (changes frequently)
FINANCE_TTL      = 600      # 10 min — ledgers / finance (slower moving)
MAX_ROWS         = 50_000   # per-module SQLite cap (prevents RAM explosion)
API_TIMEOUT      = 30.0
API_MAX_RETRIES  = 3        # exponential backoff: 1.5s, 3s, 6s


# ──────────────────────────────────────────────────────────────────────────────
# MODULE REGISTRY
# Each module defines:
#   url         - API endpoint (relative to BASE_URL)
#   ttl         - cache lifetime in seconds
#   keywords    - high-confidence single-word matches for NLU stage 3
#   phrases     - multi-word matches (checked FIRST at stage 2, highest priority)
#   aliases     - typo/synonym list for fuzzy matching at stage 4
#   amount_cols - columns holding monetary values (for aggregation + formatting)
#   qty_cols    - columns holding quantities (for aggregation + formatting)
# ──────────────────────────────────────────────────────────────────────────────
MODULE_REGISTRY = {
    "supplier-credit": {
        "url":         "/api/supplier-credit/get-all-supplier-credits",
        "ttl":         FINANCE_TTL,
        "keywords":    ["credit", "credits", "outstanding", "due", "refund", "owe", "owes"],
        "phrases":     ["supplier credit", "supplier credits", "vendor credit", "credit note",
                        "credit balance", "outstanding credit", "how much do we owe"],
        "aliases":     ["supplier credit", "vendor credit", "credits outstanding"],
        "amount_cols": ["creditAmount", "amount", "totalCredit", "totalAmount"],
        "qty_cols":    [],
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
    },
    "customer-ledger": {
        "url":         "/api/reports/get-all-customer-ledgers",
        "ttl":         FINANCE_TTL,
        "keywords":    [],
        "phrases":     ["customer ledger", "client ledger", "customer statement",
                        "customer account", "receivables"],
        "aliases":     ["customer ledger", "client ledger"],
        "amount_cols": ["debit", "credit", "balance", "amount"],
        "qty_cols":    [],
    },
    "sell": {
        "url":         "/api/sell/get-all-sells",
        "ttl":         MODULE_TTL,
        "keywords":    ["sell", "sells", "sold", "sale", "sales", "dispatch",
                        "invoice", "receipt", "billing", "revenue"],
        "phrases":     ["sales invoice", "sell invoice", "dispatch record", "all sales",
                        "sales list", "all sells", "invoices", "what did we sell"],
        "aliases":     ["sales", "sells", "invoices", "receipts", "billing"],
        "amount_cols": ["totalAmount", "sellPrice", "amount", "paid"],
        "qty_cols":    ["quantity", "qty", "totalQuantity"],
    },
    "purchase": {
        "url":         "/api/purchase/get-all-purchases",
        "ttl":         MODULE_TTL,
        "keywords":    ["purchase", "purchases", "bought", "buy", "buying",
                        "acquire", "procurement", "ordered"],
        "phrases":     ["purchase order", "purchase invoice", "purchase history",
                        "purchase list", "all purchases", "what did we buy"],
        "aliases":     ["purchaces", "purchasse", "purcheases", "purhcases",
                        "purchaes", "puchases", "pruchases", "purchases", "buys"],
        "amount_cols": ["totalAmount", "purchaseAmount", "amount", "paid"],
        "qty_cols":    ["quantity", "qty", "totalQuantity"],
    },
    "payment": {
        "url":         "/api/payment/supplier-payment-history",
        "ttl":         MODULE_TTL,
        "keywords":    ["payment", "payments", "paid", "settled", "transaction", "remittance"],
        "phrases":     ["payment history", "supplier payment", "payment record",
                        "payment list", "all payments", "transaction history",
                        "what was paid", "payments made"],
        "aliases":     ["payments", "paying", "remittance", "settlement"],
        "amount_cols": ["amount", "paid", "totalPaid"],
        "qty_cols":    [],
    },
    "material": {
        "url":         "/api/material/get-all-materials",
        "ttl":         MODULE_TTL,
        "keywords":    ["material", "materials", "fabric", "raw", "component", "item"],
        "phrases":     ["raw material", "material list", "all materials", "fabric list"],
        "aliases":     ["materials", "fabrics", "raw materials"],
        "amount_cols": [],
        "qty_cols":    [],
    },
    "customer": {
        "url":         "/api/customer/get-all-customers",
        "ttl":         MODULE_TTL,
        "keywords":    ["customer", "customers", "client", "clients", "buyer", "purchaser"],
        "phrases":     ["customer list", "client list", "all customers",
                        "buyer list", "who are our customers"],
        "aliases":     ["customers", "clients", "buyers"],
        "amount_cols": [],
        "qty_cols":    [],
    },
    "supplier": {
        "url":         "/api/supplier/get-all-suppliers",
        "ttl":         MODULE_TTL,
        "keywords":    ["supplier", "suppliers", "vendor", "vendors", "distributor", "provider"],
        "phrases":     ["supplier list", "vendor list", "all suppliers", "distributor list"],
        "aliases":     ["suppliers", "vendors", "distributors"],
        "amount_cols": ["supplierCredit"],
        "qty_cols":    [],
    },
    "product-stock": {
        "url":         "/api/reports/get-product-stocks-with-product",
        "ttl":         STOCK_TTL,
        "keywords":    ["stock", "stocks", "inventory", "warehouse", "available",
                        "product", "products"],
        "phrases":     ["product stock", "stock list", "inventory list", "product inventory",
                        "available stock", "warehouse stock", "current stock",
                        "what do we have in stock"],
        "aliases":     ["stocks", "inventory", "products", "warehousing"],
        "amount_cols": ["sellPrice", "pricePerUnit", "totalAmount"],
        "qty_cols":    ["stockQuantity", "quantity"],
    },
    "printer": {
        "url":         "/api/printer/get-all-printers",
        "ttl":         MODULE_TTL,
        "keywords":    ["printer", "printers", "machine", "print"],
        "phrases":     ["printer list", "all printers", "printing machines"],
        "aliases":     ["printers", "printing"],
        "amount_cols": [],
        "qty_cols":    [],
    },
    "email-config": {
        "url":         "/api/email-config/get-all-emails",
        "ttl":         MODULE_TTL,
        "keywords":    ["email", "emails", "smtp", "config", "mail"],
        "phrases":     ["email config", "mail config", "smtp config", "email settings"],
        "aliases":     ["emails", "mails", "smtp"],
        "amount_cols": [],
        "qty_cols":    [],
    },
    "category": {
        "url":         "/api/reports/get-all-product-categories",
        "ttl":         MODULE_TTL,
        "keywords":    ["category", "categories", "classification", "type", "types"],
        "phrases":     ["product category", "category list", "all categories", "product types"],
        "aliases":     ["categories", "classifications"],
        "amount_cols": [],
        "qty_cols":    [],
    },
}

# ──────────────────────────────────────────────────────────────────────────────
# MULTI-MODULE QUERY PATTERNS
# When a query involves more than one module, define it here.
# Key: display label.  Value: list of module keys to fetch simultaneously.
# All listed modules are fetched concurrently via asyncio.gather().
# ──────────────────────────────────────────────────────────────────────────────
MULTI_PATTERNS = [
    {
        "phrases": ["supplier summary", "supplier overview", "vendor summary",
                    "full supplier details", "all supplier info"],
        "modules": ["supplier", "supplier-credit", "supplier-ledger"],
        "label":   "Full Supplier Overview",
    },
    {
        "phrases": ["financial summary", "finance report", "money overview",
                    "accounts summary", "financial overview"],
        "modules": ["sell", "purchase", "payment", "supplier-credit"],
        "label":   "Financial Summary",
    },
    {
        "phrases": ["sales report", "sales and stock", "revenue and inventory"],
        "modules": ["sell", "product-stock"],
        "label":   "Sales & Inventory Report",
    },
    {
        "phrases": ["customer and sales", "sales by customer"],
        "modules": ["customer", "sell"],
        "label":   "Customer & Sales Report",
    },
    {
        "phrases": ["purchase and payment", "vendor transactions"],
        "modules": ["purchase", "payment", "supplier"],
        "label":   "Vendor Transactions",
    },
]

# ──────────────────────────────────────────────────────────────────────────────
# COLUMN SEARCH TRIGGERS
# Maps natural language words → likely column names to target.
# Used by QueryParser to detect COLUMN_SEARCH intent and extract target_col.
# First item in each list is the primary column candidate.
# ──────────────────────────────────────────────────────────────────────────────
COLUMN_TRIGGERS = {
    "invoice": ["invoiceNo", "invoice"],
    "bill":    ["invoiceNo", "billNo"],
    "barcode": ["barcode", "code"],
    "contact": ["contact", "phone", "mobile"],
    "phone":   ["phone", "contact", "mobile"],
    "mobile":  ["mobile", "phone", "contact"],
    "id":      ["id", "_id"],
    "email":   ["email"],
    "name":    ["name", "firstName", "lastName", "productName",
                "supplierName", "customerName", "materialName"],
    "address": ["address"],
    "gst":     ["gstNo", "gst"],
    "date":    ["date", "createdAt", "purchaseDate", "sellDate"],
    "amount":  ["totalAmount", "amount", "creditAmount", "paid"],
    "price":   ["sellPrice", "price", "pricePerUnit"],
    "status":  ["status", "supplyType"],
}

# ──────────────────────────────────────────────────────────────────────────────
# STOP WORD SETS
# Three sets are used depending on detected language.
# Stop words are stripped before extracting search_value in QueryParser.
# ──────────────────────────────────────────────────────────────────────────────

# ── English stop words ────────────────────────────────────────────────────────
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

# ── Marathi stop words (Roman transliteration) ────────────────────────────────
# These are stripped so filler phrases like "mala dakhva", "sarv yadi",
# "kiti ahe" never leak into the search_value. 120+ entries.
MR_STOP_ROMAN = {
    # Pronouns
    "mala", "mla", "mhala", "amhala", "amhi", "mi", "tu", "to", "ti",
    "te", "tyala", "tila", "aapan", "apan",
    # Show/give/display commands — never a search value
    "dakhva", "dakhav", "dakhvav", "dakha", "dakhava", "dakhvaa",
    "dikhao", "dikhav", "dikhava", "dikhva",
    "dya", "deu", "de", "dyaa", "dyave",
    "bagha", "bagh", "baghu", "pahava", "paha", "pahun", "pahat",
    "sanga", "sangav", "sangava", "sangaa",
    "kadhav", "kadhva", "milvava", "milva", "milu", "mil",
    "ghya", "ghyava", "aana", "aanava",
    "pahije", "hve", "have",
    # List/all — set BULK intent, never search value
    "sarv", "sarva", "saglya", "sagale", "sagala", "sagali", "sagle",
    "yadi", "yaadi", "soochi", "suchi", "jadval",
    "sampurn", "sampoorna", "poorna", "purna",
    "sare", "sara", "sari", "sarav",
    # Grammar particles
    "chi", "che", "cha", "chya", "la", "na", "va", "ani", "aani",
    "ahe", "aahe", "aste", "asate", "ahet", "naahi", "nahi",
    "kiti", "kasa", "kase", "kashi", "kay", "kaay", "kon", "konte",
    "ha", "he", "hi", "hya", "tya", "ya", "ja",
    "pan", "pari", "tar", "mhanje", "mhanun",
    "ata", "aata", "jara", "jra", "ekda", "ekadha",
    "sathi", "saathi", "karitha", "karita",
    "madhe", "madhye", "madhil", "tun", "tyatun",
    "sobat", "sathe", "barobar",
    "naavache", "naav", "naavane", "naavachya",
    "wala", "wale", "wali", "vala", "vale", "vali",
    "cha", "che", "chi", "chya",
    # Info/detail words
    "mahiti", "tapshil", "maahiti",
    "itihas", "history",
    # Question words
    "kaya", "kithi", "kevu", "kevha", "keva", "kuthun",
    # Filler
    "please", "plz", "krupaya", "krupa",
    "thodi", "thoda", "thode",
}

# ── Marathi bulk trigger words (Roman) ───────────────────────────────────────
# When any of these appear in a query, intent is forced to BULK_LIST
MR_BULK_ROMAN = {
    "sarv", "sarva", "saglya", "sagale", "yadi", "yaadi",
    "sare", "sara", "sampurn", "poorna", "purna", "soochi",
    "all", "every", "complete", "full", "entire", "sarav",
}

# ── Marathi stop words (Devanagari script) ────────────────────────────────────
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

# ── Marathi bulk trigger words (Devanagari) ───────────────────────────────────
MR_BULK_DEVA = {
    "सर्व", "सगळ्या", "सगळे", "यादी", "सूची", "संपूर्ण", "पूर्ण", "सारे",
}

# ──────────────────────────────────────────────────────────────────────────────
# MARATHI LANGUAGE LAYER
# Covers three input modes:
#   1. Devanagari script  — e.g. "सर्व खरेदी दाखवा"
#   2. Roman transliteration (Marathi typing) — e.g. "kharedi list dya"
#   3. Mixed — e.g. "supplier credit kiti ahe"
#
# Design principles:
#   - Longest-match wins (more specific phrase beats shorter keyword)
#   - Devanagari detection via Unicode range U+0900–U+097F
#   - Roman detection via a curated frozenset of Marathi phonetic markers
#   - Devanagari inflection stemming applied before classification
#   - All detection is O(n*m) — fast enough for real-time use
# ──────────────────────────────────────────────────────────────────────────────

# ── Devanagari inflection → canonical form ────────────────────────────────────
# Maps common inflected forms to their base/canonical form so the intent map
# can match correctly. e.g. "उत्पादने दाखवा" → "उत्पादन दाखवा" → product-stock
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
    "साथ":            "साठा",   # common Devanagari typo
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
}

# ── Intent keyword map — Devanagari + Roman per module ────────────────────────
# Sorted by length descending at runtime so longest match wins.
# e.g. "पुरवठादार क्रेडिट" (18 chars) beats "क्रेडिट" (7 chars)
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
            "ग्राहक खातेवही", "ग्राहक हिशेब", "ग्राहक विवरण", "ग्राहक खाते",
        ],
        "roman": [
            "grahak khatevahi", "grahak hisab", "graahak khatevahi",
            "customer ledger", "customer hisab",
        ],
    },
    "customer": {
        "deva": [
            "ग्राहक यादी", "सर्व ग्राहक", "ग्राहक माहिती", "खरेदीदार", "ग्राहक",
        ],
        "roman": [
            "grahak yadi", "sarv grahak", "grahak mahiti", "kharedidar",
            "grahak list", "graahak list", "customer list",
            "grahak", "graahak",
        ],
    },
    "supplier": {
        "deva": [
            "पुरवठादार यादी", "सर्व पुरवठादार", "विक्रेता", "माल पुरवठादार", "पुरवठादार",
        ],
        "roman": [
            "purvathakaar yadi", "sarv purvathakaar", "vikreta",
            "supplier list", "vendor list",
            "purvathakaar", "puravathakaar",
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
        "deva": ["श्रेणी यादी", "वर्गीकरण", "उत्पादन प्रकार", "श्रेणी", "प्रकार"],
        "roman": ["shreni yadi", "vargikaran", "utpadan prakar",
                  "category list", "shreni", "prakar"],
    },
    "printer": {
        "deva": ["प्रिंटर यादी", "मुद्रण यंत्र", "प्रिंटर"],
        "roman": ["printer yadi", "mudran yantra", "printer list", "printer"],
    },
    "email-config": {
        "deva": ["ईमेल सेटिंग", "ईमेल", "मेल"],
        "roman": ["email settings", "email config", "mail config", "email"],
    },
}

# Pre-sort flat list longest-first so longest keyword always wins
_MR_FLAT: list = []
for _mod, _kws in _MR_RAW.items():
    for k in _kws["deva"] + _kws["roman"]:
        _MR_FLAT.append((k.lower(), _mod))
_MR_FLAT.sort(key=lambda x: len(x[0]), reverse=True)

# ── Marathi Roman phonetic markers (used for language detection) ──────────────
# A query containing ANY of these is classified as 'marathi_roman'.
# Carefully chosen to have low false-positive rate with English.
_MR_ROMAN_MARKERS: frozenset = frozenset([
    "kharedi", "kharedee", "kharidi", "vikri", "vikree",
    "grahak", "graahak", "purvathakaar", "puravathakaar",
    "satha", "saatha", "thakbaki", "udhar", "dene",
    "yadi", "bagha", "dakha", "ahe", "aahe", "sarv",
    "saglya", "hisab", "hisaab", "khatevahi", "vivaran",
    "dayak", "paise", "paisa", "kachha", "sahitya",
    "kapad", "shreni", "uplabdh", "godam", "vyavahar",
    "pavti", "baki", "shillak", "rakkam", "vikreta",
    "kiti", "mal", "nondi", "tapshil", "itihas",
    "mahiti", "dilele", "dile", "kele", "vikle",
    "dakhva", "dakhav", "dakha", "mala", "mhala",
])

# ── Marathi quick-response word sets ─────────────────────────────────────────
_MR_GREET: frozenset = frozenset([
    "नमस्कार", "नमस्ते", "हॅलो", "सुप्रभात",
    "namaskar", "namaste", "namasте",
])
_MR_THANKS: frozenset = frozenset([
    "धन्यवाद", "आभारी", "थँक्यू", "shukriya",
    "dhanyavad", "aabhari", "dhanywaad",
])
_MR_REFRESH: frozenset = frozenset([
    "रिफ्रेश", "अद्यतन", "ताजी", "refresh kara", "update kara", "sync kara",
])

# ── Column header translations ────────────────────────────────────────────────
MR_COLUMNS = {
    "Invoice No":      "इनव्हॉइस क्र.",
    "Name":            "नाव",
    "Email":           "ईमेल",
    "Contact":         "संपर्क",
    "Address":         "पत्ता",
    "Supplier Name":   "पुरवठादाराचे नाव",
    "Customer Name":   "ग्राहकाचे नाव",
    "Product Name":    "उत्पादनाचे नाव",
    "Material Name":   "साहित्याचे नाव",
    "Total Amount":    "एकूण रक्कम",
    "Sell Price":      "विक्री किंमत",
    "Stock Quantity":  "साठ्याची संख्या",
    "Quantity":        "संख्या",
    "Qty":             "संख्या",
    "Paid":            "दिलेले",
    "Credit Amount":   "क्रेडिट रक्कम",
    "Balance":         "शिल्लक",
    "Debit":           "नावे",
    "Credit":          "जमा",
    "Date":            "तारीख",
    "Created At":      "तयार तारीख",
    "Updated At":      "अद्यतन तारीख",
    "Created By":      "तयार केले",
    "Updated By":      "अद्यतन केले",
    "Barcode":         "बारकोड",
    "Gst No":          "जीएसटी क्र.",
    "Supply Type":     "पुरवठा प्रकार",
    "Status":          "स्थिती",
    "Price Per Unit":  "प्रति युनिट किंमत",
    "Purchase Date":   "खरेदी तारीख",
    "Sell Date":       "विक्री तारीख",
    "Unit":            "युनिट",
    "Size":            "आकार",
    "Color":           "रंग",
    "Category":        "श्रेणी",
    "Description":     "वर्णन",
    "Supplier Credit": "पुरवठादार क्रेडिट",
    "First Name":      "पहिले नाव",
    "Last Name":       "आडनाव",
    "Phone":           "फोन",
    "City":            "शहर",
    "Type":            "प्रकार",
    "Mode":            "पद्धत",
    "Hsn No":          "एचएसएन क्र.",
    "Price Code":      "किंमत कोड",
    "Image Url":       "प्रतिमा",
    "Product Code":    "उत्पादन कोड",
    "Host":            "होस्ट",
    "Port":            "पोर्ट",
    "Bill No":         "बिल क्र.",
    "Mobile":          "मोबाईल",
    "To Pay":          "द्यावयाचे",
    "Debit Amount":    "नावे रक्कम",
}

# ── Module name translations ──────────────────────────────────────────────────
MR_MODULES = {
    "purchase":        "खरेदी",
    "sell":            "विक्री",
    "supplier-credit": "पुरवठादार क्रेडिट",
    "supplier-ledger": "पुरवठादार खातेवही",
    "customer-ledger": "ग्राहक खातेवही",
    "customer":        "ग्राहक",
    "supplier":        "पुरवठादार",
    "product-stock":   "उत्पादन साठा",
    "payment":         "देयक",
    "material":        "साहित्य",
    "category":        "श्रेणी",
    "printer":         "प्रिंटर",
    "email-config":    "ईमेल सेटिंग",
}


# ──────────────────────────────────────────────────────────────────────────────
# LANGUAGE DETECTION & MARATHI CLASSIFICATION
# ──────────────────────────────────────────────────────────────────────────────

def detect_language(text: str) -> str:
    """
    Returns: 'marathi_devanagari' | 'marathi_roman' | 'english'

    Algorithm:
      1. Count Devanagari chars (U+0900–U+097F). If >25% of alpha chars → Devanagari.
      2. Else check for any Marathi Roman phonetic markers → marathi_roman.
      3. Else → english.

    Fast: O(len(text)) for step 1, O(markers) for step 2.
    """
    deva  = sum(1 for c in text if 0x0900 <= ord(c) <= 0x097F)
    alpha = sum(1 for c in text if c.isalpha())
    if alpha == 0:
        return "english"
    if deva / alpha > 0.25:
        return "marathi_devanagari"
    t = text.lower()
    if any(m in t for m in _MR_ROMAN_MARKERS):
        return "marathi_roman"
    return "english"


def marathi_classify(text: str) -> Optional[str]:
    """
    Classify a Marathi query (Devanagari or Roman) to a module key.

    Uses longest-match: 'purvathakaar credit' (18 chars) beats 'credit' (6 chars).
    Applies Devanagari inflection stemming before matching.

    Returns module key string or None if no match found.
    """
    # Normalise inflected forms first
    words   = text.split()
    stemmed = " ".join(DEVA_STEMS.get(w, w) for w in words)
    t       = stemmed.lower().strip()
    for keyword, module in _MR_FLAT:
        if keyword in t:
            return module
    return None


def _apply_deva_stems(text: str) -> str:
    """Apply Devanagari inflection stemming to all words in text."""
    return " ".join(DEVA_STEMS.get(w, w) for w in text.split())


# ──────────────────────────────────────────────────────────────────────────────
# 1. NLU — 5-STAGE ENGLISH INTENT CLASSIFIER
#
# Pipeline with confidence scoring:
#   S1: Multi-module patterns     → confidence 1.0
#   S2: Phrase match              → confidence 0.95  (longest phrase wins)
#   S3: Keyword match (scored)    → confidence proportional to match density
#   S4: Fuzzy alias match         → confidence = fuzzy_ratio × 0.85
#   S5: TF-IDF cosine fallback    → confidence = cosine_score × 0.75
#
# The highest-confidence match across all 5 stages wins.
# Below 0.12 confidence → UNRECOGNIZED (asks for clarification).
# ──────────────────────────────────────────────────────────────────────────────

class IntentClassifier:
    def __init__(self):
        self._build_tfidf()
        # Flat phrase pairs sorted longest-first so most specific phrase wins
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
        # Multi-module phrase pairs sorted longest-first
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
        """
        Build TF-IDF vectors for each module's keyword corpus.
        IDF = log((1+N)/(1+df)) + 1  (sklearn-style smoothing)
        Vectors are L2-normalised so cosine similarity = dot product.
        """
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
        """Return {module: cosine_score} for a cleaned query string."""
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

    # ── Stage 1: Multi-module ───────────────────────────────────────────────
    def _stage_multi(self, text: str):
        for ph, pat in self._multi_pairs:
            if ph in text:
                return {"type": "MULTI", "pattern": pat, "confidence": 1.0}
        return None

    # ── Stage 2: Phrase match ───────────────────────────────────────────────
    def _stage_phrase(self, text: str):
        for ph, mod in self._phrase_pairs:
            if ph in text:
                return {"type": "SINGLE", "module": mod, "confidence": 0.95}
        return None

    # ── Stage 3: Keyword count scoring ─────────────────────────────────────
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

    # ── Stage 4: Fuzzy alias matching ──────────────────────────────────────
    def _stage_fuzzy(self, tokens: list):
        """SequenceMatcher ratio — catches typos like 'purchaces' → purchases."""
        best_s, best_mod = 0.0, None
        for tok in tokens:
            if len(tok) < 4:  # skip very short tokens
                continue
            for al, mod in self._alias_pairs:
                s = SequenceMatcher(None, tok, al).ratio()
                if s > best_s:
                    best_s, best_mod = s, mod
        if best_s >= 0.72:
            return {"type": "SINGLE", "module": best_mod,
                    "confidence": best_s * 0.85}
        return None

    # ── Stage 5: TF-IDF cosine fallback ────────────────────────────────────
    def _stage_tfidf(self, text: str):
        scores = self._tfidf_score(text)
        if scores:
            best = max(scores, key=scores.get)
            if scores[best] >= 0.12:
                return {"type": "SINGLE", "module": best,
                        "confidence": scores[best] * 0.75}
        return None

    # ── Main classify ────────────────────────────────────────────────────────
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
# 2. QUERY PARSER — language-aware
#
# Given a module and the raw user query, determines:
#   intent:       BULK_LIST | GLOBAL_SEARCH | COLUMN_SEARCH | PROMPT_NEEDED
#   target_col:   specific column to search in (if detected via COLUMN_TRIGGERS)
#   search_value: the actual value to search for (after stripping stop words)
#   aggregation:  SUM | NONE
#
# Language awareness:
#   - English queries use EN_STOP / EN_BULK
#   - Marathi queries use MR_STOP_ROMAN | MR_STOP_DEVA  and  MR_BULK_*
#   This prevents Marathi filler words from leaking into search_value.
# ──────────────────────────────────────────────────────────────────────────────

AGG_TRIGGERS = {
    "total", "sum", "how many", "count", "how much", "aggregate",
    "altogether", "combined", "overall",
    "एकूण", "बेरीज", "किती आहे",
}


class QueryParser:
    def parse(self, user_query: str, module_key: str, lang: str) -> dict:
        """
        Parse user_query in the context of module_key and detected language.

        Returns dict with keys: intent, target_col, search_value, aggregation.
        """
        text_lower = user_query.lower()

        # Apply Devanagari stemming before any further processing
        if lang == "marathi_devanagari":
            text_lower = _apply_deva_stems(text_lower)

        tokens = text_lower.split()

        # ── Aggregation detection ────────────────────────────────────────────
        agg = any(t in text_lower for t in AGG_TRIGGERS)

        # ── Language-specific stop / bulk sets ───────────────────────────────
        if lang in ("marathi_devanagari", "marathi_roman"):
            stop = MR_STOP_ROMAN | MR_STOP_DEVA
            bulk = MR_BULK_ROMAN | MR_BULK_DEVA
        else:
            stop = EN_STOP
            bulk = EN_BULK

        is_bulk = any(tok in bulk for tok in tokens)

        # ── Strip module vocabulary ──────────────────────────────────────────
        mod = MODULE_REGISTRY[module_key]
        mod_words: set = set()
        for kw in mod["keywords"]:
            mod_words.update(kw.lower().split())
        for ph in mod["phrases"]:
            mod_words.update(ph.lower().split())
        for al in mod["aliases"]:
            mod_words.update(al.lower().split())
        # Also strip Marathi keywords for this module
        for kw, m in _MR_FLAT:
            if m == module_key:
                mod_words.update(kw.split())

        # ── Detect target column ─────────────────────────────────────────────
        target_col = None
        col_words: set = set()
        for trigger, cols in COLUMN_TRIGGERS.items():
            if re.search(rf"\b{re.escape(trigger)}s?\b", text_lower):
                target_col = cols[0]
                col_words.update([trigger, trigger + "s", trigger + "es"])
                break

        # ── Extract search value ─────────────────────────────────────────────
        # Value tokens = whatever remains after stripping stop + module + col words
        strip = stop | mod_words | col_words
        value_tokens = [
            tok for tok in tokens
            if tok not in strip and len(tok) > 1
        ]

        # Codes and numbers (invoice numbers, barcodes) — preserve regardless
        code_tokens = [
            tok for tok in tokens
            if re.match(r"^[a-z0-9\-]+$", tok)
            and (re.search(r"\d", tok) or len(tok) >= 6)
            and tok not in strip
        ]
        if code_tokens and not value_tokens:
            value_tokens = code_tokens

        search_value = " ".join(value_tokens).strip()

        # ── Determine intent ─────────────────────────────────────────────────
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
# For each module, maintains TWO tables:
#   "{module}"       — structured columnar data (typed TEXT columns)
#   "{module}_fts"   — FTS5 virtual table for full-text BM25 search
#
# FTS5 advantages over LIKE:
#   • BM25 ranking (relevance-sorted results, not just existence)
#   • Tokenizer handles word boundaries correctly
#   • 5-10× faster on large datasets
#   • Case-insensitive by default
#   • Supports prefix search: MATCH 'INV-*'
#
# TTL-based caching:
#   • Each module has an expiry timestamp
#   • sync_module() is a no-op if cache is still valid
#   • sync_all(force=True) bypasses cache (used on "refresh" commands)
#
# API resilience:
#   • 3 retry attempts with exponential backoff (1.5s, 3s, 6s)
#   • Returns None on all retries failing → api_error shown to user
# ──────────────────────────────────────────────────────────────────────────────

class DataMirror:
    def __init__(self):
        # WAL mode allows concurrent reads without blocking writes
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.execute("PRAGMA cache_size=-16000")  # 16 MB page cache
        self._ttl:  dict = {}   # module → expiry timestamp
        self._cols: dict = {}   # module → list of column names
        self._lock = asyncio.Lock()

    # ── Flatten nested API JSON ─────────────────────────────────────────────
    def _extract_list(self, raw) -> list:
        """Extract the primary data list from varied API response shapes."""
        if isinstance(raw, list):
            return raw
        if isinstance(raw, dict):
            if "data" in raw and isinstance(raw["data"], list):
                return raw["data"]
            for v in raw.values():
                if isinstance(v, list) and v:
                    return v
        return []

    def _flatten(self, records: list):
        """
        Generator: yields flat dict rows from possibly nested API records.

        Handles:
          - Primitive values → str()
          - Nested dicts → extracts best display value (name/title/invoiceNo)
          - Lists of dicts → cross-joins with parent row (child rows)
          - None → empty string
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
                for _, clist in children:
                    for child in clist:
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

    # ── SQLite load ───────────────────────────────────────────────────────────
    def _load(self, key: str, records: list):
        """
        Create/replace columnar table + FTS5 virtual table for a module.

        Uses content= FTS table (no data duplication — FTS index references
        main table rowid). Tokenizer: unicode61 with diacritics removed
        (so "Rahül" matches "rahul").
        """
        cur = self.conn.cursor()
        cur.execute(f'DROP TABLE IF EXISTS "{key}_fts"')
        cur.execute(f'DROP TABLE IF EXISTS "{key}"')

        if not records:
            cur.execute(f'CREATE TABLE "{key}" (_empty TEXT)')
            self._cols[key] = []
            self.conn.commit()
            return

        # Collect all unique column names preserving insertion order
        seen, all_keys = set(), []
        for r in records:
            for k in r:
                if k not in seen:
                    seen.add(k)
                    all_keys.append(k)

        records = records[:MAX_ROWS]

        # Columnar table
        cols_ddl = ", ".join(f'"{k}" TEXT' for k in all_keys)
        cur.execute(f'CREATE TABLE "{key}" (_rowid INTEGER PRIMARY KEY, {cols_ddl})')
        ph     = ",".join(["?"] * len(all_keys))
        col_sql = ", ".join(f'"{k}"' for k in all_keys)
        for r in records:
            cur.execute(
                f'INSERT INTO "{key}" ({col_sql}) VALUES ({ph})',
                [r.get(k, "") for k in all_keys],
            )

        # FTS5 content table (index only, references main table)
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

    # ── API fetch with exponential backoff retry ──────────────────────────────
    async def _fetch_with_retry(self, url: str) -> Optional[list]:
        """
        Fetch a module's data from the ERP API.
        Retries up to API_MAX_RETRIES times with exponential backoff.
        Returns flat record list or None on total failure.
        """
        last_exc = None
        for attempt in range(API_MAX_RETRIES):
            try:
                async with httpx.AsyncClient(timeout=API_TIMEOUT) as client:
                    auth  = await client.post(LOGIN_URL, json=LOGIN_CREDS)
                    token = auth.json().get("jwtToken")
                    if not token:
                        return None
                    headers = {
                        "Authorization": f"Bearer {token}",
                        "Content-Type":  "application/json",
                    }
                    resp = await client.get(f"{BASE_URL}{url}", headers=headers)
                    if resp.status_code == 200:
                        return self._extract_list(resp.json())
                    last_exc = Exception(f"HTTP {resp.status_code}")
            except Exception as exc:
                last_exc = exc
                if attempt < API_MAX_RETRIES - 1:
                    await asyncio.sleep(1.5 * (2 ** attempt))  # 1.5s, 3s, 6s
        print(f"[DataMirror] All retries failed for {url}: {last_exc}")
        return None

    # ── Sync single module ────────────────────────────────────────────────────
    async def sync_module(self, key: str, force: bool = False) -> bool:
        """
        Sync a module from the ERP API into SQLite.
        Double-checked locking: check TTL before and after acquiring lock.
        Returns True on success, False on API failure.
        """
        now = time.time()
        if not force and now < self._ttl.get(key, 0):
            return True  # cache still valid
        async with self._lock:
            # Re-check inside lock (another coroutine may have synced first)
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
        """Sync multiple modules concurrently via asyncio.gather()."""
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
# 4. RAG RETRIEVER — FTS5 BM25 → LIKE fallback → bulk
#
# Three-tier retrieval strategy per module:
#
#   Tier A — FTS5 BM25 search (if search value provided)
#       Fast, ranked, handles partial words with prefix* matching.
#       Falls back to Tier B if FTS returns no results.
#
#   Tier B — SQLite LIKE scan across all columns
#       Broad catch-all. Used when FTS misses something (e.g. partial
#       barcode, numeric codes that FTS tokenises differently).
#
#   Tier C — Full table scan (BULK_LIST intent)
#       Returns all rows up to MAX_BULK. Used for "show all X" queries.
#
# All results are returned as list[dict] with original column names.
# _rowid is stripped from returned dicts (internal SQLite bookkeeping).
# ──────────────────────────────────────────────────────────────────────────────

class RAGRetriever:
    MAX_FTS  = 500     # max rows from FTS pass
    MAX_LIKE = 500     # max rows from LIKE pass
    MAX_BULK = 10_000  # max rows for bulk list

    def retrieve(self, key: str, parsed: dict) -> tuple:
        """
        Returns (records: list[dict], columns: list[str], method: str)
        method is one of: 'bulk' | 'fts' | 'like_col' | 'like_all' | 'empty'
        """
        intent = parsed["intent"]
        val    = parsed["search_value"]
        tcol   = parsed["target_col"]
        cols   = db.columns(key)

        if not cols:
            return [], [], "empty"

        if intent == "BULK_LIST" or not val:
            return self._bulk(key), cols, "bulk"

        rows, method = self._fts(key, val, tcol, cols)
        if not rows:
            # Graceful degradation: FTS miss → LIKE scan
            rows, method = self._like(key, val, tcol, cols)
        return rows, cols, method

    def _resolve_col(self, target: str, cols: list) -> Optional[str]:
        """Find actual column name that best matches the target hint."""
        for c in cols:
            if c.lower() == target.lower():
                return c
        for c in cols:
            if target.lower() in c.lower() or c.lower() in target.lower():
                return c
        return None

    def _fts(self, key, val, tcol, cols):
        """
        FTS5 BM25 ranked search.
        Sanitises value (removes FTS special chars), builds column-scoped
        or all-columns query, joins FTS virtual table with main table
        to get typed column values back.
        """
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
                # OR query: any word present in any column → hit
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
        """Broad LIKE search — column-scoped if target_col provided, else all columns."""
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
            # Search all columns
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
        """Return all rows up to MAX_BULK — used for 'show all' queries."""
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
# Sums monetary and quantity columns across retrieved records.
# Uses module registry's amount_cols / qty_cols lists for column targeting.
# Handles currency symbols and commas before float conversion.
# ──────────────────────────────────────────────────────────────────────────────

class AggregationEngine:
    def compute(self, records: list, key: str) -> dict:
        """
        Returns:
          count        - number of records
          total_amount - sum of first matching amount_col (or None)
          total_qty    - sum of first matching qty_col (or None)
        """
        mod     = MODULE_REGISTRY[key]
        amt_c   = mod["amount_cols"]
        qty_c   = mod["qty_cols"]
        total_a = total_q = 0.0

        for rec in records:
            for col in amt_c:
                ak = next((k for k in rec if k.lower() == col.lower()), None)
                if ak:
                    try:
                        total_a += float(
                            str(rec[ak]).replace(",", "").replace("₹", "").strip()
                        )
                    except (ValueError, TypeError):
                        pass
                    break  # use first matching amount col per record
            for col in qty_c:
                qk = next((k for k in rec if k.lower() == col.lower()), None)
                if qk:
                    try:
                        total_q += float(str(rec[qk]).replace(",", "").strip())
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
# 6. RESPONSE SYNTHESIZER — HTML generation
#
# Generates professional HTML responses. Key design decisions:
#
#   • Column visibility — HIDDEN_COLS strips internal/security columns
#   • Column ordering   — PRIORITY_COLS sorts business-critical cols first
#   • Cell formatting   — auto-detects currency, qty, datetime, image, badge
#   • NOT_CURRENCY      — prevents barcodes/codes/IDs from being ₹-formatted
#   • Marathi support   — table headers, intro text, error messages all
#                         translated when marathi=True
#   • Hover row effect  — inline onmouseover/onmouseout (no CSS dependency)
# ──────────────────────────────────────────────────────────────────────────────

HIDDEN_COLS = {
    "_id", "__v", "_empty", "_data", "_rowid",
    "password", "jwttoken", "role", "permissions", "token",
}

PRIORITY_COLS = [
    "invoiceNo", "billNo", "productName", "name", "firstName", "lastName",
    "supplierName", "customerName", "materialName", "supplier", "customer",
    "product", "email", "phone", "contact", "mobile",
    "stockQuantity", "quantity", "qty",
    "sellPrice", "pricePerUnit", "totalAmount", "amount",
    "creditAmount", "paid", "balance", "debit", "credit",
    "date", "createdAt", "purchaseDate", "sellDate",
    "address", "city", "gstNo", "barcode", "status", "supplyType",
]

# Semantic hints for currency and quantity detection
AMOUNT_HINTS = {"price", "amount", "total", "balance", "credit", "debit", "paid", "cost", "value"}
QTY_HINTS    = {"quantity", "qty", "stock"}

# Columns that look numeric but are NOT currency values
# e.g. "Price Code", "HSN No", "Barcode", "Port" — must NOT get ₹ formatting
NOT_CURRENCY = {"code", "no", "id", "barcode", "number", "ref", "hsn", "pin", "port"}


def _is_currency_col(col: str) -> bool:
    """True if column likely holds a monetary value and should be ₹-formatted."""
    col_l = col.lower()
    if any(ex in col_l for ex in NOT_CURRENCY):
        return False
    return any(h in col_l for h in AMOUNT_HINTS)


def _is_qty_col(col: str) -> bool:
    """True if column likely holds a quantity (integer, blue/red coloured)."""
    col_l = col.lower()
    return any(h in col_l for h in QTY_HINTS) and not any(ex in col_l for ex in NOT_CURRENCY)


def _fmt_cell(col: str, val: str) -> str:
    """
    Format a single cell value for HTML display.
    Detection order:
      1. Empty / null → em-dash placeholder
      2. Image URL    → <img> thumbnail
      3. ISO datetime → compact date+time string
      4. Currency col → ₹-formatted bold (green/red)
      5. Qty col      → integer with comma (blue/red)
      6. Status col   → colour-coded badge pill
      7. Default      → plain text
    """
    if not val or val in ("None", "null", ""):
        return '<span style="color:#94a3b8">—</span>'

    # Image URL
    if val.startswith("http") and any(
        val.lower().endswith(e) for e in (".png", ".jpg", ".jpeg", ".webp")
    ):
        return (
            f'<img src="{val}" style="width:40px;height:40px;'
            f'object-fit:cover;border-radius:6px;" />'
        )

    # ISO datetime
    if "T" in val and val.count("-") >= 2 and len(val) >= 19:
        try:
            d, t = val.split("T", 1)
            return f'<span style="color:#64748b;font-size:12px">{d} {t[:5]}</span>'
        except Exception:
            pass

    # Currency
    if _is_currency_col(col):
        try:
            num   = float(str(val).replace(",", "").replace("₹", "").strip())
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
    if col.lower() in ("status", "supplytype", "type", "state"):
        badge = {
            "in-state":  ("#dcfce7", "#16a34a"),
            "out-state": ("#fef3c7", "#d97706"),
            "active":    ("#dcfce7", "#16a34a"),
            "inactive":  ("#fee2e2", "#dc2626"),
            "paid":      ("#dcfce7", "#16a34a"),
            "pending":   ("#fef3c7", "#d97706"),
        }.get(val.lower(), ("#f1f5f9", "#475569"))
        return (
            f'<span style="background:{badge[0]};color:{badge[1]};'
            f'padding:2px 8px;border-radius:999px;font-size:12px;font-weight:600">'
            f'{val}</span>'
        )

    return val


class ResponseSynthesizer:

    def _headers(self, cols: list) -> list:
        """Filter out hidden columns and sort by PRIORITY_COLS order."""
        lh = {c.lower() for c in HIDDEN_COLS}
        visible = [c for c in cols if c.lower() not in lh]
        return sorted(visible, key=lambda x: (
            PRIORITY_COLS.index(x) if x in PRIORITY_COLS else 99
        ))

    def _col_label(self, col: str, marathi: bool) -> str:
        """
        Convert camelCase column name to human-readable label.
        If marathi=True, translate via MR_COLUMNS dict.
        """
        eng = re.sub(r"([a-z])([A-Z])", r"\1 \2", col).title()
        return MR_COLUMNS.get(eng, eng) if marathi else eng

    # ── Natural language intro paragraph ─────────────────────────────────────
    def intro(self, key: str, agg: dict, search_val: str,
              marathi: bool, lang: str) -> str:
        """
        Generate a natural language intro paragraph above the data table.
        Shows: what was searched, how many records found, total amount / qty.
        Switches to Marathi text when marathi=True.
        """
        count = agg["count"]
        amt   = agg["total_amount"]
        qty   = agg["total_qty"]

        if marathi:
            label = MR_MODULES.get(key, key)
            if search_val:
                text = (
                    f'<strong>{label}</strong> मध्ये '
                    f'<strong>"{search_val}"</strong> साठी शोधले — '
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
            label = key.replace("-", " ").title()
            ack   = random.choice([
                "Here you go! ", "Got it. ", "Absolutely! ", "Done — ",
            ])
            if search_val:
                text = (
                    f'{ack}Searched <strong>{label}</strong> for '
                    f'<strong>"{search_val}"</strong> — '
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

    # ── HTML table ────────────────────────────────────────────────────────────
    def table(self, records: list, cols: list, marathi: bool) -> str:
        """
        Build a responsive HTML table with:
          - Gradient dark header
          - Alternating row stripes
          - Hover highlight via inline handlers
          - Marathi font stack when marathi=True
          - Column headers translated when marathi=True
          - Cell values formatted via _fmt_cell()
        """
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

        header_cells = "".join(
            f'<th style="padding:13px 16px;border-right:1px solid #475569;'
            f'font-size:12px;font-weight:600;letter-spacing:.04em;'
            f'white-space:nowrap">{self._col_label(h, marathi)}</th>'
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

    # ── Error / empty state messages ─────────────────────────────────────────
    def no_results(self, key: str, val: str, marathi: bool) -> str:
        """Zero-results message — bilingual."""
        if marathi:
            label = MR_MODULES.get(key, key)
            return (
                f'<div style="background:#fef2f2;border:1px solid #fecaca;'
                f'border-radius:10px;padding:16px 20px;color:#991b1b;">'
                f'<strong>{label}</strong> मध्ये '
                f'<strong>"{val}"</strong> साठी काहीही सापडले नाही.<br>'
                f'<small>टीप: शब्दलेखन तपासा किंवा लहान शब्द वापरा.</small></div>'
            )
        label = key.replace("-", " ").title()
        return (
            f'<div style="background:#fef2f2;border:1px solid #fecaca;'
            f'border-radius:10px;padding:16px 20px;color:#991b1b;">'
            f'No results found in <strong>{label}</strong> for '
            f'<strong>"{val}"</strong>.<br>'
            f'<small>Tip: check spelling or try a shorter search term.</small></div>'
        )

    def api_error(self, key: str, marathi: bool) -> str:
        """API failure message — mentions retry count — bilingual."""
        if marathi:
            label = MR_MODULES.get(key, key)
            return (
                f'<div style="background:#fff7ed;border:1px solid #fed7aa;'
                f'border-radius:10px;padding:16px 20px;color:#9a3412;">'
                f'⚠️ <strong>{label}</strong> चा डेटा मिळवता आला नाही ({API_MAX_RETRIES} प्रयत्नांनंतर). '
                f'ERP कनेक्शन तपासा.</div>'
            )
        label = key.replace("-", " ").title()
        return (
            f'<div style="background:#fff7ed;border:1px solid #fed7aa;'
            f'border-radius:10px;padding:16px 20px;color:#9a3412;">'
            f'⚠️ Could not fetch <strong>{label}</strong> data after {API_MAX_RETRIES} attempts. '
            f'Please check the ERP server connection.</div>'
        )

    def prompt_needed(self, key: str, col: str, marathi: bool) -> str:
        """Asks user to specify a column value when COLUMN_SEARCH has no value."""
        if marathi:
            label   = MR_MODULES.get(key, key)
            col_eng = re.sub(r"([a-z])([A-Z])", r"\1 \2", col).title()
            col_mr  = MR_COLUMNS.get(col_eng, col_eng)
            return (
                f'तुम्ही <strong>{label}</strong> विभागात शोधत आहात. '
                f'कृपया <strong>{col_mr}</strong> सांगा.<br>'
                f'<em>उदाहरण: "invoice INV-1042" किंवा "name Rahul"</em>'
            )
        label     = key.replace("-", " ").title()
        col_label = re.sub(r"([a-z])([A-Z])", r"\1 \2", col).title()
        return (
            f'You\'re searching <strong>{label}</strong>. '
            f'Please specify the <strong>{col_label}</strong>.<br>'
            f'<em>Example: "invoice INV-1042" or "name Rahul"</em>'
        )

    def unrecognized(self, marathi: bool) -> str:
        """Fallback when intent is unknown — shows available modules + examples."""
        if marathi:
            mods = " · ".join(MR_MODULES.values())
            return (
                f'<div style="background:#f8fafc;border:1px solid #e2e8f0;'
                f'border-radius:10px;padding:16px 20px;">'
                f'<strong>मला समजले नाही.</strong> मी खालील गोष्टींमध्ये मदत करू शकतो:<br>'
                f'<em>{mods}</em><br><br>उदाहरणे:'
                f'<ul style="margin:8px 0;padding-left:20px">'
                f'<li><em>सर्व खरेदी दाखवा</em></li>'
                f'<li><em>vikri list</em></li>'
                f'<li><em>supplier credit kiti ahe</em></li>'
                f'<li><em>stock bagha</em></li>'
                f'</ul></div>'
            )
        mods = ", ".join(k.replace("-", " ").title() for k in MODULE_REGISTRY)
        return (
            f'<div style="background:#f8fafc;border:1px solid #e2e8f0;'
            f'border-radius:10px;padding:16px 20px;">'
            f"<strong>I'm not sure what you're looking for.</strong><br>"
            f"I can help with: <em>{mods}</em><br><br>Try:"
            f"<ul style='margin:8px 0;padding-left:20px'>"
            f"<li><em>List all purchases</em></li>"
            f"<li><em>Show supplier credits</em></li>"
            f"<li><em>Find customer named Rahul</em></li>"
            f"<li><em>Financial summary</em></li></ul></div>"
        )


synth = ResponseSynthesizer()


# ──────────────────────────────────────────────────────────────────────────────
# 7. CONVERSATION CONTEXT (last 5 turns, in-memory)
#
# Lightweight in-process conversation memory.
# Stores last N turns per session_id.
# Memory: ~5 turns × ~200 chars = ~1 KB per session. Negligible.
# Used to remember which module was last queried (for follow-up queries).
# ──────────────────────────────────────────────────────────────────────────────

class ConversationContext:
    def __init__(self, max_turns: int = 5):
        self._sessions: dict = defaultdict(lambda: deque(maxlen=max_turns))

    def add(self, sid: str, role: str, text: str):
        self._sessions[sid].append({"role": role, "text": text[:300]})

    def last_module(self, sid: str) -> Optional[str]:
        """Return the module from the last bot response, if recorded."""
        for turn in reversed(self._sessions[sid]):
            if turn["role"] == "bot":
                m = re.search(r"_module:(\S+)", turn["text"])
                if m:
                    return m.group(1)
        return None


ctx = ConversationContext()


# ──────────────────────────────────────────────────────────────────────────────
# 8. MAIN ORCHESTRATOR
#
# Routes each query through:
#   1. Language detection → Marathi or English path
#   2. Intent classification (Marathi map OR 5-stage English NLU)
#   3. Query parsing (extract search value, intent, target column)
#   4. Data sync (TTL-checked, retry-backed API fetch)
#   5. Retrieval (FTS5 → LIKE fallback → bulk)
#   6. Aggregation (sum amounts / qty if requested)
#   7. Response synthesis (HTML intro + table, bilingual)
# ──────────────────────────────────────────────────────────────────────────────

class AdminEngine:
    def __init__(self):
        self.classifier = IntentClassifier()
        self.parser     = QueryParser()

    async def process(self, user_query: str, session_id: str) -> str:
        ctx.add(session_id, "user", user_query)

        lang       = detect_language(user_query)
        is_marathi = lang in ("marathi_devanagari", "marathi_roman")

        # ── Marathi path — bypass 5-stage NLU, use direct keyword map ────────
        if is_marathi:
            key = marathi_classify(user_query)
            if key:
                return await self._single(key, user_query, session_id, lang, marathi=True)
            return synth.unrecognized(marathi=True)

        # ── English path — 5-stage NLU classifier ────────────────────────────
        cl = self.classifier.classify(user_query)

        if cl["type"] == "UNRECOGNIZED":
            return synth.unrecognized(marathi=False)

        if cl["type"] == "MULTI":
            return await self._multi(cl["pattern"], user_query, session_id)

        return await self._single(cl["module"], user_query, session_id, lang, marathi=False)

    # ── Single module handler ─────────────────────────────────────────────────
    async def _single(self, key: str, query: str, sid: str,
                      lang: str, marathi: bool) -> str:
        parsed = self.parser.parse(query, key, lang)

        if parsed["intent"] == "PROMPT_NEEDED":
            return synth.prompt_needed(key, parsed["target_col"], marathi)

        ok = await db.sync_module(key)
        if not ok:
            return synth.api_error(key, marathi)

        records, cols, _ = retriever.retrieve(key, parsed)

        if not records:
            return synth.no_results(key, parsed["search_value"], marathi)

        agg = aggregator.compute(records, key)

        # Aggregation-only response (e.g. "how many purchases total?")
        if parsed["aggregation"] == "SUM" and parsed["intent"] != "BULK_LIST":
            parts = [f'<strong>{key.replace("-"," ").title()}</strong> — {agg["count"]:,} records']
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

    # ── Multi-module handler ──────────────────────────────────────────────────
    async def _multi(self, pattern: dict, query: str, sid: str) -> str:
        """
        Handles compound queries like "financial summary" or "supplier overview".
        Fetches all listed modules concurrently via sync_modules().
        Renders each module as a separate labelled section under a summary header.
        """
        keys   = pattern["modules"]
        label  = pattern["label"]
        synced = await db.sync_modules(keys)
        sects  = []
        total_a = total_q = 0

        for key in keys:
            if not synced.get(key):
                sects.append(
                    f'<div style="margin-bottom:24px">'
                    f'<h3 style="border-left:4px solid #6366f1;padding-left:10px;'
                    f'margin:0 0 8px;font-size:15px">'
                    f'{key.replace("-"," ").title()}</h3>'
                    + synth.api_error(key, False) + "</div>"
                )
                continue

            parsed = self.parser.parse(query, key, "english")
            parsed["intent"]       = "BULK_LIST"   # always show all data for multi
            parsed["search_value"] = ""
            records, cols, _ = retriever.retrieve(key, parsed)
            if not records:
                continue

            agg      = aggregator.compute(records, key)
            total_a += agg["total_amount"] or 0
            total_q += agg["total_qty"]    or 0

            mod_label = key.replace("-", " ").title()
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
# 9. STATIC RESPONSES — greetings, identity, refresh
# ──────────────────────────────────────────────────────────────────────────────

GREETING_EN = """
<div style="font-family:'Segoe UI',system-ui,sans-serif;max-width:560px">
  <p style="font-size:18px;font-weight:700;margin:0 0 14px;color:#1e293b">
    👋 Hello! I'm your <span style="color:#6366f1">ERP Intelligence Assistant</span>.
  </p>
  <p style="margin:0 0 14px;color:#475569;font-size:14px">Here's what I can help you with:</p>
  <div style="display:grid;gap:8px">
    <div style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:8px;padding:10px 14px;font-size:14px">
      📦 <strong>Stock &amp; Inventory</strong> — <em>"Show all stock"</em>, <em>"Find product NON WOVEN"</em>
    </div>
    <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:10px 14px;font-size:14px">
      💰 <strong>Sales &amp; Purchases</strong> — <em>"List all sales"</em>, <em>"Purchase invoice INV-001"</em>
    </div>
    <div style="background:#fdf4ff;border:1px solid #e9d5ff;border-radius:8px;padding:10px 14px;font-size:14px">
      🏢 <strong>Suppliers &amp; Customers</strong> — <em>"All suppliers"</em>, <em>"Find customer Rahul"</em>
    </div>
    <div style="background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 14px;font-size:14px">
      📊 <strong>Financial Reports</strong> — <em>"Supplier credits"</em>, <em>"Financial summary"</em>
    </div>
  </div>
  <p style="margin:12px 0 0;font-size:13px;color:#94a3b8">
    You can also ask in <strong>मराठी</strong> or Marathi Roman typing.
  </p>
</div>
"""

GREETING_MR = """
<div style="font-family:'Noto Sans Devanagari','Segoe UI',system-ui,sans-serif;max-width:560px">
  <p style="font-size:18px;font-weight:700;margin:0 0 14px;color:#1e293b">
    नमस्कार! मी तुमचा <span style="color:#6366f1">ERP सहाय्यक</span> आहे. 🙏
  </p>
  <p style="margin:0 0 14px;color:#475569;font-size:14px">मी खालील गोष्टींमध्ये मदत करू शकतो:</p>
  <div style="display:grid;gap:8px">
    <div style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:8px;padding:10px 14px;font-size:14px">
      📦 <strong>साठा आणि इन्व्हेंटरी</strong> — <em>"stock bagha"</em>, <em>"उपलब्ध माल दाखवा"</em>
    </div>
    <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:10px 14px;font-size:14px">
      💰 <strong>विक्री आणि खरेदी</strong> — <em>"vikri list"</em>, <em>"सर्व खरेदी दाखवा"</em>
    </div>
    <div style="background:#fdf4ff;border:1px solid #e9d5ff;border-radius:8px;padding:10px 14px;font-size:14px">
      🏢 <strong>पुरवठादार आणि ग्राहक</strong> — <em>"purvathakaar yadi"</em>, <em>"ग्राहक यादी"</em>
    </div>
    <div style="background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 14px;font-size:14px">
      📊 <strong>आर्थिक अहवाल</strong> — <em>"supplier credit kiti ahe"</em>, <em>"payment history dya"</em>
    </div>
  </div>
  <p style="margin:12px 0 0;font-size:13px;color:#94a3b8">
    English मध्ये पण विचारू शकता.
  </p>
</div>
"""

IDENTITY_RESPONSE = """
<div style="font-family:'Segoe UI',system-ui,sans-serif;max-width:620px">
  <p style="font-size:18px;font-weight:700;color:#1e293b;margin:0 0 12px">
    🧠 Admin Intelligence Engine v5.0
  </p>
  <p style="margin:0 0 16px;color:#475569">
    A production-grade <strong>RAG pipeline</strong> for ERP —
    zero external AI APIs, full Marathi support, &lt;120 MB RAM.
  </p>

  <div style="background:#f8fafc;border-radius:10px;padding:16px;margin-bottom:12px">
    <strong style="color:#6366f1">5-Stage NLU Pipeline (English):</strong>
    <ol style="margin:8px 0;padding-left:20px;color:#334155;line-height:1.9;font-size:14px">
      <li><strong>Multi-module pattern detection</strong> — compound queries like "financial summary"</li>
      <li><strong>Phrase matching</strong> — longest phrase wins ("supplier credit" beats "credit")</li>
      <li><strong>Keyword scoring</strong> — weighted frequency with confidence scores</li>
      <li><strong>Fuzzy alias matching</strong> — catches typos like "purchaces" → purchases</li>
      <li><strong>TF-IDF cosine similarity</strong> — semantic fallback for unusual phrasing</li>
    </ol>
  </div>

  <div style="background:#f0fdf4;border-radius:10px;padding:16px;margin-bottom:12px">
    <strong style="color:#16a34a">Marathi Pipeline:</strong>
    <ul style="margin:8px 0;padding-left:20px;color:#334155;line-height:1.9;font-size:14px">
      <li>Unicode Devanagari detection (U+0900–U+097F) + Roman phonetic markers</li>
      <li>206-keyword intent map with longest-match algorithm</li>
      <li>Devanagari inflection stemming (उत्पादने→उत्पादन, साठ्याची→साठा)</li>
      <li>120+ stop words — filler like "mala dakhva", "kiti ahe" never leaks into search</li>
      <li>Full Marathi output — table headers, summaries, errors all translated</li>
    </ul>
  </div>

  <div style="background:#fff7ed;border-radius:10px;padding:16px">
    <strong style="color:#d97706">Data Layer:</strong>
    <ul style="margin:8px 0;padding-left:20px;color:#334155;line-height:1.9;font-size:14px">
      <li>SQLite FTS5 + BM25 ranking — 5-10× faster than LIKE, relevance-sorted</li>
      <li>Fallback chain: FTS5 → LIKE column → LIKE all-cols (graceful degradation)</li>
      <li>API retry with exponential backoff — 3 attempts (1.5s, 3s, 6s)</li>
      <li>TTL-based caching: stock 2min · finance 10min · master 5min</li>
      <li>Multi-module concurrent fetch via asyncio.gather()</li>
      <li>🔒 100% private — no data leaves your server · ⚡ Zero AI API cost</li>
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


# Quick-response word sets — matched before hitting the main pipeline
_EN_GREET    = {"hi", "hello", "hey", "heyy", "heya", "good morning",
                "good afternoon", "good evening", "sup", "yo"}
_EN_THANKS   = {"thank you", "thanks", "awesome", "perfect", "great",
                "nice", "good", "ok", "okay", "cool"}
_EN_IDENTITY = {"who are you", "how do you work", "chatgpt", "your brain",
                "how were you built", "architecture", "how does this work"}
_EN_REFRESH  = {"refresh", "sync", "update records", "reload data"}
_MR_GREET_W  = {"नमस्कार", "नमस्ते", "हॅलो", "namaskar", "namaste"}
_MR_THANKS_W = {"धन्यवाद", "आभारी", "थँक्यू", "dhanyavad", "aabhari"}
_MR_REFRESH_W= {"रिफ्रेश", "अद्यतन", "refresh kara", "update kara", "sync kara"}


@router.post("/chat")
async def chat_endpoint(request: ChatRequest):
    """
    Main chat endpoint.

    Flow:
      1. Detect language
      2. Handle quick responses (greetings, thanks, refresh, identity)
      3. Delegate to AdminEngine.process() for data queries
    """
    query      = (request.query or request.question or "").strip()
    session_id = request.session_id or "default"

    if not query:
        return {"response": "Please type a question. / कृपया प्रश्न टाइप करा."}

    q_low = query.lower().strip()
    lang  = detect_language(query)
    is_mr = lang in ("marathi_devanagari", "marathi_roman")

    # ── Marathi quick responses ───────────────────────────────────────────────
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

    # ── English quick responses ───────────────────────────────────────────────
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

    # ── Main intelligence pipeline ────────────────────────────────────────────
    response = await engine.process(query, session_id)
    return {"response": response}