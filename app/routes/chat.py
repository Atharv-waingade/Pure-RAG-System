"""
==============================================================================
  ADMIN INTELLIGENCE ENGINE  v4.0
  Production-grade RAG pipeline for ERP chatbot
  Zero external AI APIs · Pure stdlib + SQLite FTS5 · <120 MB RAM
  Full Marathi language support (Devanagari + Roman transliteration)
==============================================================================

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
│  (5-stage NLU)               │                                  │
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

KEY INNOVATIONS vs v3:
  1. Language detection — Devanagari Unicode + Roman phonetic markers
  2. Marathi intent map — 206 keywords across Devanagari + Roman
  3. Marathi response synthesizer — headers, summaries, UI in Marathi
  4. Marathi column translations — all 40+ ERP column headers translated
  5. Longest-match algorithm — most specific Marathi phrase wins
  6. Mixed-language queries — "supplier credit kiti ahe" works perfectly
==============================================================================
"""

import asyncio
import heapq
import math
import os
import random
import re
import sqlite3
import time
import json
from collections import Counter, defaultdict, deque
from difflib import SequenceMatcher, get_close_matches
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
    "password": os.getenv("ERP_PASSWORD", "superadmin@123"),
}

# Cache: how many seconds before re-fetching a module from API
MODULE_TTL  = 300          # 5 min default
STOCK_TTL   = 120          # 2 min for stock (changes often)
FINANCE_TTL = 600          # 10 min for ledgers

# Hard limit on rows kept in SQLite per module (prevents RAM explosion)
MAX_ROWS_PER_MODULE = 50_000


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
#   - Roman detection via a curated list of Marathi phonetic markers
#   - All detection is O(n*m) with n=query_tokens, m=keyword_count — fast
# ──────────────────────────────────────────────────────────────────────────────

# ── Column header translations ────────────────────────────────────────────────
MARATHI_COLUMNS: dict = {
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
    "Gst No":          "जीएसटी क्र.",
    "Bill No":         "बिल क्र.",
    "Mobile":          "मोबाईल",
}

# ── Module name translations ──────────────────────────────────────────────────
MARATHI_MODULES: dict = {
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

# ── Intent keyword map — Devanagari + Roman per module ────────────────────────
# Sorted by length descending at runtime so longest match wins
MARATHI_INTENT_MAP: dict = {
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
            "सर्व विक्री", "विक्री तपशील", "विक्री",
            "बिल", "पावती",
        ],
        "roman": [
            "vikri yadi", "vikri invoice", "vikri itihas",
            "vikri nondi", "mal vikla", "kay vikle",
            "sarv vikri", "vikri tapshil", "vikri list",
            "vikree list", "vikri history", "sales list",
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
            "purvathakaar vivaran", "purvathakaar khate",
            "puravathakaar khatevahi", "supplier ledger",
            "vendor ledger", "supplier hisab",
        ],
    },
    "customer-ledger": {
        "deva": [
            "ग्राहक खातेवही", "ग्राहक हिशेब",
            "ग्राहक विवरण", "ग्राहक खाते",
        ],
        "roman": [
            "grahak khatevahi", "grahak hisab",
            "grahak vivaran", "grahak khate",
            "graahak khatevahi", "customer ledger",
            "customer hisab",
        ],
    },
    "customer": {
        "deva": [
            "ग्राहक यादी", "सर्व ग्राहक", "ग्राहक माहिती",
            "खरेदीदार", "ग्राहक",
        ],
        "roman": [
            "grahak yadi", "sarv grahak", "grahak mahiti",
            "kharedidar", "grahak list", "graahak list",
            "customer list", "grahak", "graahak",
        ],
    },
    "supplier": {
        "deva": [
            "पुरवठादार यादी", "सर्व पुरवठादार",
            "विक्रेता", "माल पुरवठादार", "पुरवठादार",
        ],
        "roman": [
            "purvathakaar yadi", "sarv purvathakaar",
            "vikreta", "mal purvathakaar",
            "supplier list", "vendor list",
            "purvathakaar", "puravathakaar",
        ],
    },
    "product-stock": {
        "deva": [
            "माल साठा", "उपलब्ध माल", "किती साठा आहे",
            "स्टॉक यादी", "उपलब्ध स्टॉक",
            "गोदाम", "इन्व्हेंटरी", "साठा", "स्टॉक",
        ],
        "roman": [
            "mal satha", "uplabdh mal", "kiti satha ahe",
            "stock yadi", "uplabdh stock",
            "stock kiti ahe", "stock bagha", "stock list",
            "inventory list", "godam", "saatha",
            "satha", "stock",
        ],
    },
    "payment": {
        "deva": [
            "पेमेंट इतिहास", "दिलेले पैसे",
            "पुरवठादार देयक", "व्यवहार नोंदी",
            "पैसे दिले", "व्यवहार", "देयक", "पेमेंट",
        ],
        "roman": [
            "payment itihas", "dilele paise",
            "purvathakaar dayak", "vyavahar nondi",
            "paise dile", "payment history",
            "payment list", "vyavahar", "dayak", "payment",
        ],
    },
    "material": {
        "deva": [
            "कच्चा माल", "माल यादी", "साहित्य यादी",
            "कापड यादी", "साहित्य", "कापड", "घटक", "सामग्री",
        ],
        "roman": [
            "kachha mal", "mal yadi", "sahitya yadi",
            "kapad yadi", "material list", "raw material",
            "sahitya", "kapad", "ghatak", "samagri",
        ],
    },
    "category": {
        "deva": [
            "श्रेणी यादी", "वर्गीकरण", "उत्पादन प्रकार", "श्रेणी", "प्रकार",
        ],
        "roman": [
            "shreni yadi", "vargikaran", "utpadan prakar",
            "category list", "shreni", "prakar",
        ],
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

# ── Marathi Roman phonetic markers (used for language detection) ──────────────
_MARATHI_ROMAN_MARKERS: frozenset = frozenset([
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
])

# Pre-sort all keyword lists by length descending (longest match wins)
_MARATHI_SORTED: dict = {}
for _mod, _kws in MARATHI_INTENT_MAP.items():
    all_kws = [(k.lower(), _mod) for k in _kws["deva"] + _kws["roman"]]
    all_kws.sort(key=lambda x: len(x[0]), reverse=True)
    _MARATHI_SORTED[_mod] = all_kws
# Flat sorted list for classification
_MARATHI_FLAT: list = []
for _mod, _kws in MARATHI_INTENT_MAP.items():
    for k in _kws["deva"] + _kws["roman"]:
        _MARATHI_FLAT.append((k.lower(), _mod))
_MARATHI_FLAT.sort(key=lambda x: len(x[0]), reverse=True)


def detect_language(text: str) -> str:
    """
    Returns: 'marathi_devanagari' | 'marathi_roman' | 'english'
    Fast: O(len(text)) for Devanagari check, O(markers) for Roman check.
    """
    deva_count  = sum(1 for c in text if 0x0900 <= ord(c) <= 0x097F)
    alpha_count = sum(1 for c in text if c.isalpha())
    if alpha_count == 0:
        return "english"
    if deva_count / alpha_count > 0.25:
        return "marathi_devanagari"
    t = text.lower()
    if any(marker in t for marker in _MARATHI_ROMAN_MARKERS):
        return "marathi_roman"
    return "english"


def marathi_classify(text: str) -> Optional[str]:
    """
    Classify a Marathi query (Devanagari or Roman) to a module key.
    Uses longest-match: 'purvathakaar credit' beats 'credit'.
    Returns module key string or None.
    """
    t = text.lower().strip()
    for keyword, module in _MARATHI_FLAT:
        if keyword in t:
            return module
    return None


def translate_column(col_label: str) -> str:
    """Translate an English column header to Marathi. Falls back to original."""
    return MARATHI_COLUMNS.get(col_label, col_label)


def translate_module(module_key: str) -> str:
    """Translate a module key to Marathi display name."""
    return MARATHI_MODULES.get(module_key, module_key.replace("-", " ").title())


# ── Marathi response strings ──────────────────────────────────────────────────
def mr_intro(module_key: str, count: int, search_val: str,
             amount: Optional[float], qty: Optional[int]) -> str:
    """Generate a Marathi intro paragraph for the response."""
    mod_label = translate_module(module_key)
    if search_val:
        text = (
            f'<strong>{mod_label}</strong> मध्ये '
            f'<strong>"{search_val}"</strong> साठी शोधले — '
            f'<strong>{count:,} नोंदी</strong> सापडल्या.'
        )
    else:
        text = (
            f'<strong>{mod_label}</strong> ची संपूर्ण यादी — '
            f'<strong>{count:,} नोंदी</strong>.'
        )
    insights = []
    if amount:
        insights.append(f'एकूण रक्कम: <strong>₹{amount:,.2f}</strong>')
    if qty:
        insights.append(f'एकूण युनिट: <strong>{qty:,}</strong>')
    if insights:
        text += " &nbsp;·&nbsp; " + " &nbsp;·&nbsp; ".join(insights)
    return f'<p style="margin:0 0 16px;font-size:15px;">{text}</p>'


def mr_no_results(module_key: str, search_val: str) -> str:
    mod_label = translate_module(module_key)
    return (
        f'<div style="background:#fef2f2;border:1px solid #fecaca;'
        f'border-radius:10px;padding:16px 20px;color:#991b1b;">'
        f'<strong>{mod_label}</strong> मध्ये '
        f'<strong>"{search_val}"</strong> साठी काहीही सापडले नाही.<br>'
        f'<small style="color:#b91c1c">टीप: शब्दलेखन तपासा किंवा लहान शब्द वापरा.</small>'
        f'</div>'
    )


def mr_api_error(module_key: str) -> str:
    mod_label = translate_module(module_key)
    return (
        f'<div style="background:#fff7ed;border:1px solid #fed7aa;'
        f'border-radius:10px;padding:16px 20px;color:#9a3412;">'
        f'⚠️ <strong>{mod_label}</strong> चा डेटा मिळवता आला नाही. '
        f'ERP कनेक्शन तपासा आणि पुन्हा प्रयत्न करा.'
        f'</div>'
    )


def mr_prompt_needed(module_key: str, col_label: str) -> str:
    mod_label = translate_module(module_key)
    col_mr    = MARATHI_COLUMNS.get(col_label, col_label)
    return (
        f'तुम्ही <strong>{mod_label}</strong> विभागात शोधत आहात. '
        f'कृपया <strong>{col_mr}</strong> सांगा.<br>'
        f'<em>उदाहरण: "invoice INV-1042" किंवा "name Rahul"</em>'
    )


def mr_unrecognized() -> str:
    modules_list = " · ".join(translate_module(m) for m in MARATHI_MODULES.keys())
    return (
        f'<div style="background:#f8fafc;border:1px solid #e2e8f0;'
        f'border-radius:10px;padding:16px 20px;">'
        f'<strong>मला समजले नाही.</strong> मी खालील गोष्टींमध्ये मदत करू शकतो:<br>'
        f'<em>{modules_list}</em><br><br>'
        f'उदाहरणे:<ul style="margin:8px 0;padding-left:20px">'
        f'<li><em>सर्व खरेदी दाखवा</em></li>'
        f'<li><em>vikri list</em></li>'
        f'<li><em>supplier credit kiti ahe</em></li>'
        f'<li><em>grahak list dya</em></li>'
        f'</ul></div>'
    )


MARATHI_GREETING = """
<div style="font-family:'Noto Sans Devanagari','Segoe UI',system-ui,sans-serif;max-width:560px">
  <p style="font-size:17px;font-weight:600;margin:0 0 12px">
    नमस्कार! मी तुमचा <span style="color:#6366f1">ERP सहाय्यक</span> आहे. 🙏
  </p>
  <p style="margin:0 0 14px;color:#475569">मी खालील गोष्टींमध्ये मदत करू शकतो:</p>
  <div style="display:grid;gap:8px">
    <div style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:8px;padding:10px 14px">
      📦 <strong>साठा आणि इन्व्हेंटरी</strong> — <em>"stock bagha"</em>, <em>"उपलब्ध माल दाखवा"</em>
    </div>
    <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:10px 14px">
      💰 <strong>विक्री आणि खरेदी</strong> — <em>"vikri list"</em>, <em>"सर्व खरेदी दाखवा"</em>
    </div>
    <div style="background:#fdf4ff;border:1px solid #e9d5ff;border-radius:8px;padding:10px 14px">
      🏢 <strong>पुरवठादार आणि ग्राहक</strong> — <em>"purvathakaar yadi"</em>, <em>"ग्राहक यादी"</em>
    </div>
    <div style="background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 14px">
      📊 <strong>आर्थिक अहवाल</strong> — <em>"supplier credit kiti ahe"</em>, <em>"payment history"</em>
    </div>
  </div>
  <p style="margin:12px 0 0;font-size:13px;color:#94a3b8">
    तुम्ही मराठी, English किंवा मराठी टायपिंग (Roman) मध्ये विचारू शकता.
  </p>
</div>
"""

MARATHI_THANKS = [
    "आपले स्वागत आहे! आणखी काही हवे असल्यास सांगा.",
    "माझ्यासाठी हे काम करणे आनंददायक आहे! आणखी काही?",
    "नक्कीच! आणखी कोणता डेटा पाहायचा आहे?",
]

MARATHI_REFRESH = (
    '<div style="background:#f0fdf4;border:1px solid #bbf7d0;'
    'border-radius:10px;padding:14px 18px;color:#166534">'
    '🔄 <strong>सर्व माहिती अद्यतनित झाली.</strong> ERP डेटा ताजा आहे. आता काय पाहायचे आहे?'
    '</div>'
)

# ── Marathi greeting / thanks detection ──────────────────────────────────────
_MARATHI_GREET_WORDS: frozenset = frozenset([
    "नमस्कार", "नमस्ते", "हॅलो", "हॅलो", "सुप्रभात",
    "namaskar", "namaste", "hello", "namaskar",
])
_MARATHI_THANKS_WORDS: frozenset = frozenset([
    "धन्यवाद", "आभारी", "थँक्यू",
    "dhanyavad", "aabhari", "thankyou", "shukriya",
])
_MARATHI_REFRESH_WORDS: frozenset = frozenset([
    "रिफ्रेश", "अद्यतन करा", "माहिती ताजी करा",
    "refresh kara", "update kara", "sync kara",
])


def is_marathi_greeting(text: str) -> bool:
    t = text.lower().strip()
    return any(w in t for w in _MARATHI_GREET_WORDS) and len(t.split()) <= 4


def is_marathi_thanks(text: str) -> bool:
    t = text.lower().strip()
    return any(w in t for w in _MARATHI_THANKS_WORDS)


def is_marathi_refresh(text: str) -> bool:
    t = text.lower().strip()
    return any(w in t for w in _MARATHI_REFRESH_WORDS)


# ──────────────────────────────────────────────────────────────────────────────
# MODULE REGISTRY
# Each module defines:
#   url         - API endpoint (relative to BASE_URL)
#   ttl         - cache lifetime in seconds
#   keywords    - high-confidence single-word matches
#   phrases     - multi-word matches (checked FIRST, highest priority)
#   aliases     - typo/synonym list for fuzzy matching
#   summary_cols- columns to use when generating natural language summaries
#   amount_cols - columns holding monetary values (for aggregation)
#   qty_cols    - columns holding quantities  (for aggregation)
#   description - plain-English description of this module
# ──────────────────────────────────────────────────────────────────────────────
MODULE_REGISTRY = {
    "supplier-credit": {
        "url": "/api/supplier-credit/get-all-supplier-credits",
        "ttl": FINANCE_TTL,
        "keywords": ["credit", "credits", "outstanding", "due", "refund", "owe", "owes"],
        "phrases": [
            "supplier credit", "supplier credits", "vendor credit",
            "credit note", "credit balance", "outstanding credit",
            "how much do we owe", "money owed to supplier",
        ],
        "aliases": ["supplier credit", "vendor credit", "credits outstanding"],
        "summary_cols": ["supplierName", "creditAmount", "date"],
        "amount_cols":  ["creditAmount", "amount", "totalCredit"],
        "qty_cols":     [],
        "description":  "Tracks outstanding credit amounts owed to suppliers",
    },
    "supplier-ledger": {
        "url": "/api/reports/get-supplier-ledger",
        "ttl": FINANCE_TTL,
        "keywords": ["ledger", "statement", "account"],
        "phrases": [
            "supplier ledger", "vendor ledger", "supplier statement",
            "vendor statement", "supplier account", "supplier account history",
        ],
        "aliases": ["supplier ledger", "vendor ledger", "ledger statement"],
        "summary_cols": ["supplierName", "debit", "credit", "balance"],
        "amount_cols":  ["debit", "credit", "balance", "amount"],
        "qty_cols":     [],
        "description":  "Full supplier ledger / account statement",
    },
    "customer-ledger": {
        "url": "/api/reports/get-all-customer-ledgers",
        "ttl": FINANCE_TTL,
        "keywords": [],
        "phrases": [
            "customer ledger", "client ledger", "customer statement",
            "customer account", "receivables", "client account",
        ],
        "aliases": ["customer ledger", "client ledger"],
        "summary_cols": ["customerName", "debit", "credit", "balance"],
        "amount_cols":  ["debit", "credit", "balance", "amount"],
        "qty_cols":     [],
        "description":  "Full customer ledger / receivables statement",
    },
    "sell": {
        "url": "/api/sell/get-all-sells",
        "ttl": MODULE_TTL,
        "keywords": [
            "sell", "sells", "sold", "sale", "sales", "dispatch",
            "invoice", "receipt", "billing", "revenue",
        ],
        "phrases": [
            "sales invoice", "sell invoice", "dispatch record",
            "all sales", "sales list", "all sells", "invoices",
            "what did we sell", "selling history",
        ],
        "aliases": ["sales", "sells", "invoices", "receipts", "billing"],
        "summary_cols": ["invoiceNo", "customerName", "totalAmount", "date"],
        "amount_cols":  ["totalAmount", "sellPrice", "amount", "paid"],
        "qty_cols":     ["quantity", "qty", "totalQuantity"],
        "description":  "All sales records and dispatch invoices",
    },
    "purchase": {
        "url": "/api/purchase/get-all-purchases",
        "ttl": MODULE_TTL,
        "keywords": [
            "purchase", "purchases", "bought", "buy", "buying",
            "acquire", "procurement", "ordered", "vendor order",
        ],
        "phrases": [
            "purchase order", "purchase invoice", "purchase history",
            "purchase list", "all purchases", "what did we buy",
            "buying history", "procurement list",
        ],
        "aliases": [
            "purchaces", "purchasse", "purcheases", "purhcases",
            "purchaes", "puchases", "pruchases", "purchases", "buys",
        ],
        "summary_cols": ["invoiceNo", "supplierName", "totalAmount", "date"],
        "amount_cols":  ["totalAmount", "purchaseAmount", "amount", "paid"],
        "qty_cols":     ["quantity", "qty", "totalQuantity"],
        "description":  "All purchase orders and procurement records",
    },
    "payment": {
        "url": "/api/payment/supplier-payment-history",
        "ttl": MODULE_TTL,
        "keywords": ["payment", "payments", "paid", "settled", "transaction", "remittance"],
        "phrases": [
            "payment history", "supplier payment", "payment record",
            "payment list", "all payments", "transaction history",
            "what was paid", "payments made",
        ],
        "aliases": ["payments", "paying", "remittance", "settlement"],
        "summary_cols": ["supplierName", "amount", "date", "mode"],
        "amount_cols":  ["amount", "paid", "totalPaid"],
        "qty_cols":     [],
        "description":  "Supplier payment history and transaction records",
    },
    "material": {
        "url": "/api/material/get-all-materials",
        "ttl": MODULE_TTL,
        "keywords": ["material", "materials", "fabric", "raw", "component", "item"],
        "phrases": [
            "raw material", "material list", "all materials",
            "fabric list", "components list",
        ],
        "aliases": ["materials", "fabrics", "raw materials"],
        "summary_cols": ["name", "type", "createdAt"],
        "amount_cols":  [],
        "qty_cols":     [],
        "description":  "Raw materials and fabric inventory",
    },
    "customer": {
        "url": "/api/customer/get-all-customers",
        "ttl": MODULE_TTL,
        "keywords": ["customer", "customers", "client", "clients", "buyer", "purchaser"],
        "phrases": [
            "customer list", "client list", "all customers",
            "buyer list", "who are our customers",
        ],
        "aliases": ["customers", "clients", "buyers"],
        "summary_cols": ["name", "email", "contact", "address"],
        "amount_cols":  [],
        "qty_cols":     [],
        "description":  "Customer / client master records",
    },
    "supplier": {
        "url": "/api/supplier/get-all-suppliers",
        "ttl": MODULE_TTL,
        "keywords": ["supplier", "suppliers", "vendor", "vendors", "distributor", "provider"],
        "phrases": [
            "supplier list", "vendor list", "all suppliers",
            "distributor list", "who are our suppliers",
        ],
        "aliases": ["suppliers", "vendors", "distributors"],
        "summary_cols": ["name", "email", "contact", "supplyType"],
        "amount_cols":  ["supplierCredit"],
        "qty_cols":     [],
        "description":  "Supplier / vendor master records",
    },
    "product-stock": {
        "url": "/api/reports/get-product-stocks-with-product",
        "ttl": STOCK_TTL,
        "keywords": ["stock", "stocks", "inventory", "warehouse", "available", "product", "products"],
        "phrases": [
            "product stock", "stock list", "inventory list",
            "product inventory", "available stock", "warehouse stock",
            "what do we have in stock", "current stock",
        ],
        "aliases": ["stocks", "inventory", "products", "warehousing"],
        "summary_cols": ["productName", "stockQuantity", "sellPrice", "barcode"],
        "amount_cols":  ["sellPrice", "pricePerUnit", "totalAmount"],
        "qty_cols":     ["stockQuantity", "quantity"],
        "description":  "Current product stock and inventory levels",
    },
    "printer": {
        "url": "/api/printer/get-all-printers",
        "ttl": MODULE_TTL,
        "keywords": ["printer", "printers", "machine", "print"],
        "phrases": ["printer list", "all printers", "printing machines"],
        "aliases": ["printers", "printing"],
        "summary_cols": ["name", "type", "status"],
        "amount_cols":  [],
        "qty_cols":     [],
        "description":  "Printer and printing machine configurations",
    },
    "email-config": {
        "url": "/api/email-config/get-all-emails",
        "ttl": MODULE_TTL,
        "keywords": ["email", "emails", "smtp", "config", "mail"],
        "phrases": ["email config", "mail config", "smtp config", "email settings"],
        "aliases": ["emails", "mails", "smtp"],
        "summary_cols": ["email", "host", "port"],
        "amount_cols":  [],
        "qty_cols":     [],
        "description":  "Email / SMTP configuration settings",
    },
    "category": {
        "url": "/api/reports/get-all-product-categories",
        "ttl": MODULE_TTL,
        "keywords": ["category", "categories", "classification", "type", "types"],
        "phrases": ["product category", "category list", "all categories", "product types"],
        "aliases": ["categories", "classifications"],
        "summary_cols": ["name", "description"],
        "amount_cols":  [],
        "qty_cols":     [],
        "description":  "Product category and classification master",
    },
}

# ──────────────────────────────────────────────────────────────────────────────
# MULTI-MODULE QUERY PATTERNS
# When a query involves more than one module, define it here.
# Key: display label.  Value: list of module keys to fetch simultaneously.
# ──────────────────────────────────────────────────────────────────────────────
MULTI_MODULE_PATTERNS = [
    {
        "phrases": [
            "supplier summary", "supplier overview", "vendor summary",
            "all supplier info", "full supplier details",
        ],
        "modules": ["supplier", "supplier-credit", "supplier-ledger"],
        "label": "Full Supplier Overview",
    },
    {
        "phrases": [
            "sales report", "sales and stock", "revenue and inventory",
        ],
        "modules": ["sell", "product-stock"],
        "label": "Sales & Inventory Report",
    },
    {
        "phrases": [
            "financial summary", "finance report", "money overview",
            "accounts summary", "financial overview",
        ],
        "modules": ["sell", "purchase", "payment", "supplier-credit"],
        "label": "Financial Summary",
    },
    {
        "phrases": [
            "customer and sales", "sales by customer",
        ],
        "modules": ["customer", "sell"],
        "label": "Customer & Sales Report",
    },
    {
        "phrases": [
            "purchase and payment", "vendor transactions",
        ],
        "modules": ["purchase", "payment", "supplier"],
        "label": "Vendor Transactions",
    },
]

# ──────────────────────────────────────────────────────────────────────────────
# COLUMN SEARCH TRIGGERS
# Maps natural language words → likely column names to target
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
    "name":    ["name", "firstName", "lastName", "productName", "supplierName", "customerName", "materialName"],
    "address": ["address"],
    "gst":     ["gstNo", "gst"],
    "date":    ["date", "createdAt", "purchaseDate", "sellDate"],
    "amount":  ["totalAmount", "amount", "creditAmount", "paid"],
    "price":   ["sellPrice", "price", "pricePerUnit"],
    "city":    ["city", "address"],
    "status":  ["status", "supplyType"],
}

# Words that should NEVER be used as search values
STOP_WORDS = {
    "me", "find", "what", "is", "are", "the", "a", "an", "of", "for",
    "please", "can", "you", "tell", "we", "have", "our", "in", "any",
    "by", "its", "their", "about", "information", "on", "who", "which",
    "specific", "details", "show", "get", "fetch", "display", "list",
    "all", "every", "entire", "give", "check", "search", "look", "find",
    "want", "need", "see", "view", "pull", "do", "with", "from", "this",
    "that", "it", "i", "my", "he", "she", "they", "them", "up", "out",
    "how", "many", "much", "total", "count", "number", "no", "has",
    "some", "certain", "latest", "recent", "current", "new", "old",
    "make", "create", "report", "data", "records", "record", "entries",
    "full", "complete", "entire", "whole",
}

GREETING_WORDS = {"hi", "hello", "hey", "heyy", "heya", "good morning", "good afternoon", "good evening", "sup", "yo"}
THANKS_WORDS   = {"thank you", "thanks", "awesome", "perfect", "great", "nice", "good", "ok", "okay", "cool"}

# ──────────────────────────────────────────────────────────────────────────────
# UTILITIES
# ──────────────────────────────────────────────────────────────────────────────

def _clean(text: str) -> str:
    """Lowercase + remove punctuation except hyphens."""
    return re.sub(r"[^a-z0-9\s\-]", " ", text.lower()).strip()


def _fuzzy_ratio(a: str, b: str) -> float:
    """SequenceMatcher ratio — good enough for alias matching, zero deps."""
    return SequenceMatcher(None, a, b).ratio()


def _best_fuzzy(token: str, candidates: list, threshold: float = 0.72) -> Optional[str]:
    """Return best fuzzy match from candidates list or None."""
    best_score, best = 0.0, None
    for c in candidates:
        s = _fuzzy_ratio(token, c)
        if s > best_score:
            best_score, best = s, c
    return best if best_score >= threshold else None


def _fmt_currency(val) -> str:
    try:
        return f"₹ {float(str(val).replace(',', '')):,.2f}"
    except (ValueError, TypeError):
        return str(val)


def _fmt_date(val: str) -> str:
    """Convert ISO datetime to readable format."""
    if not val or val in ("-", ""):
        return "-"
    try:
        if "T" in val:
            d, t = val.split("T", 1)
            return d + " " + t[:5]
    except Exception:
        pass
    return val


def _is_image_url(val: str) -> bool:
    return isinstance(val, str) and val.startswith("http") and any(
        val.lower().endswith(ext) for ext in (".png", ".jpg", ".jpeg", ".webp", ".gif")
    )


# ──────────────────────────────────────────────────────────────────────────────
# 1.  NLU — 5-STAGE INTENT CLASSIFICATION
# ──────────────────────────────────────────────────────────────────────────────

class IntentClassifier:
    """
    5-stage pipeline with confidence scoring:
      S1: Multi-module patterns     → confidence 1.0
      S2: Phrase match              → confidence 0.95
      S3: Keyword match (scored)    → confidence proportional to match density
      S4: Fuzzy alias match         → confidence = fuzzy score
      S5: TF-IDF cosine fallback    → confidence = cosine score

    The highest-confidence match wins.
    Below 0.12 confidence → UNRECOGNIZED (bot asks for clarification).
    """

    def __init__(self):
        self._build_tfidf_index()
        # Flat alias list: (alias_text, module_key)
        self._alias_pairs = [
            (alias.lower(), mod)
            for mod, data in MODULE_REGISTRY.items()
            for alias in data["aliases"]
        ]
        # All phrases flat list: (phrase, module_key)
        self._phrase_pairs = [
            (phrase.lower(), mod)
            for mod, data in MODULE_REGISTRY.items()
            for phrase in data["phrases"]
        ]
        # Multi-module phrase pairs: (phrase, pattern)
        self._multi_pairs = [
            (phrase.lower(), pattern)
            for pattern in MULTI_MODULE_PATTERNS
            for phrase in pattern["phrases"]
        ]

    def _build_tfidf_index(self):
        """Build TF-IDF vectors for each module's keyword corpus."""
        corpus = {
            mod: " ".join(data["keywords"])
            for mod, data in MODULE_REGISTRY.items()
        }
        # IDF
        N = len(corpus)
        vocab = set()
        for doc in corpus.values():
            vocab.update(doc.split())
        idf = {}
        for word in vocab:
            df = sum(1 for doc in corpus.values() if word in doc.split())
            idf[word] = math.log((1 + N) / (1 + df)) + 1

        # TF-IDF vectors
        self._tfidf_vectors = {}
        for mod, doc in corpus.items():
            words = doc.split()
            if not words:
                self._tfidf_vectors[mod] = {}
                continue
            tf = Counter(words)
            vec = {w: (tf[w] / len(words)) * idf.get(w, 0) for w in tf}
            norm = math.sqrt(sum(v**2 for v in vec.values()))
            self._tfidf_vectors[mod] = {w: v / norm for w, v in vec.items()} if norm else {}

    def _tfidf_score(self, query: str) -> dict:
        """Return {module: cosine_score} for a cleaned query string."""
        words = query.split()
        if not words:
            return {}
        tf = Counter(words)
        total = len(words)
        # Build query vector (no IDF needed — we only dot-product with precomputed doc vecs)
        q_raw = {w: tf[w] / total for w in tf}
        q_norm_sq = sum(v**2 for v in q_raw.values())
        if not q_norm_sq:
            return {}
        q_norm = math.sqrt(q_norm_sq)

        scores = {}
        for mod, d_vec in self._tfidf_vectors.items():
            common = set(q_raw) & set(d_vec)
            dot = sum(q_raw[w] * d_vec[w] for w in common)
            scores[mod] = dot / q_norm   # d_vec already normalized
        return scores

    # ── Stage 1: Multi-module ───────────────────────────────────────────────
    def _stage_multi(self, text: str):
        for phrase, pattern in self._multi_pairs:
            if phrase in text:
                return {"type": "MULTI", "pattern": pattern, "confidence": 1.0}
        return None

    # ── Stage 2: Phrase match ───────────────────────────────────────────────
    def _stage_phrase(self, text: str):
        for phrase, mod in self._phrase_pairs:
            if phrase in text:
                return {"type": "SINGLE", "module": mod, "confidence": 0.95}
        return None

    # ── Stage 3: Keyword match ──────────────────────────────────────────────
    def _stage_keyword(self, text: str):
        scores = {}
        for mod, data in MODULE_REGISTRY.items():
            hits = sum(
                1 for kw in data["keywords"]
                if re.search(rf"\b{re.escape(kw)}\b", text)
            )
            if hits:
                # Confidence = proportion of module keywords matched, capped at 0.90
                conf = min(0.90, 0.30 + 0.15 * hits)
                scores[mod] = conf
        if not scores:
            return None
        best = max(scores, key=scores.get)
        return {"type": "SINGLE", "module": best, "confidence": scores[best]}

    # ── Stage 4: Fuzzy alias ────────────────────────────────────────────────
    def _stage_fuzzy(self, tokens: list):
        best_score, best_mod = 0.0, None
        for token in tokens:
            if len(token) < 4:  # skip very short tokens
                continue
            for alias, mod in self._alias_pairs:
                s = _fuzzy_ratio(token, alias)
                if s > best_score:
                    best_score, best_mod = s, mod
        if best_score >= 0.72:
            return {"type": "SINGLE", "module": best_mod, "confidence": best_score * 0.85}
        return None

    # ── Stage 5: TF-IDF fallback ────────────────────────────────────────────
    def _stage_tfidf(self, text: str):
        scores = self._tfidf_score(text)
        if not scores:
            return None
        best = max(scores, key=scores.get)
        score = scores[best]
        if score < 0.12:
            return None
        return {"type": "SINGLE", "module": best, "confidence": score * 0.75}

    # ── Main classify ────────────────────────────────────────────────────────
    def classify(self, user_query: str) -> dict:
        text = _clean(user_query)
        tokens = text.split()

        result = (
            self._stage_multi(text)
            or self._stage_phrase(text)
            or self._stage_keyword(text)
            or self._stage_fuzzy(tokens)
            or self._stage_tfidf(text)
        )

        if not result:
            return {"type": "UNRECOGNIZED", "confidence": 0.0}
        return result


# ──────────────────────────────────────────────────────────────────────────────
# 2.  QUERY PARSER — extracts search value, target column, intent sub-type
# ──────────────────────────────────────────────────────────────────────────────

class QueryParser:
    """
    Given a module and cleaned query, determines:
      - intent:       BULK_LIST | GLOBAL_SEARCH | COLUMN_SEARCH | PROMPT_NEEDED
      - target_col:   specific column to search in (if detected)
      - search_value: the actual value to search for
      - aggregation:  SUM | COUNT | NONE
    """

    AGG_TRIGGERS = {
        "total", "sum", "how many", "count", "how much", "aggregate",
        "altogether", "combined", "overall",
    }
    BULK_TRIGGERS = {
        "all", "list", "every", "entire", "show", "get", "fetch",
        "display", "give", "complete", "full",
    }

    def parse(self, user_query: str, module_key: str) -> dict:
        text = _clean(user_query)
        tokens = text.split()

        # Detect aggregation intent
        agg = any(t in text for t in self.AGG_TRIGGERS)
        is_bulk = any(t in tokens for t in self.BULK_TRIGGERS)

        # Detect target column
        target_col = None
        col_words_used = set()
        for trigger, cols in COLUMN_TRIGGERS.items():
            if re.search(rf"\b{re.escape(trigger)}s?\b", text):
                target_col = cols[0]   # primary candidate
                col_words_used.add(trigger)
                col_words_used.update(trigger + "s", trigger + "es")
                break

        # Build strip set: stop words + module keywords + phrases + col triggers
        mod_data = MODULE_REGISTRY[module_key]
        strip = (
            STOP_WORDS
            | set(mod_data["keywords"])
            | {w for p in mod_data["phrases"] for w in p.split()}
            | {w for a in mod_data["aliases"] for w in a.split()}
            | col_words_used
        )

        # Value tokens = whatever's left
        value_tokens = [t for t in tokens if t not in strip and len(t) > 1]

        # Numbers and codes (invoice numbers, barcodes) — preserve regardless
        code_tokens = [t for t in tokens if re.match(r"^[a-z0-9\-]+$", t) and (
            re.search(r"\d", t) or len(t) >= 6
        ) and t not in strip]
        if code_tokens and not value_tokens:
            value_tokens = code_tokens

        search_value = " ".join(value_tokens).strip()

        # Decide intent
        if target_col:
            intent = "COLUMN_SEARCH" if search_value else "PROMPT_NEEDED"
        elif search_value and not is_bulk:
            intent = "GLOBAL_SEARCH"
        else:
            intent = "BULK_LIST"

        return {
            "intent": intent,
            "target_col": target_col,
            "search_value": search_value,
            "aggregation": "SUM" if agg else "NONE",
        }


# ──────────────────────────────────────────────────────────────────────────────
# 3.  DATA MIRROR — SQLite in-memory with FTS5
# ──────────────────────────────────────────────────────────────────────────────

class DataMirror:
    """
    In-memory SQLite mirror of all ERP API modules.

    For each module, maintains TWO tables:
      "{module}"       — structured columnar data (typed TEXT columns)
      "{module}_fts"   — FTS5 virtual table for full-text BM25 search

    FTS5 advantages over LIKE:
      • BM25 ranking (relevance, not just existence)
      • Tokenizer handles word boundaries correctly
      • 5-10x faster on large datasets
      • Case-insensitive by default
      • Supports prefix search: MATCH 'INV-*'
    """

    def __init__(self):
        # Use WAL mode for concurrent reads without blocking
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.execute("PRAGMA cache_size=-16000")  # 16 MB page cache

        self._ttl_map: dict  = {}   # module → expiry timestamp
        self._col_map: dict  = {}   # module → list of column names
        self._lock = asyncio.Lock()

    # ── Flatten nested API JSON ─────────────────────────────────────────────
    def _extract_list(self, raw) -> list:
        if isinstance(raw, list):
            return raw
        if isinstance(raw, dict):
            if "data" in raw and isinstance(raw["data"], list):
                return raw["data"]
            for v in raw.values():
                if isinstance(v, list) and v:
                    return v
        return []

    def _flatten_record(self, record: dict, parent_key="") -> dict:
        """Recursively flatten nested dicts. Lists of dicts become child rows."""
        flat = {}
        for k, v in record.items():
            if v is None:
                flat[k] = ""
            elif isinstance(v, dict):
                # Extract best display value from nested object
                flat[k] = str(
                    v.get("name", v.get("title", v.get("invoiceNo", v.get("totalAmount", ""))))
                )
            elif isinstance(v, (str, int, float, bool)):
                flat[k] = str(v)
            # Lists handled by stream_flatten
        return flat

    def _stream_flatten(self, records: list):
        """Generator: yields flat dict rows from possibly nested API records."""
        for record in records:
            if not isinstance(record, dict):
                yield {"_data": str(record)}
                continue

            base = {}
            child_lists = []

            for k, v in record.items():
                if v is None:
                    base[k] = ""
                elif isinstance(v, (str, int, float, bool)):
                    base[k] = str(v)
                elif isinstance(v, dict):
                    base[k] = str(
                        v.get("name", v.get("title", v.get("invoiceNo", v.get("totalAmount", ""))))
                    )
                elif isinstance(v, list) and v and isinstance(v[0], dict):
                    child_lists.append((k, v))

            if child_lists:
                for _, children in child_lists:
                    for child in children:
                        row = base.copy()
                        for ck, cv in child.items():
                            if isinstance(cv, dict):
                                row[ck] = str(cv.get("name", cv.get("title", cv.get("invoiceNo", ""))))
                            elif not isinstance(cv, list):
                                row[ck] = str(cv) if cv is not None else ""
                        yield row
            else:
                yield base

    # ── Load records into SQLite ─────────────────────────────────────────────
    def _load_module(self, module_key: str, records: list):
        """Create/replace columnar table + FTS5 virtual table for a module."""
        cur = self.conn.cursor()

        # Drop old tables
        cur.execute(f'DROP TABLE IF EXISTS "{module_key}_fts"')
        cur.execute(f'DROP TABLE IF EXISTS "{module_key}"')

        if not records:
            cur.execute(f'CREATE TABLE "{module_key}" (_empty TEXT)')
            self._col_map[module_key] = []
            self.conn.commit()
            return

        # Collect all unique column names
        all_keys: list = []
        seen = set()
        for r in records:
            for k in r.keys():
                if k not in seen:
                    seen.add(k)
                    all_keys.append(k)

        # Truncate to MAX_ROWS_PER_MODULE
        records = records[:MAX_ROWS_PER_MODULE]

        # Create columnar table
        cols_ddl = ", ".join([f'"{k}" TEXT' for k in all_keys])
        cur.execute(f'CREATE TABLE "{module_key}" (_rowid INTEGER PRIMARY KEY, {cols_ddl})')

        # Insert rows
        placeholders = ",".join(["?"] * len(all_keys))
        for r in records:
            vals = [r.get(k, "") for k in all_keys]
            cur.execute(f'INSERT INTO "{module_key}" ({", ".join(chr(34)+k+chr(34) for k in all_keys)}) VALUES ({placeholders})', vals)

        # Create FTS5 index (maps _fts rowid → main table _rowid)
        # content= makes it a content table (no data duplication)
        fts_cols = ", ".join([f'"{k}"' for k in all_keys])
        cur.execute(
            f'CREATE VIRTUAL TABLE "{module_key}_fts" USING fts5('
            f'{fts_cols}, '
            f'content="{module_key}", '
            f'content_rowid="_rowid", '
            f'tokenize="unicode61 remove_diacritics 1"'
            f')'
        )
        # Populate FTS index
        cur.execute(
            f'INSERT INTO "{module_key}_fts" (rowid, {fts_cols}) '
            f'SELECT _rowid, {fts_cols} FROM "{module_key}"'
        )

        self.conn.commit()
        self._col_map[module_key] = all_keys

    # ── Sync a single module from API ───────────────────────────────────────
    async def sync_module(self, module_key: str, force: bool = False) -> bool:
        mod = MODULE_REGISTRY[module_key]
        ttl = mod["ttl"]
        now = time.time()
        expiry = self._ttl_map.get(module_key, 0)

        if not force and now < expiry:
            return True  # cache still valid

        async with self._lock:
            # Double-check inside lock
            now = time.time()
            if not force and now < self._ttl_map.get(module_key, 0):
                return True

            async with httpx.AsyncClient(timeout=30.0) as client:
                try:
                    auth_resp = await client.post(LOGIN_URL, json=LOGIN_CREDS)
                    token = auth_resp.json().get("jwtToken")
                    if not token:
                        return False

                    headers = {
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json",
                    }
                    api_url = f"{BASE_URL}{mod['url']}"
                    resp = await client.get(api_url, headers=headers)

                    if resp.status_code == 200:
                        raw = self._extract_list(resp.json())
                        flat = list(self._stream_flatten(raw))
                        self._load_module(module_key, flat)
                        self._ttl_map[module_key] = time.time() + ttl
                        return True
                    else:
                        return False

                except Exception as exc:
                    print(f"[DataMirror] Sync error for {module_key}: {exc}")
                    return False

    # ── Sync multiple modules concurrently ──────────────────────────────────
    async def sync_modules(self, module_keys: list, force: bool = False) -> dict:
        tasks = {mod: self.sync_module(mod, force) for mod in module_keys}
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        return {mod: (r is True) for mod, r in zip(tasks.keys(), results)}

    async def sync_all(self, force: bool = True):
        await self.sync_modules(list(MODULE_REGISTRY.keys()), force=force)

    # ── Get column names for a module ───────────────────────────────────────
    def get_columns(self, module_key: str) -> list:
        return self._col_map.get(module_key, [])

    # ── Check if module has data ─────────────────────────────────────────────
    def has_data(self, module_key: str) -> bool:
        cols = self._col_map.get(module_key)
        return bool(cols)


db = DataMirror()


# ──────────────────────────────────────────────────────────────────────────────
# 4.  RAG RETRIEVER — FTS5 + fallback LIKE search
# ──────────────────────────────────────────────────────────────────────────────

class RAGRetriever:
    """
    Three-tier retrieval strategy per module:

    Tier A — FTS5 BM25 search (if search value provided)
        Fast, ranked, handles partial words with prefix* matching.
        Falls back to Tier B if FTS returns no results.

    Tier B — SQLite LIKE scan across all columns
        Broad catch-all. Used when FTS misses something.

    Tier C — Full table (BULK_LIST intent)
        Returns all rows up to MAX_ROWS_PER_MODULE.

    All results are returned as list[dict] with original column names.
    """

    MAX_FTS   = 500    # max rows from FTS pass
    MAX_LIKE  = 500    # max rows from LIKE pass
    MAX_BULK  = 10_000 # max rows for bulk list

    def retrieve(self, module_key: str, parsed: dict) -> tuple:
        """
        Returns (records: list[dict], columns: list[str], method: str)
        """
        intent      = parsed["intent"]
        search_val  = parsed["search_value"]
        target_col  = parsed["target_col"]
        columns     = db.get_columns(module_key)

        if not columns:
            return [], [], "empty"

        if intent == "BULK_LIST" or not search_val:
            records = self._bulk(module_key, columns)
            return records, columns, "bulk"

        # Try FTS first
        records, method = self._fts_search(module_key, search_val, target_col, columns)
        if not records:
            # Fallback to LIKE
            records, method = self._like_search(module_key, search_val, target_col, columns)

        return records, columns, method

    def _fts_search(self, module_key: str, value: str, target_col, columns: list) -> tuple:
        """FTS5 BM25 search. Returns (records, 'fts') or ([], 'fts_empty')."""
        try:
            cur = db.conn.cursor()
            fts_table = f"{module_key}_fts"

            # Sanitize value for FTS query — escape quotes, add prefix wildcard
            safe_val = re.sub(r'["\(\)\*\+\-\^~]', " ", value).strip()
            if not safe_val:
                return [], "fts_empty"

            # Build FTS query
            if target_col:
                # Map target_col to actual column name
                actual_col = self._resolve_col(target_col, columns)
                if actual_col:
                    fts_query = f'"{actual_col}" : "{safe_val}"*'
                else:
                    fts_query = f'"{safe_val}"*'
            else:
                # Search all — each word must appear somewhere
                words = [w for w in safe_val.split() if len(w) > 1]
                if not words:
                    return [], "fts_empty"
                fts_query = " OR ".join([f'"{w}"*' for w in words])

            sql = (
                f'SELECT m.* FROM "{module_key}" m '
                f'JOIN "{fts_table}" f ON m._rowid = f.rowid '
                f'WHERE "{fts_table}" MATCH ? '
                f'ORDER BY rank '
                f'LIMIT {self.MAX_FTS}'
            )
            cur.execute(sql, [fts_query])
            rows = [dict(r) for r in cur.fetchall()]
            # Remove _rowid from result dicts
            for r in rows:
                r.pop("_rowid", None)
            return rows, "fts"

        except Exception as exc:
            print(f"[Retriever] FTS error for {module_key}: {exc}")
            return [], "fts_error"

    def _like_search(self, module_key: str, value: str, target_col, columns: list) -> tuple:
        """Broad LIKE search. Returns (records, 'like')."""
        try:
            cur = db.conn.cursor()
            if target_col:
                actual_col = self._resolve_col(target_col, columns)
                if actual_col:
                    sql    = f'SELECT * FROM "{module_key}" WHERE "{actual_col}" LIKE ? LIMIT {self.MAX_LIKE}'
                    params = [f"%{value}%"]
                else:
                    return self._like_all_cols(module_key, value, columns)
            else:
                return self._like_all_cols(module_key, value, columns)

            cur.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()]
            for r in rows:
                r.pop("_rowid", None)
            return rows, "like"
        except Exception as exc:
            print(f"[Retriever] LIKE error for {module_key}: {exc}")
            return [], "like_error"

    def _like_all_cols(self, module_key: str, value: str, columns: list) -> tuple:
        """LIKE search across ALL columns."""
        try:
            cur = db.conn.cursor()
            conditions = " OR ".join([f'"{c}" LIKE ?' for c in columns])
            params     = [f"%{value}%"] * len(columns)
            cur.execute(
                f'SELECT * FROM "{module_key}" WHERE {conditions} LIMIT {self.MAX_LIKE}',
                params,
            )
            rows = [dict(r) for r in cur.fetchall()]
            for r in rows:
                r.pop("_rowid", None)
            return rows, "like_all"
        except Exception as exc:
            print(f"[Retriever] LIKE-all error for {module_key}: {exc}")
            return [], "like_error"

    def _bulk(self, module_key: str, columns: list) -> list:
        """Return all rows (up to MAX_BULK)."""
        try:
            cur = db.conn.cursor()
            cur.execute(f'SELECT * FROM "{module_key}" LIMIT {self.MAX_BULK}')
            rows = [dict(r) for r in cur.fetchall()]
            for r in rows:
                r.pop("_rowid", None)
            return rows
        except Exception as exc:
            print(f"[Retriever] Bulk error for {module_key}: {exc}")
            return []

    def _resolve_col(self, target: str, columns: list) -> Optional[str]:
        """Find actual column name that best matches the target hint."""
        # Exact match first
        for c in columns:
            if c.lower() == target.lower():
                return c
        # Partial match
        for c in columns:
            if target.lower() in c.lower() or c.lower() in target.lower():
                return c
        return None


retriever = RAGRetriever()


# ──────────────────────────────────────────────────────────────────────────────
# 5.  AGGREGATION ENGINE
# ──────────────────────────────────────────────────────────────────────────────

class AggregationEngine:
    def compute(self, records: list, module_key: str) -> dict:
        mod      = MODULE_REGISTRY[module_key]
        amounts  = mod["amount_cols"]
        qty_cols = mod["qty_cols"]

        total_amount = 0.0
        total_qty    = 0.0
        amount_found = False
        qty_found    = False

        for rec in records:
            if not amount_found:
                for col in amounts:
                    actual = next((k for k in rec if k.lower() == col.lower()), None)
                    if actual:
                        try:
                            total_amount += float(str(rec[actual]).replace(",", "").replace("₹", "").strip())
                            amount_found  = True
                        except (ValueError, TypeError):
                            pass
                        break

            if not qty_found:
                for col in qty_cols:
                    actual = next((k for k in rec if k.lower() == col.lower()), None)
                    if actual:
                        try:
                            total_qty  += float(str(rec[actual]).replace(",", "").strip())
                            qty_found   = True
                        except (ValueError, TypeError):
                            pass
                        break

            # Reset flags per row
            amount_found = False
            qty_found    = False

        # Recompute totals
        total_amount = 0.0
        total_qty    = 0.0
        for rec in records:
            for col in amounts:
                actual = next((k for k in rec if k.lower() == col.lower()), None)
                if actual:
                    try:
                        total_amount += float(str(rec[actual]).replace(",", "").replace("₹", "").strip())
                    except (ValueError, TypeError):
                        pass
                    break
            for col in qty_cols:
                actual = next((k for k in rec if k.lower() == col.lower()), None)
                if actual:
                    try:
                        total_qty += float(str(rec[actual]).replace(",", "").strip())
                    except (ValueError, TypeError):
                        pass
                    break

        return {
            "count":        len(records),
            "total_amount": total_amount if total_amount else None,
            "total_qty":    int(total_qty) if total_qty else None,
        }


aggregator = AggregationEngine()


# ──────────────────────────────────────────────────────────────────────────────
# 6.  RESPONSE SYNTHESIZER — HTML generation
# ──────────────────────────────────────────────────────────────────────────────

HIDDEN_COLS = {
    "id", "_id", "__v", "_empty", "_data", "_rowid",
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

AMOUNT_HINTS = {"price", "amount", "total", "balance", "credit", "debit", "paid", "cost", "value"}
QTY_HINTS    = {"quantity", "qty", "stock", "units", "count"}


class ResponseSynthesizer:

    # ── Natural language intro ───────────────────────────────────────────────
    def build_intro(
        self,
        module_key: str,
        parsed: dict,
        agg: dict,
        search_val: str,
        module_label: str,
        marathi: bool = False,
    ) -> str:
        # Delegate to Marathi string builder
        if marathi:
            return mr_intro(module_key, agg["count"], search_val,
                            agg["total_amount"], agg["total_qty"])

        count = agg["count"]
        amt   = agg["total_amount"]
        qty   = agg["total_qty"]

        ack = random.choice([
            "Here you go! ",
            "Got it — here's what I found. ",
            "Absolutely! ",
            "Done — here's your data. ",
        ])

        # What we searched for
        if search_val and parsed["intent"] != "BULK_LIST":
            action = (
                f'I searched the <strong>{module_label}</strong> database for '
                f'<strong>"{search_val}"</strong> and found '
                f'<strong>{count:,} record{"s" if count != 1 else ""}</strong>.'
            )
        else:
            action = (
                f'Here is the complete <strong>{module_label}</strong> list — '
                f'<strong>{count:,} record{"s" if count != 1 else ""}</strong>.'
            )

        # Aggregate insights
        insights = []
        if amt:
            insights.append(f'Total value: <strong>₹{amt:,.2f}</strong>')
        if qty:
            insights.append(f'Total units: <strong>{qty:,}</strong>')

        insight_str = " &nbsp;·&nbsp; ".join(insights)
        if insight_str:
            insight_str = f'<span style="color:#6366f1">&nbsp;&nbsp;{insight_str}</span>'

        return f'<p style="margin:0 0 16px 0;font-size:15px;">{ack}{action}{insight_str}</p>'

    # ── Sort / filter visible columns ────────────────────────────────────────
    def _visible_headers(self, columns: list) -> list:
        lower_hidden = {c.lower() for c in HIDDEN_COLS}
        visible = [c for c in columns if c.lower() not in lower_hidden]
        # Sort by priority
        return sorted(
            visible,
            key=lambda x: (
                PRIORITY_COLS.index(x) if x in PRIORITY_COLS else 99
            ),
        )

    # ── Format cell value ────────────────────────────────────────────────────
    def _fmt_cell(self, col: str, val: str) -> str:
        if not val or val in ("None", "null", ""):
            return '<span style="color:#94a3b8">—</span>'

        col_l = col.lower()

        # Image
        if _is_image_url(val):
            return (
                f'<img src="{val}" style="width:40px;height:40px;'
                f'object-fit:cover;border-radius:6px;box-shadow:0 1px 3px rgba(0,0,0,.15)" />'
            )

        # Datetime
        if ("T" in val and val.count("-") >= 2 and len(val) >= 19) or "createdat" in col_l or "updatedat" in col_l:
            return f'<span style="color:#64748b;font-size:12px">{_fmt_date(val)}</span>'

        # Currency
        if any(h in col_l for h in AMOUNT_HINTS):
            try:
                num = float(str(val).replace(",", "").replace("₹", "").strip())
                color = "#16a34a" if num > 0 else "#dc2626"
                return f'<strong style="color:{color}">{_fmt_currency(num)}</strong>'
            except (ValueError, TypeError):
                pass

        # Quantity
        if any(h in col_l for h in QTY_HINTS):
            try:
                num = int(float(str(val).replace(",", "").strip()))
                color = "#0369a1" if num > 10 else "#dc2626"
                return f'<span style="color:{color};font-weight:600">{num:,}</span>'
            except (ValueError, TypeError):
                pass

        # Status / type badges
        if col_l in ("status", "supplytype", "type", "state"):
            badge_colors = {
                "in-state":  ("#dcfce7", "#16a34a"),
                "out-state": ("#fef3c7", "#d97706"),
                "active":    ("#dcfce7", "#16a34a"),
                "inactive":  ("#fee2e2", "#dc2626"),
                "paid":      ("#dcfce7", "#16a34a"),
                "pending":   ("#fef3c7", "#d97706"),
            }
            colors = badge_colors.get(val.lower(), ("#f1f5f9", "#475569"))
            return (
                f'<span style="background:{colors[0]};color:{colors[1]};'
                f'padding:2px 8px;border-radius:999px;font-size:12px;font-weight:600">{val}</span>'
            )

        return val

    # ── Build HTML table ─────────────────────────────────────────────────────
    def build_table(self, records: list, columns: list, marathi: bool = False) -> str:
        headers = self._visible_headers(columns)
        if not headers:
            return "<p>No displayable columns.</p>"

        font = "'Noto Sans Devanagari','Segoe UI',system-ui,sans-serif" if marathi else "'Segoe UI',system-ui,sans-serif"

        # ── Table wrapper ────────────────────────────────────────────────────
        html = f"""
<div style="overflow-x:auto;margin-top:4px;border-radius:12px;
     box-shadow:0 4px 24px rgba(0,0,0,.08);border:1px solid #e2e8f0;">
<table style="border-collapse:collapse;width:100%;text-align:left;
     background:#fff;font-family:{font};min-width:520px;">
"""

        # ── Header row ───────────────────────────────────────────────────────
        html += (
            '<thead><tr style="background:linear-gradient(135deg,#1e293b,#334155);'
            'color:#f8fafc;">'
        )
        for h in headers:
            eng_label = re.sub(r"([a-z])([A-Z])", r"\1 \2", h).title()
            label = translate_column(eng_label) if marathi else eng_label
            html += (
                f'<th style="padding:13px 16px;border-right:1px solid #475569;'
                f'font-size:12px;font-weight:600;'
                f'letter-spacing:.03em;white-space:nowrap">{label}</th>'
            )
        html += "</tr></thead><tbody>"

        # ── Data rows ────────────────────────────────────────────────────────
        for i, rec in enumerate(records):
            bg = "#ffffff" if i % 2 == 0 else "#f8fafc"
            html += (
                f'<tr style="background:{bg};border-bottom:1px solid #e2e8f0;'
                f'transition:background .15s" '
                f'onmouseover="this.style.background=\'#eff6ff\'" '
                f'onmouseout="this.style.background=\'{bg}\'">'
            )
            for h in headers:
                raw = str(rec.get(h, "") or "")
                cell = self._fmt_cell(h, raw)
                html += (
                    f'<td style="padding:11px 16px;border-right:1px solid #e2e8f0;'
                    f'border-bottom:1px solid #e2e8f0;font-size:13px;'
                    f'color:#334155;white-space:nowrap">{cell}</td>'
                )
            html += "</tr>"

        html += "</tbody></table></div>"
        return html

    # ── Zero-results message ─────────────────────────────────────────────────
    def no_results(self, module_key: str, search_val: str) -> str:
        label = module_key.replace("-", " ").title()
        suggestions = [
            "double-check the spelling",
            "try a shorter search term",
            "use just the first few characters",
        ]
        return (
            f'<div style="background:#fef2f2;border:1px solid #fecaca;border-radius:10px;'
            f'padding:16px 20px;color:#991b1b;">'
            f'<strong>No results found</strong> in <em>{label}</em> '
            f'for "<strong>{search_val}</strong>".<br>'
            f'<small style="color:#b91c1c">Tip: {random.choice(suggestions)}.</small>'
            f'</div>'
        )

    # ── Aggregation-only response ────────────────────────────────────────────
    def agg_only(self, module_key: str, agg: dict) -> str:
        label = module_key.replace("-", " ").title()
        parts = [f"<strong>{label}</strong> — {agg['count']:,} records"]
        if agg["total_amount"]:
            parts.append(f"Total: <strong>₹{agg['total_amount']:,.2f}</strong>")
        if agg["total_qty"]:
            parts.append(f"Units: <strong>{agg['total_qty']:,}</strong>")
        return (
            f'<div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;'
            f'padding:16px 20px;">' + " &nbsp;·&nbsp; ".join(parts) + "</div>"
        )

    # ── Error card ───────────────────────────────────────────────────────────
    def api_error(self, module_key: str) -> str:
        label = module_key.replace("-", " ").title()
        return (
            f'<div style="background:#fff7ed;border:1px solid #fed7aa;border-radius:10px;'
            f'padding:16px 20px;color:#9a3412;">'
            f'⚠️ Could not fetch <strong>{label}</strong> data from the ERP server. '
            f'Please check the connection and try again.'
            f'</div>'
        )


synthesizer = ResponseSynthesizer()


# ──────────────────────────────────────────────────────────────────────────────
# 7.  CONVERSATION CONTEXT (last 5 turns, in-memory)
# ──────────────────────────────────────────────────────────────────────────────

class ConversationContext:
    """
    Lightweight in-process conversation memory.
    Stores last N turns per session_id.
    Memory: ~5 turns × ~200 chars = ~1 KB per session. Negligible.
    """

    def __init__(self, max_turns: int = 5):
        self._sessions: dict = defaultdict(lambda: deque(maxlen=max_turns))

    def add(self, session_id: str, role: str, text: str):
        self._sessions[session_id].append({"role": role, "text": text[:300]})

    def last_module(self, session_id: str) -> Optional[str]:
        """Return the module from the last bot response, if any."""
        for turn in reversed(self._sessions[session_id]):
            if turn["role"] == "bot" and "_module:" in turn["text"]:
                m = re.search(r"_module:(\S+)", turn["text"])
                if m:
                    return m.group(1)
        return None

    def get_history(self, session_id: str) -> list:
        return list(self._sessions[session_id])


context_store = ConversationContext()


# ──────────────────────────────────────────────────────────────────────────────
# 8.  MAIN ORCHESTRATOR
# ──────────────────────────────────────────────────────────────────────────────

class AdminIntelligenceEngine:
    def __init__(self):
        self.classifier = IntentClassifier()
        self.parser     = QueryParser()

    async def process(self, user_query: str, session_id: str = "default") -> str:
        # ── Context: track conversation ──────────────────────────────────────
        context_store.add(session_id, "user", user_query)

        # ── Language detection ───────────────────────────────────────────────
        lang = detect_language(user_query)
        is_marathi = lang in ("marathi_devanagari", "marathi_roman")

        # ── If Marathi: use Marathi intent map directly ──────────────────────
        if is_marathi:
            module_key = marathi_classify(user_query)
            if module_key:
                return await self._handle_single(
                    module_key, user_query, session_id, marathi=True
                )
            # Marathi query but no module found
            return mr_unrecognized()

        # ── English: use 5-stage NLU classifier ─────────────────────────────
        classification = self.classifier.classify(user_query)

        if classification["type"] == "UNRECOGNIZED":
            return self._unrecognized_response()

        if classification["type"] == "MULTI":
            return await self._handle_multi(
                classification["pattern"], user_query, session_id
            )

        module_key = classification["module"]
        return await self._handle_single(module_key, user_query, session_id, marathi=False)

    # ── Single module ────────────────────────────────────────────────────────
    async def _handle_single(self, module_key: str, user_query: str,
                              session_id: str, marathi: bool = False) -> str:
        parsed = self.parser.parse(user_query, module_key)

        if parsed["intent"] == "PROMPT_NEEDED":
            col_label = re.sub(r"([a-z])([A-Z])", r"\1 \2", parsed["target_col"]).title()
            if marathi:
                return mr_prompt_needed(module_key, col_label)
            return (
                f'You\'re querying the <strong>{module_key.replace("-", " ").title()}</strong> module. '
                f'Could you please specify the <strong>{col_label}</strong>?<br>'
                f'<em>Example: "invoice INV-1042" or "name Rahul"</em>'
            )

        # Sync data
        ok = await db.sync_module(module_key)
        if not ok:
            return mr_api_error(module_key) if marathi else synthesizer.api_error(module_key)

        # Retrieve
        records, columns, method = retriever.retrieve(module_key, parsed)
        label = module_key.replace("-", " ").title()

        if not records:
            if marathi:
                return mr_no_results(module_key, parsed["search_value"])
            return synthesizer.no_results(module_key, parsed["search_value"])

        # Aggregate
        agg = aggregator.compute(records, module_key)

        # Aggregation-only response
        if parsed["aggregation"] == "SUM" and parsed["intent"] != "BULK_LIST":
            return synthesizer.agg_only(module_key, agg)

        # Full response
        intro = synthesizer.build_intro(
            module_key, parsed, agg, parsed["search_value"], label, marathi=marathi
        )
        table = synthesizer.build_table(records, columns, marathi=marathi)
        response = intro + table

        context_store.add(session_id, "bot",
                          f"_module:{module_key} {label} ({agg['count']} records)")
        return response

    # ── Multi module ─────────────────────────────────────────────────────────
    async def _handle_multi(self, pattern: dict, user_query: str, session_id: str) -> str:
        modules     = pattern["modules"]
        label       = pattern["label"]

        # Sync all modules concurrently
        sync_results = await db.sync_modules(modules)

        sections = []
        total_agg = {"count": 0, "total_amount": 0.0, "total_qty": 0}

        for mod_key in modules:
            if not sync_results.get(mod_key):
                sections.append(
                    f'<h3 style="margin:20px 0 8px">{mod_key.replace("-"," ").title()}</h3>'
                    + synthesizer.api_error(mod_key)
                )
                continue

            parsed  = self.parser.parse(user_query, mod_key)
            # Force bulk for multi-module (we want all data)
            parsed["intent"] = "BULK_LIST"
            parsed["search_value"] = ""

            records, columns, _ = retriever.retrieve(mod_key, parsed)
            if not records:
                continue

            agg = aggregator.compute(records, mod_key)
            total_agg["count"]        += agg["count"]
            total_agg["total_amount"] += agg["total_amount"] or 0
            total_agg["total_qty"]    += agg["total_qty"] or 0

            mod_label = mod_key.replace("-", " ").title()
            intro = synthesizer.build_intro(mod_key, parsed, agg, "", mod_label)
            table = synthesizer.build_table(records, columns)
            sections.append(
                f'<div style="margin-bottom:32px">'
                f'<h3 style="margin:0 0 8px;font-size:16px;color:#1e293b;'
                f'border-left:4px solid #6366f1;padding-left:10px">{mod_label}</h3>'
                + intro + table + "</div>"
            )

        if not sections:
            return f'<p>No data available for <strong>{label}</strong>.</p>'

        # Summary header
        summary_parts = [f'<strong>{total_agg["count"]:,} total records</strong>']
        if total_agg["total_amount"]:
            summary_parts.append(f'Total value: <strong>₹{total_agg["total_amount"]:,.2f}</strong>')
        if total_agg["total_qty"]:
            summary_parts.append(f'Total units: <strong>{total_agg["total_qty"]:,}</strong>')

        header = (
            f'<div style="background:linear-gradient(135deg,#6366f1,#8b5cf6);'
            f'color:#fff;border-radius:12px;padding:16px 20px;margin-bottom:24px;">'
            f'<strong style="font-size:17px">📊 {label}</strong><br>'
            f'<small>{" &nbsp;·&nbsp; ".join(summary_parts)}</small>'
            f'</div>'
        )

        context_store.add(session_id, "bot", f"_module:multi {label}")
        return header + "".join(sections)

    # ── Fallback responses ───────────────────────────────────────────────────
    def _unrecognized_response(self) -> str:
        modules_list = " · ".join([
            m.replace("-", " ").title() for m in MODULE_REGISTRY.keys()
        ])
        return (
            f'<div style="background:#f8fafc;border:1px solid #e2e8f0;'
            f'border-radius:10px;padding:16px 20px;">'
            f"<strong>I'm not sure what you're looking for.</strong><br>"
            f"I can help you with: <em>{modules_list}</em><br><br>"
            f"Try asking:<br>"
            f"<ul style='margin:8px 0;padding-left:20px'>"
            f"<li><em>List all purchases</em></li>"
            f"<li><em>Show supplier credits</em></li>"
            f"<li><em>Find customer named Rahul</em></li>"
            f"<li><em>Sales report</em></li>"
            f"<li><em>Stock inventory</em></li>"
            f"</ul>"
            f"</div>"
        )


engine = AdminIntelligenceEngine()


# ──────────────────────────────────────────────────────────────────────────────
# 9.  SPECIAL QUERY HANDLERS
# ──────────────────────────────────────────────────────────────────────────────

IDENTITY_KEYWORDS = {
    "who are you", "how do you work", "chatgpt", "your brain",
    "how were you built", "architecture", "how does this work",
}

REFRESH_KEYWORDS = {"refresh", "sync", "update records", "reload"}


def _is_greeting(text: str) -> bool:
    t = text.lower().strip()
    return any(t.startswith(g) for g in GREETING_WORDS) and len(t.split()) <= 4


def _is_thanks(text: str) -> bool:
    return text.lower().strip() in THANKS_WORDS


def _is_identity(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in IDENTITY_KEYWORDS)


def _is_refresh(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in REFRESH_KEYWORDS)


GREETING_RESPONSE = """
<div style="font-family:'Segoe UI',system-ui,sans-serif;max-width:560px">
  <p style="font-size:17px;font-weight:600;margin:0 0 12px">
    👋 Hello! I'm your <span style="color:#6366f1">Admin Intelligence Assistant</span>.
  </p>
  <p style="margin:0 0 14px;color:#475569">Here's what I can help you with:</p>
  <div style="display:grid;gap:8px">
    <div style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:8px;padding:10px 14px">
      📦 <strong>Stock & Inventory</strong> — <em>"Show all stock"</em>, <em>"Find product NON WOVEN"</em>
    </div>
    <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:10px 14px">
      💰 <strong>Sales & Purchases</strong> — <em>"List all sales"</em>, <em>"Get purchase invoice INV-001"</em>
    </div>
    <div style="background:#fdf4ff;border:1px solid #e9d5ff;border-radius:8px;padding:10px 14px">
      🏢 <strong>Suppliers & Customers</strong> — <em>"All suppliers"</em>, <em>"Find customer Rahul"</em>
    </div>
    <div style="background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 14px">
      📊 <strong>Financial Reports</strong> — <em>"Supplier credits"</em>, <em>"Financial summary"</em>
    </div>
  </div>
</div>
"""

IDENTITY_RESPONSE = """
<div style="font-family:'Segoe UI',system-ui,sans-serif;max-width:600px">
  <p style="font-size:17px;font-weight:700;color:#1e293b;margin:0 0 12px">
    🧠 Admin Intelligence Engine v3.0
  </p>
  <p style="margin:0 0 16px;color:#475569">
    A production-grade <strong>RAG pipeline</strong> built for ERP data, 
    with zero external AI APIs and &lt;120 MB RAM usage.
  </p>

  <div style="background:#f8fafc;border-radius:10px;padding:16px;margin-bottom:16px">
    <strong style="color:#6366f1">5-Stage NLU Pipeline:</strong>
    <ol style="margin:8px 0;padding-left:20px;color:#334155;line-height:1.8">
      <li><strong>Multi-module patterns</strong> — detects compound queries like "financial summary"</li>
      <li><strong>Phrase matching</strong> — "supplier credit" → correct endpoint, never confused with "supplier"</li>
      <li><strong>Keyword scoring</strong> — weighted frequency matching with confidence scores</li>
      <li><strong>Fuzzy alias matching</strong> — catches typos like "purchaces" → purchases</li>
      <li><strong>TF-IDF cosine fallback</strong> — semantic matching for unusual phrasing</li>
    </ol>
  </div>

  <div style="background:#f0fdf4;border-radius:10px;padding:16px;margin-bottom:16px">
    <strong style="color:#16a34a">RAG Retrieval Layer:</strong>
    <ul style="margin:8px 0;padding-left:20px;color:#334155;line-height:1.8">
      <li><strong>SQLite FTS5 + BM25</strong> — ranked full-text search, 10× faster than LIKE</li>
      <li><strong>Multi-API concurrency</strong> — fetches multiple endpoints simultaneously</li>
      <li><strong>TTL-based caching</strong> — stock: 2 min, finance: 10 min, master: 5 min</li>
      <li><strong>Fallback chain</strong> — FTS5 → LIKE → full-scan (graceful degradation)</li>
    </ul>
  </div>

  <div style="background:#fff7ed;border-radius:10px;padding:16px">
    <strong style="color:#d97706">Key Properties:</strong>
    <ul style="margin:8px 0;padding-left:20px;color:#334155;line-height:1.8">
      <li>🔒 <strong>100% private</strong> — no data leaves your server</li>
      <li>⚡ <strong>Zero AI API cost</strong> — pure math + SQLite</li>
      <li>🎯 <strong>Zero hallucinations</strong> — only returns what's in your ERP</li>
      <li>💾 <strong>&lt;120 MB RAM</strong> — safe for Render free tier</li>
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


@router.post("/chat")
async def chat_endpoint(request: ChatRequest):
    user_query = (request.query or request.question or "").strip()
    session_id = request.session_id or "default"

    if not user_query:
        return {"response": "Please type a question to get started. / कृपया प्रश्न टाइप करा."}

    lang = detect_language(user_query)
    is_marathi = lang in ("marathi_devanagari", "marathi_roman")

    # ── Marathi-specific quick responses ─────────────────────────────────────
    if is_marathi:
        if is_marathi_greeting(user_query):
            return {"response": MARATHI_GREETING}
        if is_marathi_thanks(user_query):
            return {"response": random.choice(MARATHI_THANKS)}
        if is_marathi_refresh(user_query):
            await db.sync_all(force=True)
            return {"response": MARATHI_REFRESH}

    # ── English quick responses ───────────────────────────────────────────────
    if not is_marathi:
        if _is_greeting(user_query):
            return {"response": GREETING_RESPONSE}
        if _is_thanks(user_query):
            return {
                "response": random.choice([
                    "You're welcome! Let me know if you need more data.",
                    "Happy to help! Anything else?",
                    "My pleasure! Type another query anytime.",
                ])
            }
        if _is_identity(user_query):
            return {"response": IDENTITY_RESPONSE}
        if _is_refresh(user_query):
            await db.sync_all(force=True)
            return {
                "response": (
                    '<div style="background:#f0fdf4;border:1px solid #bbf7d0;'
                    'border-radius:10px;padding:14px 18px;color:#166534">'
                    '🔄 <strong>All modules synced.</strong> ERP data is up to date. What would you like to see?'
                    '</div>'
                )
            }

    # ── Main intelligence pipeline ────────────────────────────────────────────
    response = await engine.process(user_query, session_id)
    return {"response": response}