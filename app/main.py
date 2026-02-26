import os
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

# Import your chatbot logic
from app.routes import chat 

app = FastAPI(title="Maison Luxe AI Concierge")

# Security
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Connect the chatbot route
app.include_router(chat.router)

# --- BULLETPROOF STATIC FILES ---
# Dynamically find the absolute path to the static folder
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATIC_DIR = os.path.join(BASE_DIR, "static")

# Only try to mount the frontend if the folder actually exists
if os.path.exists(STATIC_DIR):
    app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
else:
    print(f"⚠️ WARNING: No 'static' folder found at {STATIC_DIR}. API is running, but frontend is not mounted.")