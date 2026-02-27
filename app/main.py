import os
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

# Import your ERP AI Agent logic
from app.routes import chat 

# Updated title for your Enterprise Dashboard
app = FastAPI(title="Umbrella AI")

# Security & CORS 
# (This ensures your admin dashboard frontend can talk to this backend safely)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Connect the AI Agent route
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