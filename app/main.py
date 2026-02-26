from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

# Import your brilliant new chat engine
from app.routes import chat 

app = FastAPI(title="Maison Luxe AI Concierge")

# Security: Allow frontend to talk to backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Connect the chatbot logic
app.include_router(chat.router)

# Mount the frontend website (assuming your HTML/JS is in a folder named 'static')
app.mount("/", StaticFiles(directory="static", html=True), name="static")