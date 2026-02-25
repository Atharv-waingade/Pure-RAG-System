from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import chat, sync, health, admin
from app.core.embedding_model import GlobalEmbeddingModel

# Preload model on startup
# This ensures the heavy AI model is loaded into memory before the first request comes in.
GlobalEmbeddingModel.get_instance()

app = FastAPI(title="Pure RAG SaaS", version="1.0.0")

# --- CORS CONFIGURATION ---
# This is what allows your website to talk to this API.
# Currently set to ["*"] which allows ALL websites.
# For strict production security later, replace ["*"] with specific domains:
# e.g., origins = ["http://localhost:3000", "https://your-real-website.com"]

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],  # Allows all HTTP methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)

# --- REGISTER ROUTES ---
app.include_router(health.router)
app.include_router(chat.router)
app.include_router(sync.router)
app.include_router(admin.router)

if __name__ == "__main__":
    import uvicorn
    # Use host="0.0.0.0" to make it accessible on the local network/internet
    uvicorn.run(app, host="0.0.0.0", port=8000)