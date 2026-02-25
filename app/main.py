from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import chat, sync, health, admin
# We import the class, but we DO NOT initialize it yet
from app.core.embedding_model import GlobalEmbeddingModel

app = FastAPI(title="Pure RAG SaaS", version="1.0.0")

# --- CORS CONFIGURATION ---
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- NO LOADING ON STARTUP ---
# We removed the "GlobalEmbeddingModel.get_instance()" line.
# This allows the server to start instantly with very low RAM.
# The model will load automatically the first time someone chats.

# --- REGISTER ROUTES ---
app.include_router(health.router)
app.include_router(chat.router)
app.include_router(sync.router)
app.include_router(admin.router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=10000)