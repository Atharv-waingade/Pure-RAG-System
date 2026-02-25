import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import chat, sync, health, admin
from app.core.embedding_model import GlobalEmbeddingModel

# --- BACKGROUND LOADER ---
async def load_model_in_background():
    """Loads the heavy AI model without stopping the server from starting."""
    print("⏳ Triggering background model load...")
    # Run the blocking load function in a separate thread so it doesn't freeze the app
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, GlobalEmbeddingModel.get_instance)
    print("✅ Model loaded in background! Ready for queries.")

# --- LIFESPAN MANAGER ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Start the background task immediately when app starts
    task = asyncio.create_task(load_model_in_background())
    yield
    # (Optional: Clean up resources when app shuts down)

app = FastAPI(title="Pure RAG SaaS", version="1.0.0", lifespan=lifespan)

# --- CORS CONFIGURATION ---
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- REGISTER ROUTES ---
app.include_router(health.router)
app.include_router(chat.router)
app.include_router(sync.router)
app.include_router(admin.router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)