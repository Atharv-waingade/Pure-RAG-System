from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import chat, sync, health, admin
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

# --- LOAD MODEL ON STARTUP (The Fast Way) ---
# Since build.sh already downloaded the model to './model_cache',
# this line will now take 0.5 seconds to run.
# It ensures the model is ready BEFORE the first user asks a question.
print("⚡ Booting up AI Brain from local cache...")
GlobalEmbeddingModel.get_instance()
print("✅ AI Brain active!")

# --- REGISTER ROUTES ---
app.include_router(health.router)
app.include_router(chat.router)
app.include_router(sync.router)
app.include_router(admin.router)

if __name__ == "__main__":
    import uvicorn
    # Render overrides the port, but this is good for local testing
    uvicorn.run(app, host="0.0.0.0", port=10000)