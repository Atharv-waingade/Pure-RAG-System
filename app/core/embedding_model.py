from sentence_transformers import SentenceTransformer
import logging
import os

logger = logging.getLogger(__name__)

class GlobalEmbeddingModel:
    _instance = None
    _model = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        if self._model is not None:
            return
        
        # --- THE FIX IS HERE ---
        # We look for the folder created by build.sh
        cache_path = "./model_cache"
        
        if os.path.exists(cache_path):
            logger.info(f"Loading Global Embedding Model from local cache: {cache_path}...")
            # Load from disk (Fast & Safe)
            self._model = SentenceTransformer(cache_path, device='cpu')
        else:
            logger.warning("Local cache not found! Downloading model (this might be slow)...")
            # Fallback (Slow)
            self._model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2', device='cpu')
            
        logger.info("Model loaded successfully.")

    def encode(self, texts):
        # Normalize embeddings for Cosine Similarity (Inner Product in FAISS)
        return self._model.encode(texts, convert_to_numpy=True, normalize_embeddings=True)

# Global accessor
def get_embedding_model():
    return GlobalEmbeddingModel.get_instance()