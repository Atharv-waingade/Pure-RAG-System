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
        
        # Look for the LITE model in the cache
        cache_path = "./model_cache"
        
        if os.path.exists(cache_path) and os.path.exists(os.path.join(cache_path, "config.json")):
            print(f"✅ Loading Lite model from disk: {cache_path}...")
            self._model = SentenceTransformer(cache_path, device='cpu')
        else:
            print("⚠️ Cache missing! Downloading Lite model (Fallback)...")
            self._model = SentenceTransformer('sentence-transformers/paraphrase-MiniLM-L3-v2', device='cpu')

    def encode(self, texts):
        return self._model.encode(texts, convert_to_numpy=True, normalize_embeddings=True)

def get_embedding_model():
    return GlobalEmbeddingModel.get_instance()