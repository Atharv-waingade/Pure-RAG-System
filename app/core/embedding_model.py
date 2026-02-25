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
        
        # We look for the folder created by build.sh
        cache_path = "./model_cache"
        
        if os.path.exists(cache_path) and os.path.exists(os.path.join(cache_path, "config.json")):
            print(f"✅ Loading model from local folder: {cache_path}...")
            self._model = SentenceTransformer(cache_path, device='cpu')
        else:
            print("⚠️ Local model not found! Downloading from internet (Slow)...")
            self._model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2', device='cpu')

    def encode(self, texts):
        return self._model.encode(texts, convert_to_numpy=True, normalize_embeddings=True)

def get_embedding_model():
    return GlobalEmbeddingModel.get_instance()