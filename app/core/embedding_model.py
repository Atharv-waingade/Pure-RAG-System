from sentence_transformers import SentenceTransformer
import logging

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
        logger.info("Loading Global Embedding Model (MiniLM-L6-v2)...")
        # CPU only, memory optimized
        self._model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2', device='cpu')
        logger.info("Model loaded successfully.")

    def encode(self, texts):
        # Normalize embeddings for Cosine Similarity (Inner Product in FAISS)
        return self._model.encode(texts, convert_to_numpy=True, normalize_embeddings=True)

# Global accessor
def get_embedding_model():
    return GlobalEmbeddingModel.get_instance()