import os
import json
import pickle
import hashlib
import faiss
import numpy as np
import logging

logger = logging.getLogger(__name__)

CLIENTS_DIR = "clients"

class ClientStorage:
    def __init__(self, client_id: str):
        self.client_id = client_id
        self.client_dir = os.path.join(CLIENTS_DIR, client_id)
        os.makedirs(self.client_dir, exist_ok=True)

        self.hashes_file = os.path.join(self.client_dir, "page_hashes.json")
        self.data_file = os.path.join(self.client_dir, "data_store.pkl")
        self.index_file = os.path.join(self.client_dir, "faiss.index")

    def load_state(self):
        # Load Hashes
        if os.path.exists(self.hashes_file):
            with open(self.hashes_file, 'r') as f:
                hashes = json.load(f)
        else:
            hashes = {}

        # Load Data Store (URL -> {hash, chunks, embeddings})
        if os.path.exists(self.data_file):
            with open(self.data_file, 'rb') as f:
                data_store = pickle.load(f)
        else:
            data_store = {}

        return hashes, data_store

    def save_state(self, hashes, data_store):
        with open(self.hashes_file, 'w') as f:
            json.dump(hashes, f)

        with open(self.data_file, 'wb') as f:
            pickle.dump(data_store, f)

    def save_index(self, index):
        faiss.write_index(index, self.index_file)

    def load_index(self):
        if os.path.exists(self.index_file):
            return faiss.read_index(self.index_file)
        return None

    @staticmethod
    def compute_hash(content: str) -> str:
        return hashlib.sha256(content.encode('utf-8')).hexdigest()