import os
import faiss
import numpy as np
from app.core.embedding_model import get_embedding_model
from app.core.storage_manager import ClientStorage
from app.core.chunker import chunk_text

class RetrieverEngine:
    def __init__(self, client_id: str):
        self.storage = ClientStorage(client_id)
        self.model = get_embedding_model()

    def sync_content(self, pages: list):
        current_hashes, data_store = self.storage.load_state()

        updated_count = 0
        new_data_store = {} # We rebuild data_store to handle deletions implicitly

        all_chunks = []
        all_embeddings = []

        # Process Input Pages
        for page in pages:
            url = page['url']
            content = page['content']
            new_hash = self.storage.compute_hash(content)

            # Check if unchanged
            if url in current_hashes and current_hashes[url] == new_hash and url in data_store:
                # REUSE existing
                page_data = data_store[url]
                new_data_store[url] = page_data

                # Collect for Index
                for chunk in page_data['chunks']:
                    all_chunks.append(chunk)
                all_embeddings.append(page_data['embeddings'])

            else:
                # NEW or UPDATED
                chunks = chunk_text(content, url)
                if not chunks:
                    continue

                texts = [c['text'] for c in chunks]
                embeddings = self.model.encode(texts)

                page_data = {
                    "hash": new_hash,
                    "chunks": chunks,
                    "embeddings": embeddings
                }
                new_data_store[url] = page_data
                updated_count += 1

                all_chunks.append(chunks) # Flatten later
                all_embeddings.append(embeddings)

        # Flatten list for FAISS
        final_chunks_flat = []
        for c_list in all_chunks:
            if isinstance(c_list, list):
                final_chunks_flat.extend(c_list)
            else:
                final_chunks_flat.append(c_list) # Should not happen based on logic above

        if all_embeddings:
            final_embeddings_matrix = np.vstack(all_embeddings)
        else:
            final_embeddings_matrix = np.empty((0, 384)) # MiniLM dim

        # Build FAISS Index (IndexFlatIP for Cosine Similarity)
        dimension = 384
        index = faiss.IndexFlatIP(dimension)

        if final_embeddings_matrix.shape[0] > 0:
            index.add(final_embeddings_matrix)

        # Save everything
        new_hashes = {url: data['hash'] for url, data in new_data_store.items()}

        # We store flattened chunks in data_store metadata for retrieval mapping? 
        # Actually, simpler: store the flat list of chunks in a 'master_chunks.pkl' 
        # specifically for retrieval mapping by index ID.

        self.storage.save_state(new_hashes, new_data_store)
        self.storage.save_index(index)

        # Save retrieval map
        import pickle
        with open(os.path.join(self.storage.client_dir, "retrieval_map.pkl"), 'wb') as f:
            pickle.dump(final_chunks_flat, f)

        return updated_count

    def search(self, query: str, top_k: int = 3):
        index = self.storage.load_index()
        if not index or index.ntotal == 0:
            return [], []

        # Load retrieval map
        import pickle
        map_path = os.path.join(self.storage.client_dir, "retrieval_map.pkl")
        if not os.path.exists(map_path):
            return [], []

        with open(map_path, 'rb') as f:
            chunks_map = pickle.load(f)

        query_vector = self.model.encode([query])

        # Search
        distances, indices = index.search(query_vector, top_k)

        results = []
        scores = []

        for i in range(top_k):
            idx = indices[0][i]
            score = distances[0][i]
            if idx != -1 and idx < len(chunks_map):
                results.append(chunks_map[idx])
                scores.append(float(score))

        return results, scores