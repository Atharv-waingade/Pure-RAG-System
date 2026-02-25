import re

def chunk_text(text: str, url: str, chunk_size: int = 400, overlap: int = 50):

    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()

    words = text.split()
    chunks = []

    if not words:
        return []

    for i in range(0, len(words), chunk_size - overlap):
        chunk_words = words[i:i + chunk_size]
        chunk_text = " ".join(chunk_words)

        chunks.append({
            "text": chunk_text,
            "url": url,
            "chunk_index": len(chunks)
        })

        if i + chunk_size >= len(words):
            break

    return chunks