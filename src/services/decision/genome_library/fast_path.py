import numpy as np

FAST_PATH_SIMILARITY_THRESHOLD = 0.92

def cosine_similarity(a: list[float], b: list[float]) -> float:
    a, b = np.array(a), np.array(b)
    denom = (np.linalg.norm(a) * np.linalg.norm(b)) or 1e-9
    return float(np.dot(a, b) / denom)

def find_fast_path(fingerprint: list[float], genome_library_entries: list[dict]) -> dict:
    """genome_library_entries: past promoted genomes for this app_id, each with a
    stored 'fingerprint_vector' and 'genome'. Pulled from Cosmos DB in production;
    passed in as a list here so this function stays unit-testable."""
    best = {"found": False, "matched_genome_id": None, "similarity": 0.0}
    for entry in genome_library_entries:
        sim = cosine_similarity(fingerprint, entry["fingerprint_vector"])
        if sim > best["similarity"]:
            best = {"found": sim >= FAST_PATH_SIMILARITY_THRESHOLD,
                     "matched_genome_id": entry.get("genome_id") if sim >= FAST_PATH_SIMILARITY_THRESHOLD else None,
                     "similarity": sim}
    return best