"""
Retriever para consultas RAG sobre legislação do FUNDEB.

Usa busca por palavras-chave (TF-IDF simplificado) sobre chunks pré-indexados em JSON.
Não requer ChromaDB nem gRPC — funciona em qualquer ambiente.
"""

import json
import math
import re
from functools import lru_cache
from pathlib import Path

RAG_JSON = Path(__file__).resolve().parent.parent / "data" / "rag_chunks.json"


@lru_cache(maxsize=1)
def _load_chunks() -> list[dict]:
    """Carrega chunks do JSON (cached)."""
    if not RAG_JSON.exists():
        return []
    with open(RAG_JSON, encoding="utf-8") as f:
        return json.load(f)


def _tokenize(text: str) -> list[str]:
    """Tokeniza texto em palavras normalizadas."""
    text = text.lower()
    # Remove acentos simples para matching
    replacements = {
        "á": "a", "à": "a", "ã": "a", "â": "a",
        "é": "e", "ê": "e",
        "í": "i",
        "ó": "o", "ô": "o", "õ": "o",
        "ú": "u", "ü": "u",
        "ç": "c",
    }
    for k, v in replacements.items():
        text = text.replace(k, v)
    return re.findall(r"\b\w{2,}\b", text)


def _score_chunk(query_tokens: list[str], chunk_text: str, n_docs: int, doc_freq: dict) -> float:
    """Calcula score BM25-like para um chunk."""
    chunk_tokens = _tokenize(chunk_text)
    if not chunk_tokens:
        return 0.0

    chunk_len = len(chunk_tokens)
    avg_len = 300  # estimativa média
    k1 = 1.5
    b = 0.75

    score = 0.0
    token_counts = {}
    for t in chunk_tokens:
        token_counts[t] = token_counts.get(t, 0) + 1

    for qt in set(query_tokens):
        tf = token_counts.get(qt, 0)
        if tf == 0:
            continue
        df = doc_freq.get(qt, 1)
        idf = math.log((n_docs - df + 0.5) / (df + 0.5) + 1)
        tf_norm = (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * chunk_len / avg_len))
        score += idf * tf_norm

    return score


def buscar_contexto(query: str, n_results: int = 5) -> list[dict]:
    """
    Busca documentos relevantes para a query.

    Returns:
        Lista de dicts com 'texto', 'fonte', 'titulo', 'score'
    """
    chunks = _load_chunks()
    if not chunks:
        return []

    query_tokens = _tokenize(query)
    if not query_tokens:
        return []

    # Calcular document frequency para IDF
    n_docs = len(chunks)
    doc_freq: dict[str, int] = {}
    for chunk in chunks:
        seen = set(_tokenize(chunk["texto"]))
        for token in seen:
            doc_freq[token] = doc_freq.get(token, 0) + 1

    # Scorar todos os chunks
    scored = []
    for chunk in chunks:
        score = _score_chunk(query_tokens, chunk["texto"], n_docs, doc_freq)
        if score > 0:
            scored.append((score, chunk))

    # Ordenar por score decrescente
    scored.sort(key=lambda x: x[0], reverse=True)

    return [
        {
            "texto": chunk["texto"],
            "fonte": chunk["fonte"],
            "titulo": chunk["titulo"],
            "score": round(score, 4),
        }
        for score, chunk in scored[:n_results]
    ]


def formatar_contexto_para_prompt(contextos: list[dict]) -> str:
    """Formata contextos recuperados para inserir no prompt do LLM."""
    if not contextos:
        return ""

    partes = ["CONTEXTO LEGISLATIVO (documentos oficiais do FUNDEB):"]
    for i, ctx in enumerate(contextos, 1):
        partes.append(
            f"\n[{i}] {ctx['titulo']}\n{ctx['texto'][:800]}"
        )

    return "\n".join(partes)
