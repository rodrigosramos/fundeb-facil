"""
Retriever para consultas RAG sobre legislação do FUNDEB.

Usa ChromaDB local com embeddings all-MiniLM-L6-v2.
"""

import chromadb
from pathlib import Path

CHROMA_DIR = Path(__file__).resolve().parent.parent / "data" / "chroma_db"


def _get_collection():
    """Obtém a coleção ChromaDB."""
    client = chromadb.PersistentClient(path=str(CHROMA_DIR))
    return client.get_collection("fundeb_docs")


def buscar_contexto(query: str, n_results: int = 5) -> list[dict]:
    """
    Busca documentos relevantes para a query.

    Returns:
        Lista de dicts com 'texto', 'fonte', 'titulo', 'score'
    """
    try:
        collection = _get_collection()
    except Exception:
        return []

    results = collection.query(query_texts=[query], n_results=n_results)

    contextos = []
    for i in range(len(results["documents"][0])):
        contextos.append({
            "texto": results["documents"][0][i],
            "fonte": results["metadatas"][0][i].get("source", ""),
            "titulo": results["metadatas"][0][i].get("titulo", ""),
            "score": results["distances"][0][i] if results.get("distances") else None,
        })

    return contextos


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
