"""
Indexador de documentos legais do FUNDEB para RAG.

Lê PDFs de docs/TCC/referencias/, faz chunking e persiste no ChromaDB.
Usa embeddings locais (all-MiniLM-L6-v2) para não depender de API externa.
"""

import chromadb
from pypdf import PdfReader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from pathlib import Path

# Caminhos
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DOCS_DIR = PROJECT_ROOT / "docs" / "TCC" / "referencias"
CHROMA_DIR = Path(__file__).resolve().parent.parent / "data" / "chroma_db"

# Documentos legais relevantes para FUNDEB
DOCUMENTOS = {
    "resolucao-cif-5-2024-ponderadores.pdf": {
        "titulo": "Resolução CIF nº 5/2024",
        "descricao": "Define ponderadores e fatores de ajuste para distribuição do FUNDEB",
        "tipo": "resolucao",
    },
    "fatores-ponderacao-fundeb-2025.pdf": {
        "titulo": "Fatores de Ponderação FUNDEB 2025",
        "descricao": "Tabela oficial de ponderadores por etapa e modalidade",
        "tipo": "ponderadores",
    },
    "nota-tecnica-11-2024-drec-inep.pdf": {
        "titulo": "Nota Técnica INEP nº 11/2024",
        "descricao": "Metodologia de cálculo do indicador DRec (Disponibilidade de Recursos)",
        "tipo": "nota_tecnica",
    },
    "portaria-mec-mf-4-2025-fundeb.pdf": {
        "titulo": "Portaria MEC/MF nº 4/2025",
        "descricao": "Estimativas de complementação VAAT/VAAF/VAAR por município",
        "tipo": "portaria",
    },
}


def extrair_texto_pdf(caminho: Path, max_pages: int = 50) -> str:
    """Extrai texto de um PDF, limitando páginas para documentos grandes."""
    reader = PdfReader(str(caminho))
    paginas = min(len(reader.pages), max_pages)
    texto = ""
    for i in range(paginas):
        texto += reader.pages[i].extract_text() or ""
        texto += f"\n\n--- Página {i + 1} ---\n\n"
    return texto


def indexar_documentos():
    """Indexa todos os documentos legais no ChromaDB."""
    print("=" * 60)
    print("FUNDEB Fácil - Indexação de Documentos Legais (RAG)")
    print("=" * 60)

    # Criar/conectar ChromaDB persistente
    CHROMA_DIR.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(CHROMA_DIR))

    # Deletar coleção existente para re-indexar
    try:
        client.delete_collection("fundeb_docs")
    except Exception:
        pass

    collection = client.create_collection(
        name="fundeb_docs",
        metadata={"hnsw:space": "cosine"},
    )

    # Splitter de texto
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=200,
        separators=["\n\n", "\n", ". ", " ", ""],
    )

    total_chunks = 0

    for filename, meta in DOCUMENTOS.items():
        filepath = DOCS_DIR / filename
        if not filepath.exists():
            print(f"  ⚠️  {filename} não encontrado, pulando...")
            continue

        print(f"\n  Processando: {meta['titulo']}")
        print(f"    Arquivo: {filename} ({filepath.stat().st_size / 1024:.0f} KB)")

        # Extrair texto
        texto = extrair_texto_pdf(filepath)
        print(f"    Texto extraído: {len(texto):,} caracteres")

        if len(texto) < 100:
            print(f"    ⚠️  Texto muito curto, pulando...")
            continue

        # Fazer chunking
        chunks = splitter.split_text(texto)
        print(f"    Chunks: {len(chunks)}")

        # Adicionar ao ChromaDB
        ids = [f"{filename}_{i}" for i in range(len(chunks))]
        metadatas = [
            {
                "source": filename,
                "titulo": meta["titulo"],
                "tipo": meta["tipo"],
                "chunk_index": i,
            }
            for i in range(len(chunks))
        ]

        collection.add(documents=chunks, ids=ids, metadatas=metadatas)
        total_chunks += len(chunks)

    print(f"\n{'=' * 60}")
    print(f"Total: {total_chunks} chunks indexados em {CHROMA_DIR}")
    print(f"{'=' * 60}")

    return total_chunks


if __name__ == "__main__":
    indexar_documentos()
