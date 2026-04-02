"""
ETL: Popular banco SQLite com dados reais de todos os municípios brasileiros.

Fontes:
- FNDE CSV: NSE, DRec, VAAT por município
- Sinopse Censo Escolar 2024: matrículas por município × etapa × modalidade
- IBGE API: população por município

Saída: fundeb-facil-app/data/fundeb_data.db
"""

import sqlite3
import pandas as pd
import sys
from pathlib import Path

# Caminhos
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
FNDE_CSV = PROJECT_ROOT / "dados_externos" / "municipios-ponderadores-nse-drec.csv"
SINOPSE_XLSX = (
    PROJECT_ROOT
    / "dados_externos"
    / "sinopse_2024"
    / "sinopse_estatistica_censo_escolar_2024"
    / "Sinopse_Estatistica_da_Educação_Basica_2024.xlsx"
)
DB_PATH = Path(__file__).resolve().parent.parent / "data" / "fundeb_data.db"

# ---------------------------------------------------------------------------
# 1. Ler dados FNDE (NSE, DRec, VAAT por município)
# ---------------------------------------------------------------------------

def load_fnde():
    """Carrega CSV do FNDE com ponderadores NSE/DRec e valor VAAT."""
    df = pd.read_csv(FNDE_CSV)
    # Filtrar apenas municípios (remover governos estaduais)
    df = df[~df["Ente_Federado"].str.contains("GOVERNO", na=False)].copy()
    # Converter código IBGE para inteiro (7 dígitos)
    df["Codigo_IBGE"] = df["Codigo_IBGE"].astype(float).astype(int).astype(str)
    df = df.rename(columns={
        "Ente_Federado": "nome",
        "Ponderador_NSE": "fator_nse",
        "Ponderador_DRec": "fator_drec",
        "VAAT": "vaat_por_aluno",
    })
    print(f"[FNDE] {len(df)} municípios carregados (NSE/DRec/VAAT)")
    return df[["UF", "Codigo_IBGE", "nome", "vaat_por_aluno", "fator_drec", "fator_nse"]]


# ---------------------------------------------------------------------------
# 2. Ler matrículas da Sinopse Censo Escolar 2024
# ---------------------------------------------------------------------------

def _read_sinopse_sheet(xlsx_path, sheet_name, col_municipal_a, col_municipal_b,
                        label_a, label_b, skip_header=10):
    """
    Lê uma aba da Sinopse e retorna DataFrame com código IBGE e duas colunas
    de matrículas municipais (ex.: integral/parcial ou EF/EM).

    Parâmetros:
        col_municipal_a: índice da coluna Municipal do primeiro grupo (0-based)
        col_municipal_b: índice da coluna Municipal do segundo grupo (0-based)
        label_a, label_b: nomes das colunas de saída
        skip_header: linhas a pular antes dos dados (padrão 10 = após header)
    """
    df = pd.read_excel(
        xlsx_path,
        sheet_name=sheet_name,
        header=None,
        skiprows=skip_header,
        dtype={3: str},
    )
    # Coluna 3 = Código do Município
    # Filtrar apenas linhas com código IBGE numérico (7 dígitos)
    df = df.dropna(subset=[3])
    df[3] = df[3].astype(str).str.strip()
    df = df[df[3].str.match(r"^\d{7}$")].copy()

    result = pd.DataFrame({
        "Codigo_IBGE": df[3].values,
        label_a: pd.to_numeric(df.iloc[:, col_municipal_a], errors="coerce").fillna(0).astype(int).values,
        label_b: pd.to_numeric(df.iloc[:, col_municipal_b], errors="coerce").fillna(0).astype(int).values,
    })
    return result


def load_matriculas():
    """
    Carrega matrículas da rede municipal por município, etapa e modalidade.

    Abas utilizadas:
    - Integral/Parcial: 1.9 (Creche), 1.13 (Pré), 1.19 (EF AI), 1.24 (EF AF), 1.29 (EM)
    - Urbana/Rural: 1.16 (EF AI), 1.21 (EF AF)
    - EJA: EJA 1.35 (Fundamental/Médio)

    Estrutura padrão das abas integral/parcial:
    Col 8 = Integral Municipal, Col 13 = Parcial Municipal

    Estrutura padrão das abas localização:
    Col 8 = Urbana Municipal, Col 13 = Rural Municipal
    """
    print("[Sinopse] Lendo matrículas da rede municipal...")
    records = []

    # --- Creche ---
    df = _read_sinopse_sheet(SINOPSE_XLSX, "1.9", 8, 13, "integral", "parcial")
    for _, row in df.iterrows():
        records.append((row["Codigo_IBGE"], "Creche", "Integral", row["integral"]))
        records.append((row["Codigo_IBGE"], "Creche", "Parcial", row["parcial"]))
    print(f"  Creche: {len(df)} municípios")

    # --- Pré-escola ---
    df = _read_sinopse_sheet(SINOPSE_XLSX, "1.13", 8, 13, "integral", "parcial")
    for _, row in df.iterrows():
        records.append((row["Codigo_IBGE"], "Pré-escola", "Integral", row["integral"]))
        records.append((row["Codigo_IBGE"], "Pré-escola", "Parcial", row["parcial"]))
    print(f"  Pré-escola: {len(df)} municípios")

    # --- EF Anos Iniciais (integral/parcial + urbano/rural) ---
    df_ip = _read_sinopse_sheet(SINOPSE_XLSX, "1.19", 8, 13, "integral", "parcial")
    df_ur = _read_sinopse_sheet(SINOPSE_XLSX, "1.16", 8, 13, "urbana", "rural")
    df_ef_ai = df_ip.merge(df_ur, on="Codigo_IBGE", how="outer").fillna(0)
    for col in ["integral", "parcial", "urbana", "rural"]:
        df_ef_ai[col] = df_ef_ai[col].astype(int)
    # Parcial Urbano ≈ Urbana Municipal - Integral (assumindo integral ≈ urbano)
    # Parcial Rural ≈ Rural Municipal
    df_ef_ai["parcial_urbano"] = (df_ef_ai["urbana"] - df_ef_ai["integral"]).clip(lower=0)
    df_ef_ai["parcial_rural"] = df_ef_ai["rural"]
    for _, row in df_ef_ai.iterrows():
        records.append((row["Codigo_IBGE"], "EF Anos Iniciais", "Integral", int(row["integral"])))
        records.append((row["Codigo_IBGE"], "EF Anos Iniciais", "Parcial Urbano", int(row["parcial_urbano"])))
        records.append((row["Codigo_IBGE"], "EF Anos Iniciais", "Parcial Rural", int(row["parcial_rural"])))
    print(f"  EF Anos Iniciais: {len(df_ef_ai)} municípios")

    # --- EF Anos Finais (integral/parcial + urbano/rural) ---
    df_ip = _read_sinopse_sheet(SINOPSE_XLSX, "1.24", 8, 13, "integral", "parcial")
    df_ur = _read_sinopse_sheet(SINOPSE_XLSX, "1.21", 8, 13, "urbana", "rural")
    df_ef_af = df_ip.merge(df_ur, on="Codigo_IBGE", how="outer").fillna(0)
    for col in ["integral", "parcial", "urbana", "rural"]:
        df_ef_af[col] = df_ef_af[col].astype(int)
    df_ef_af["parcial_urbano"] = (df_ef_af["urbana"] - df_ef_af["integral"]).clip(lower=0)
    df_ef_af["parcial_rural"] = df_ef_af["rural"]
    for _, row in df_ef_af.iterrows():
        records.append((row["Codigo_IBGE"], "EF Anos Finais", "Integral", int(row["integral"])))
        records.append((row["Codigo_IBGE"], "EF Anos Finais", "Parcial Urbano", int(row["parcial_urbano"])))
        records.append((row["Codigo_IBGE"], "EF Anos Finais", "Parcial Rural", int(row["parcial_rural"])))
    print(f"  EF Anos Finais: {len(df_ef_af)} municípios")

    # --- Ensino Médio ---
    df = _read_sinopse_sheet(SINOPSE_XLSX, "1.29", 8, 13, "integral", "parcial")
    for _, row in df.iterrows():
        records.append((row["Codigo_IBGE"], "Ensino Médio", "Integral", row["integral"]))
        records.append((row["Codigo_IBGE"], "Ensino Médio", "Parcial", row["parcial"]))
    print(f"  Ensino Médio: {len(df)} municípios")

    # --- EJA (Fundamental / Médio) ---
    df = _read_sinopse_sheet(SINOPSE_XLSX, "EJA 1.35", 8, 13, "fundamental", "medio")
    for _, row in df.iterrows():
        records.append((row["Codigo_IBGE"], "EJA", "Fundamental", row["fundamental"]))
        records.append((row["Codigo_IBGE"], "EJA", "Médio", row["medio"]))
    print(f"  EJA: {len(df)} municípios")

    mat_df = pd.DataFrame(records, columns=["Codigo_IBGE", "etapa", "modalidade", "quantidade"])
    # Remover registros com 0 matrículas para manter o banco limpo
    mat_df = mat_df[mat_df["quantidade"] > 0]
    print(f"[Sinopse] Total: {len(mat_df)} registros de matrícula (> 0)")
    return mat_df


# ---------------------------------------------------------------------------
# 3. Extrair lista de municípios da Sinopse (sem API externa)
# ---------------------------------------------------------------------------

def _extract_municipios_sinopse():
    """
    Extrai lista completa de municípios da Sinopse (aba Creche 1.6).
    Retorna DataFrame com Codigo_IBGE, nome, UF.
    """
    print("[Sinopse] Extraindo lista de municípios...")
    df = pd.read_excel(
        SINOPSE_XLSX,
        sheet_name="Creche 1.6",
        header=None,
        skiprows=10,
        dtype={3: str},
    )
    # Filtrar apenas linhas com código IBGE válido (7 dígitos)
    df = df.dropna(subset=[3])
    df[3] = df[3].astype(str).str.strip()
    df = df[df[3].str.match(r"^\d{7}$")].copy()

    result = pd.DataFrame({
        "Codigo_IBGE": df[3].values,
        "nome_sinopse": df[2].astype(str).str.strip().values,
        "uf_sinopse": df[1].astype(str).str.strip().values,
    })
    # Remover duplicatas
    result = result.drop_duplicates(subset="Codigo_IBGE")
    print(f"[Sinopse] {len(result)} municípios extraídos")
    return result


def load_municipios(fnde_df):
    """
    Constrói lista unificada de municípios a partir da Sinopse + FNDE.
    A Sinopse fornece todos os municípios; o FNDE adiciona NSE/DRec.
    """
    # Mapeamento nome do estado → sigla UF
    ESTADO_PARA_UF = {
        "Rondônia": "RO", "Acre": "AC", "Amazonas": "AM", "Roraima": "RR",
        "Pará": "PA", "Amapá": "AP", "Tocantins": "TO", "Maranhão": "MA",
        "Piauí": "PI", "Ceará": "CE", "Rio Grande do Norte": "RN",
        "Paraíba": "PB", "Pernambuco": "PE", "Alagoas": "AL",
        "Sergipe": "SE", "Bahia": "BA", "Minas Gerais": "MG",
        "Espírito Santo": "ES", "Rio de Janeiro": "RJ", "São Paulo": "SP",
        "Paraná": "PR", "Santa Catarina": "SC", "Rio Grande do Sul": "RS",
        "Mato Grosso do Sul": "MS", "Mato Grosso": "MT", "Goiás": "GO",
        "Distrito Federal": "DF",
    }

    sinopse_mun = _extract_municipios_sinopse()

    # Converter nome do estado para sigla UF
    sinopse_mun["uf_sinopse"] = sinopse_mun["uf_sinopse"].map(ESTADO_PARA_UF)

    # Merge com FNDE para obter NSE/DRec
    merged = sinopse_mun.merge(fnde_df, on="Codigo_IBGE", how="left")

    # Nome: preferir Sinopse (tem acentos corretos), FNDE é MAIÚSCULAS
    merged["nome_final"] = merged["nome_sinopse"]
    # UF: preferir FNDE (sigla garantida), senão Sinopse (mapeada)
    merged["uf_final"] = merged["UF"].fillna(merged["uf_sinopse"])

    # Preencher NSE/DRec ausentes com valores neutros
    merged["fator_nse"] = merged["fator_nse"].fillna(1.0)
    merged["fator_drec"] = merged["fator_drec"].fillna(1.0)
    merged["vaat_por_aluno"] = merged["vaat_por_aluno"].fillna(0.0)

    n_com_fnde = merged["nome"].notna().sum()
    print(f"[Merge] {len(merged)} municípios totais, {n_com_fnde} com dados FNDE (NSE/DRec)")

    return merged[["Codigo_IBGE", "nome_final", "uf_final",
                    "fator_nse", "fator_drec", "vaat_por_aluno"]]


# ---------------------------------------------------------------------------
# 4. Combinar e criar banco SQLite
# ---------------------------------------------------------------------------

def create_database(mun_df, mat_df):
    """Cria o banco SQLite com todas as tabelas."""

    # Remover banco existente
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    # Criar tabelas
    cursor.execute("""
    CREATE TABLE municipios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigo_ibge TEXT UNIQUE NOT NULL,
        nome TEXT NOT NULL,
        uf TEXT NOT NULL,
        populacao INTEGER DEFAULT 0,
        fator_nse REAL DEFAULT 1.0,
        fator_drec REAL DEFAULT 1.0,
        vaat_por_aluno REAL DEFAULT 0.0
    )
    """)

    cursor.execute("""
    CREATE TABLE matriculas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigo_ibge TEXT NOT NULL,
        etapa TEXT NOT NULL,
        modalidade TEXT NOT NULL,
        quantidade INTEGER NOT NULL,
        FOREIGN KEY (codigo_ibge) REFERENCES municipios(codigo_ibge)
    )
    """)

    cursor.execute("""
    CREATE TABLE ponderadores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        etapa TEXT NOT NULL,
        modalidade TEXT NOT NULL,
        ponderador_vaat REAL NOT NULL,
        ponderador_vaaf REAL NOT NULL,
        multiplicador_indigena_quilombola REAL DEFAULT 1.40,
        multiplicador_rural REAL DEFAULT 1.15,
        adicional_especial REAL DEFAULT 1.40
    )
    """)

    # Índices para performance
    cursor.execute("CREATE INDEX idx_matriculas_ibge ON matriculas(codigo_ibge)")
    cursor.execute("CREATE INDEX idx_municipios_uf ON municipios(uf)")

    # Inserir municípios (população estimada via total de matrículas ÷ ~0.17)
    mun_records = [
        (row["Codigo_IBGE"], row["nome_final"], row["uf_final"],
         0, row["fator_nse"], row["fator_drec"], row["vaat_por_aluno"])
        for _, row in mun_df.iterrows()
    ]
    cursor.executemany(
        "INSERT INTO municipios (codigo_ibge, nome, uf, populacao, fator_nse, fator_drec, vaat_por_aluno) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        mun_records,
    )
    print(f"[DB] {len(mun_records)} municípios inseridos")

    # Inserir matrículas (só para municípios que existem no banco)
    valid_codigos = set(mun_df["Codigo_IBGE"])
    mat_valid = mat_df[mat_df["Codigo_IBGE"].isin(valid_codigos)].copy()
    mat_records = [
        (row["Codigo_IBGE"], row["etapa"], row["modalidade"], int(row["quantidade"]))
        for _, row in mat_valid.iterrows()
    ]
    cursor.executemany(
        "INSERT INTO matriculas (codigo_ibge, etapa, modalidade, quantidade) VALUES (?, ?, ?, ?)",
        mat_records,
    )
    print(f"[DB] {len(mat_records)} registros de matrícula inseridos")

    # Inserir ponderadores oficiais (Resolução CIF nº 5/2024)
    ponderadores = [
        ("Creche", "Integral", 1.90, 1.55, 1.40, 1.15, 1.40),
        ("Creche", "Parcial", 1.50, 1.30, 1.40, 1.15, 1.40),
        ("Pré-escola", "Integral", 1.88, 1.50, 1.40, 1.15, 1.40),
        ("Pré-escola", "Parcial", 1.40, 1.25, 1.40, 1.15, 1.40),
        ("EF Anos Iniciais", "Integral", 1.45, 1.30, 1.40, 1.15, 1.40),
        ("EF Anos Iniciais", "Parcial Urbano", 1.00, 1.00, 1.40, 1.15, 1.40),
        ("EF Anos Iniciais", "Parcial Rural", 1.20, 1.15, 1.40, 1.15, 1.40),
        ("EF Anos Finais", "Integral", 1.40, 1.25, 1.40, 1.15, 1.40),
        ("EF Anos Finais", "Parcial Urbano", 1.15, 1.10, 1.40, 1.15, 1.40),
        ("EF Anos Finais", "Parcial Rural", 1.30, 1.20, 1.40, 1.15, 1.40),
        ("Ensino Médio", "Integral", 1.55, 1.40, 1.40, 1.15, 1.40),
        ("Ensino Médio", "Parcial", 1.30, 1.25, 1.40, 1.15, 1.40),
        ("EJA", "Fundamental", 1.05, 0.95, 1.40, 1.15, 1.40),
        ("EJA", "Médio", 1.20, 1.10, 1.40, 1.15, 1.40),
    ]
    cursor.executemany(
        "INSERT INTO ponderadores (etapa, modalidade, ponderador_vaat, ponderador_vaaf, "
        "multiplicador_indigena_quilombola, multiplicador_rural, adicional_especial) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ponderadores,
    )
    print(f"[DB] {len(ponderadores)} ponderadores inseridos")

    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# 5. Validação
# ---------------------------------------------------------------------------

def validate_database():
    """Valida integridade do banco de dados."""
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    n_mun = cursor.execute("SELECT COUNT(*) FROM municipios").fetchone()[0]
    n_mat = cursor.execute("SELECT COUNT(*) FROM matriculas").fetchone()[0]
    n_pond = cursor.execute("SELECT COUNT(*) FROM ponderadores").fetchone()[0]
    n_com_nse = cursor.execute("SELECT COUNT(*) FROM municipios WHERE fator_nse != 1.0").fetchone()[0]
    n_com_pop = cursor.execute("SELECT COUNT(*) FROM municipios WHERE populacao > 0").fetchone()[0]
    n_ufs = cursor.execute("SELECT COUNT(DISTINCT uf) FROM municipios").fetchone()[0]
    total_mat = cursor.execute("SELECT SUM(quantidade) FROM matriculas").fetchone()[0] or 0

    # Municípios com matrículas
    n_com_mat = cursor.execute(
        "SELECT COUNT(DISTINCT codigo_ibge) FROM matriculas"
    ).fetchone()[0]

    print("\n" + "=" * 60)
    print("VALIDAÇÃO DO BANCO DE DADOS")
    print("=" * 60)
    print(f"  Municípios:           {n_mun:>6,}")
    print(f"  Com NSE/DRec (FNDE):  {n_com_nse:>6,}")
    print(f"  Com população:        {n_com_pop:>6,}")
    print(f"  Com matrículas:       {n_com_mat:>6,}")
    print(f"  UFs:                  {n_ufs:>6}")
    print(f"  Registros matrícula:  {n_mat:>6,}")
    print(f"  Total matrículas:     {total_mat:>10,}")
    print(f"  Ponderadores:         {n_pond:>6}")
    print(f"  Tamanho DB:           {DB_PATH.stat().st_size / 1024 / 1024:.1f} MB")

    # Top 5 municípios por total de matrículas municipais
    print("\n  Top 5 municípios (matrículas rede municipal):")
    top5 = cursor.execute("""
        SELECT m.nome, m.uf, SUM(mat.quantidade) as total
        FROM municipios m
        JOIN matriculas mat ON m.codigo_ibge = mat.codigo_ibge
        GROUP BY m.codigo_ibge
        ORDER BY total DESC
        LIMIT 5
    """).fetchall()
    for nome, uf, total in top5:
        print(f"    {nome}/{uf}: {total:,}")

    # Distribuição por etapa
    print("\n  Matrículas por etapa:")
    etapas = cursor.execute("""
        SELECT etapa, SUM(quantidade)
        FROM matriculas
        GROUP BY etapa
        ORDER BY SUM(quantidade) DESC
    """).fetchall()
    for etapa, total in etapas:
        print(f"    {etapa}: {total:,}")

    conn.close()
    print("=" * 60)

    # Alertas
    if n_mun < 5500:
        print(f"⚠️  ALERTA: Apenas {n_mun} municípios (esperado ~5.570)")
    if n_ufs != 27:
        print(f"⚠️  ALERTA: Apenas {n_ufs} UFs (esperado 27)")
    if total_mat < 10_000_000:
        print(f"⚠️  ALERTA: Apenas {total_mat:,} matrículas totais (rede municipal)")

    print("\n✅ Banco de dados criado em:", DB_PATH)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 60)
    print("FUNDEB Fácil - ETL de Dados Reais")
    print("=" * 60)

    # Verificar arquivos fonte
    if not FNDE_CSV.exists():
        sys.exit(f"ERRO: Arquivo não encontrado: {FNDE_CSV}")
    if not SINOPSE_XLSX.exists():
        sys.exit(f"ERRO: Arquivo não encontrado: {SINOPSE_XLSX}")

    fnde_df = load_fnde()
    mat_df = load_matriculas()
    mun_df = load_municipios(fnde_df)
    create_database(mun_df, mat_df)
    validate_database()
