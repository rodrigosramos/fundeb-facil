"""
Agente Calculadora — Ferramentas determinísticas para cálculos do FUNDEB.

Não usa LLM. Fornece ferramentas (tools) que o agente conversacional
pode invocar via LangGraph para realizar cálculos e consultas ao banco.
"""

import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "fundeb_data.db"


def _get_conn():
    return sqlite3.connect(str(DB_PATH), check_same_thread=False)


def obter_dados_municipio(codigo_ibge: str) -> dict:
    """Busca todos os dados de um município no banco."""
    conn = _get_conn()
    cur = conn.cursor()

    mun = cur.execute(
        "SELECT codigo_ibge, nome, uf, populacao, fator_nse, fator_drec, vaat_por_aluno "
        "FROM municipios WHERE codigo_ibge = ?",
        (codigo_ibge,),
    ).fetchone()

    if not mun:
        return {"erro": f"Município com código {codigo_ibge} não encontrado."}

    matriculas = cur.execute(
        "SELECT etapa, modalidade, quantidade FROM matriculas WHERE codigo_ibge = ?",
        (codigo_ibge,),
    ).fetchall()

    conn.close()

    return {
        "codigo_ibge": mun[0],
        "nome": mun[1],
        "uf": mun[2],
        "populacao": mun[3],
        "fator_nse": mun[4],
        "fator_drec": mun[5],
        "vaat_por_aluno": mun[6],
        "matriculas": [
            {"etapa": m[0], "modalidade": m[1], "quantidade": m[2]} for m in matriculas
        ],
        "total_matriculas": sum(m[2] for m in matriculas),
    }


def calcular_complementacao(codigo_ibge: str, alteracoes: Optional[dict] = None) -> dict:
    """
    Calcula complementações VAAT/VAAF para um município.

    Args:
        codigo_ibge: Código IBGE do município
        alteracoes: Dict opcional com alterações de matrículas.
                    Formato: {"Creche/Integral": 500, "EF Anos Iniciais/Parcial Urbano": 1200}

    Returns:
        Dict com resultados detalhados do cálculo
    """
    dados = obter_dados_municipio(codigo_ibge)
    if "erro" in dados:
        return dados

    conn = _get_conn()
    ponderadores = pd.read_sql_query("SELECT * FROM ponderadores", conn)
    conn.close()

    fator_nse = dados["fator_nse"]
    fator_drec = dados["fator_drec"]
    # Valor médio nacional de complementação por matrícula ajustada
    vaat_por_aluno = 4100
    vaaf_por_aluno = 4138

    # Aplicar alterações de matrículas (se fornecidas)
    matriculas = {f"{m['etapa']}/{m['modalidade']}": m["quantidade"] for m in dados["matriculas"]}
    if alteracoes:
        for chave, novo_valor in alteracoes.items():
            matriculas[chave] = novo_valor

    resultados = []
    total_brutas = 0
    total_ajust_vaat = 0
    total_ajust_vaaf = 0

    for chave, qtd in sorted(matriculas.items()):
        etapa, modalidade = chave.split("/", 1)
        pond = ponderadores[
            (ponderadores["etapa"] == etapa) & (ponderadores["modalidade"] == modalidade)
        ]
        if pond.empty:
            continue

        p_vaat = pond["ponderador_vaat"].values[0]
        p_vaaf = pond["ponderador_vaaf"].values[0]

        ajust_vaat = qtd * p_vaat * fator_nse * fator_drec
        ajust_vaaf = qtd * p_vaaf * fator_nse * fator_drec

        total_brutas += qtd
        total_ajust_vaat += ajust_vaat
        total_ajust_vaaf += ajust_vaaf

        resultados.append({
            "etapa": etapa,
            "modalidade": modalidade,
            "matriculas": qtd,
            "ponderador_vaat": p_vaat,
            "ponderador_vaaf": p_vaaf,
            "ajustadas_vaat": round(ajust_vaat, 1),
            "ajustadas_vaaf": round(ajust_vaaf, 1),
        })

    valor_vaat = total_ajust_vaat * vaat_por_aluno
    valor_vaaf = total_ajust_vaaf * vaaf_por_aluno

    return {
        "municipio": f"{dados['nome']}/{dados['uf']}",
        "codigo_ibge": codigo_ibge,
        "fator_nse": fator_nse,
        "fator_drec": fator_drec,
        "vaat_por_aluno": vaat_por_aluno,
        "total_matriculas_brutas": total_brutas,
        "total_ajustadas_vaat": round(total_ajust_vaat, 1),
        "total_ajustadas_vaaf": round(total_ajust_vaaf, 1),
        "valor_vaat": round(valor_vaat, 2),
        "valor_vaaf": round(valor_vaaf, 2),
        "valor_total": round(valor_vaat + valor_vaaf, 2),
        "detalhamento": resultados,
        "alteracoes_aplicadas": alteracoes is not None,
    }


def simular_cenario(codigo_ibge: str, descricao: str) -> dict:
    """
    Simula cenários comuns a partir de descrição textual.

    Cenários suportados:
    - "expandir creche em X%"
    - "dobrar tempo integral"
    - "aumentar EJA em X alunos"
    """
    dados = obter_dados_municipio(codigo_ibge)
    if "erro" in dados:
        return dados

    descricao_lower = descricao.lower()
    alteracoes = {}

    # Padrão: "expandir X em Y%"
    for m in dados["matriculas"]:
        chave = f"{m['etapa']}/{m['modalidade']}"
        etapa_lower = m["etapa"].lower()

        if "creche" in descricao_lower and "creche" in etapa_lower:
            fator = _extrair_percentual(descricao_lower)
            alteracoes[chave] = int(m["quantidade"] * (1 + fator / 100))
        elif "integral" in descricao_lower and "integral" in m["modalidade"].lower():
            fator = _extrair_percentual(descricao_lower)
            alteracoes[chave] = int(m["quantidade"] * (1 + fator / 100))
        elif "eja" in descricao_lower and "eja" in etapa_lower:
            fator = _extrair_percentual(descricao_lower)
            alteracoes[chave] = int(m["quantidade"] * (1 + fator / 100))

    if not alteracoes:
        return {
            "erro": "Não foi possível interpretar o cenário. "
            "Tente algo como 'expandir creche em 20%' ou 'dobrar tempo integral'."
        }

    resultado = calcular_complementacao(codigo_ibge, alteracoes)
    resultado["cenario"] = descricao
    return resultado


def _extrair_percentual(texto: str) -> float:
    """Extrai percentual de um texto como 'expandir em 20%'."""
    import re
    match = re.search(r"(\d+)\s*%", texto)
    if match:
        return float(match.group(1))
    if "dobr" in texto:
        return 100.0
    if "triplic" in texto:
        return 200.0
    return 20.0  # default
