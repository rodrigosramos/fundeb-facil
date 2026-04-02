"""
Agente Conversacional — Claude + RAG + contexto do município.

3 modos operacionais:
1. Explicação Contextual: explica cálculos passo a passo
2. Simulação Guiada: interpreta cenários e usa agente calculadora
3. Consulta Legislativa: responde sobre legislação usando RAG
"""

import os
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage

from agents.calculator import obter_dados_municipio, calcular_complementacao, simular_cenario
from rag.retriever import buscar_contexto, formatar_contexto_para_prompt

SYSTEM_PROMPT = """Você é o assistente do FUNDEB Fácil, um especialista em financiamento da educação básica brasileira.

Seu papel é ajudar gestores municipais a compreender o FUNDEB, especialmente as complementações VAAT e VAAF.

REGRAS:
- Responda SEMPRE em português brasileiro
- Seja didático, claro e objetivo
- Use linguagem acessível para gestores sem formação técnica em economia
- Quando citar valores, mostre os cálculos passo a passo
- Quando citar legislação, indique a fonte (Lei, Resolução, Portaria)
- Se não tiver certeza, diga que não sabe em vez de inventar

CONTEXTO DO MUNICÍPIO SELECIONADO:
{contexto_municipio}

{contexto_rag}

Você pode ajudar com:
1. EXPLICAR cálculos de complementação VAAT/VAAF do município
2. SIMULAR cenários (ex: "o que acontece se expandir creches em 20%?")
3. CONSULTAR legislação do FUNDEB (Lei 14.113/2020, Resolução CIF nº 5/2024, etc.)
"""


def _formatar_contexto_municipio(dados: dict) -> str:
    """Formata dados do município para o prompt do LLM."""
    if "erro" in dados:
        return "Nenhum município selecionado."

    matriculas_texto = "\n".join(
        f"  - {m['etapa']} / {m['modalidade']}: {m['quantidade']:,} alunos"
        for m in dados["matriculas"]
    )

    return (
        f"Município: {dados['nome']}/{dados['uf']}\n"
        f"Código IBGE: {dados['codigo_ibge']}\n"
        f"Fator NSE: {dados['fator_nse']:.4f}\n"
        f"Fator DRec: {dados['fator_drec']:.4f}\n"
        f"VAAT por aluno: R$ {dados['vaat_por_aluno']:,.2f}\n"
        f"Total de matrículas (rede municipal): {dados['total_matriculas']:,}\n"
        f"Matrículas por etapa/modalidade:\n{matriculas_texto}"
    )


def _detectar_modo(mensagem: str) -> str:
    """Detecta o modo operacional com base na mensagem do usuário."""
    msg_lower = mensagem.lower()

    # Simulação
    palavras_simulacao = [
        "simul", "cenário", "expandir", "aumentar", "dobrar", "triplicar",
        "reduzir", "o que acontece", "se eu", "impacto de",
    ]
    if any(p in msg_lower for p in palavras_simulacao):
        return "simulacao"

    # Explicação de cálculo
    palavras_calculo = [
        "calcul", "como é feito", "explique o cálculo", "passo a passo",
        "como funciona o vaat", "como funciona o vaaf", "resultado",
        "complementação", "quanto receb",
    ]
    if any(p in msg_lower for p in palavras_calculo):
        return "explicacao"

    # Default: consulta legislativa
    return "consulta"


def gerar_resposta(
    mensagem: str,
    codigo_ibge: str,
    historico: list[dict] | None = None,
    resultado_calculo: dict | None = None,
) -> str:
    """
    Gera resposta do assistente conversacional.

    Args:
        mensagem: Pergunta do usuário
        codigo_ibge: Código IBGE do município selecionado
        historico: Lista de mensagens anteriores [{"role": "user/assistant", "content": "..."}]
        resultado_calculo: Resultado do último cálculo (se disponível)

    Returns:
        Texto da resposta do assistente
    """
    # Verificar se temos API key
    api_key = os.getenv("ANTHROPIC_API_KEY") or st_get_secret("ANTHROPIC_API_KEY")
    if not api_key:
        return (
            "O assistente IA requer uma chave de API da Anthropic para funcionar.\n\n"
            "Configure a variável `ANTHROPIC_API_KEY` nas variáveis de ambiente ou "
            "nos secrets do Streamlit Cloud."
        )

    # Obter dados do município
    dados_municipio = obter_dados_municipio(codigo_ibge)
    contexto_mun = _formatar_contexto_municipio(dados_municipio)

    # Detectar modo e preparar contexto
    modo = _detectar_modo(mensagem)

    # Para simulações, executar o cálculo antes
    info_simulacao = ""
    if modo == "simulacao":
        resultado_sim = simular_cenario(codigo_ibge, mensagem)
        if "erro" not in resultado_sim:
            info_simulacao = (
                f"\n\nRESULTADO DA SIMULAÇÃO ('{resultado_sim.get('cenario', '')}'):\n"
                f"- Matrículas brutas: {resultado_sim['total_matriculas_brutas']:,}\n"
                f"- Matrículas ajustadas VAAT: {resultado_sim['total_ajustadas_vaat']:,.1f}\n"
                f"- Valor VAAT estimado: R$ {resultado_sim['valor_vaat']:,.2f}\n"
                f"- Valor VAAF estimado: R$ {resultado_sim['valor_vaaf']:,.2f}\n"
                f"- Total estimado: R$ {resultado_sim['valor_total']:,.2f}\n"
            )

    # Para explicações, incluir resultado do cálculo
    if modo == "explicacao" and resultado_calculo:
        info_simulacao += (
            f"\n\nÚLTIMO CÁLCULO REALIZADO:\n"
            f"- VAAT: R$ {resultado_calculo.get('valor_vaat', 0):,.2f}\n"
            f"- VAAF: R$ {resultado_calculo.get('valor_vaaf', 0):,.2f}\n"
            f"- Total: R$ {resultado_calculo.get('valor_vaat', 0) + resultado_calculo.get('valor_vaaf', 0):,.2f}\n"
        )

    # RAG: buscar contexto legislativo
    contextos_rag = buscar_contexto(mensagem, n_results=3)
    contexto_rag_texto = formatar_contexto_para_prompt(contextos_rag)

    # Montar prompt
    system_msg = SYSTEM_PROMPT.format(
        contexto_municipio=contexto_mun + info_simulacao,
        contexto_rag=contexto_rag_texto,
    )

    # Construir mensagens
    messages = [SystemMessage(content=system_msg)]

    if historico:
        for msg in historico[-6:]:  # últimas 6 mensagens para contexto
            if msg["role"] == "user":
                messages.append(HumanMessage(content=msg["content"]))
            else:
                messages.append(AIMessage(content=msg["content"]))

    messages.append(HumanMessage(content=mensagem))

    # Chamar Claude
    llm = ChatAnthropic(
        model="claude-sonnet-4-20250514",
        api_key=api_key,
        max_tokens=1500,
        temperature=0.3,
    )

    try:
        response = llm.invoke(messages)
        return response.content
    except Exception as e:
        return f"Erro ao processar sua pergunta: {str(e)}"


def st_get_secret(key: str) -> str | None:
    """Tenta obter um secret do Streamlit."""
    try:
        import streamlit as st
        return st.secrets.get(key)
    except Exception:
        return None
