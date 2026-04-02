"""
Orquestração LangGraph — StateGraph com 2 agentes.

Grafo:
  entrada → router → calculadora (determinístico)
                   → conversacional (LLM + RAG)

O router decide o caminho com base na mensagem do usuário.
Para simulações: router → calculadora → conversacional (explica o resultado).
Para perguntas: router → conversacional (com ou sem RAG).
"""

from typing import TypedDict, Literal
from langgraph.graph import StateGraph, END

from agents.calculator import calcular_complementacao, simular_cenario
from agents.conversational import gerar_resposta, _detectar_modo


class FundebState(TypedDict):
    """Estado compartilhado do grafo."""
    mensagem: str                   # Mensagem do usuário
    codigo_ibge: str                # Município selecionado
    historico: list[dict]           # Histórico de mensagens
    resultado_calculo: dict | None  # Último resultado de cálculo do Streamlit
    modo: str                       # explicacao | simulacao | consulta
    resultado_simulacao: dict | None  # Resultado de simulação (se aplicável)
    resposta: str                   # Resposta final


def router_node(state: FundebState) -> FundebState:
    """Detecta o modo da pergunta e roteia para o agente correto."""
    modo = _detectar_modo(state["mensagem"])
    return {**state, "modo": modo}


def calculadora_node(state: FundebState) -> FundebState:
    """Executa cálculos determinísticos para simulações."""
    if state["modo"] != "simulacao":
        return state

    resultado = simular_cenario(state["codigo_ibge"], state["mensagem"])
    return {**state, "resultado_simulacao": resultado}


def conversacional_node(state: FundebState) -> FundebState:
    """Gera resposta usando Claude + RAG + contexto."""
    resposta = gerar_resposta(
        mensagem=state["mensagem"],
        codigo_ibge=state["codigo_ibge"],
        historico=state.get("historico"),
        resultado_calculo=state.get("resultado_calculo") or state.get("resultado_simulacao"),
    )
    return {**state, "resposta": resposta}


def should_calculate(state: FundebState) -> Literal["calculadora", "conversacional"]:
    """Decide se precisa passar pela calculadora primeiro."""
    if state["modo"] == "simulacao":
        return "calculadora"
    return "conversacional"


def build_graph() -> StateGraph:
    """Constrói o grafo LangGraph."""
    graph = StateGraph(FundebState)

    graph.add_node("router", router_node)
    graph.add_node("calculadora", calculadora_node)
    graph.add_node("conversacional", conversacional_node)

    graph.set_entry_point("router")
    graph.add_conditional_edges("router", should_calculate)
    graph.add_edge("calculadora", "conversacional")
    graph.add_edge("conversacional", END)

    return graph.compile()


# Instância global do grafo compilado
fundeb_graph = build_graph()


def processar_mensagem(
    mensagem: str,
    codigo_ibge: str,
    historico: list[dict] | None = None,
    resultado_calculo: dict | None = None,
) -> str:
    """
    Interface principal para processar mensagens do chat.

    Args:
        mensagem: Pergunta do usuário
        codigo_ibge: Código IBGE do município
        historico: Histórico de mensagens
        resultado_calculo: Último resultado de cálculo

    Returns:
        Resposta do assistente
    """
    state = FundebState(
        mensagem=mensagem,
        codigo_ibge=codigo_ibge,
        historico=historico or [],
        resultado_calculo=resultado_calculo,
        modo="",
        resultado_simulacao=None,
        resposta="",
    )

    result = fundeb_graph.invoke(state)
    return result["resposta"]
