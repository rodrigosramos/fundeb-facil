"""
FUNDEB Fácil
Sistema Inteligente para Compreensão e Projeção de Complementações Orçamentárias do FUNDEB
"""

import os
import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Importar agente conversacional (LangGraph)
CHAT_ERRO = None
try:
    from agents.graph import processar_mensagem
    CHAT_DISPONIVEL = True
except Exception as e:
    CHAT_DISPONIVEL = False
    CHAT_ERRO = str(e)

# Configuração da página
st.set_page_config(
    page_title="FUNDEB Fácil",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Caminho do banco de dados
DB_PATH = Path(__file__).parent / "data" / "fundeb_data.db"

# Estilo CSS customizado
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .info-box {
        background-color: #e7f3ff;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #0066cc;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

def _has_streamlit_secret(key: str) -> bool:
    """Verifica se um secret está configurado no Streamlit."""
    try:
        return bool(st.secrets.get(key))
    except Exception:
        return False


# ========== FUNÇÕES DE DADOS ==========

@st.cache_resource
def get_db_connection():
    """Conectar ao banco SQLite"""
    return sqlite3.connect(str(DB_PATH), check_same_thread=False)


@st.cache_data
def get_ufs():
    """Obter lista de UFs ordenada"""
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT DISTINCT uf FROM municipios ORDER BY uf", conn)
    return df["uf"].tolist()


@st.cache_data
def get_municipios_por_uf(uf):
    """Obter municípios de uma UF específica"""
    conn = get_db_connection()
    df = pd.read_sql_query(
        "SELECT codigo_ibge, nome, uf, populacao, fator_nse, fator_drec, vaat_por_aluno "
        "FROM municipios WHERE uf = ? ORDER BY nome",
        conn,
        params=(uf,),
    )
    return df


@st.cache_data
def get_matriculas(codigo_ibge):
    """Obter matrículas de um município"""
    conn = get_db_connection()
    df = pd.read_sql_query(
        "SELECT etapa, modalidade, quantidade FROM matriculas WHERE codigo_ibge = ?",
        conn,
        params=(codigo_ibge,),
    )
    return df


@st.cache_data
def get_ponderadores():
    """Obter ponderadores VAAT/VAAF"""
    conn = get_db_connection()
    return pd.read_sql_query("SELECT * FROM ponderadores", conn)


# ========== FUNÇÕES DE CÁLCULO ==========

def calcular_complementacoes(matriculas_df, municipio_info, ponderadores_df):
    """
    Calcular complementações VAAT e VAAF.

    Fórmula: Matrículas Ajustadas = Matrículas × Ponderador Base × Fator NSE × Fator DRec
    Valor Complementação = Matrículas Ajustadas × Valor por Aluno
    """
    fator_nse = municipio_info["fator_nse"].values[0]
    fator_drec = municipio_info["fator_drec"].values[0]
    vaat_por_aluno = municipio_info["vaat_por_aluno"].values[0]

    resultados = []
    total_matriculas_brutas = 0
    total_ajustadas_vaat = 0
    total_ajustadas_vaaf = 0

    for _, row in matriculas_df.iterrows():
        etapa = row["etapa"]
        modalidade = row["modalidade"]
        quantidade = int(row["quantidade"])

        pond = ponderadores_df[
            (ponderadores_df["etapa"] == etapa)
            & (ponderadores_df["modalidade"] == modalidade)
        ]

        if not pond.empty:
            pond_vaat = pond["ponderador_vaat"].values[0]
            pond_vaaf = pond["ponderador_vaaf"].values[0]

            ajustadas_vaat = quantidade * pond_vaat * fator_nse * fator_drec
            ajustadas_vaaf = quantidade * pond_vaaf * fator_nse * fator_drec

            total_matriculas_brutas += quantidade
            total_ajustadas_vaat += ajustadas_vaat
            total_ajustadas_vaaf += ajustadas_vaaf

            resultados.append({
                "Etapa": etapa,
                "Modalidade": modalidade,
                "Matrículas": quantidade,
                "Pond. VAAT": pond_vaat,
                "Pond. VAAF": pond_vaaf,
                "Mat. Ajust. VAAT": round(ajustadas_vaat, 1),
                "Mat. Ajust. VAAF": round(ajustadas_vaaf, 1),
            })

    # Valor médio nacional de complementação por matrícula ajustada
    # VAAT: R$ 24,2 bi ÷ ~5,9M matrículas ajustadas ≈ R$ 4.100/matrícula
    # VAAF: R$ 26,9 bi ÷ ~6,5M matrículas ajustadas ≈ R$ 4.138/matrícula
    # Nota: o campo vaat_por_aluno do FNDE é o valor-aluno-ano total (não complementação)
    val_vaat = 4100
    val_vaaf = 4138

    valor_vaat = total_ajustadas_vaat * val_vaat
    valor_vaaf = total_ajustadas_vaaf * val_vaaf

    return {
        "resultados": pd.DataFrame(resultados),
        "total_matriculas_brutas": total_matriculas_brutas,
        "total_ajustadas_vaat": total_ajustadas_vaat,
        "total_ajustadas_vaaf": total_ajustadas_vaaf,
        "valor_vaat": valor_vaat,
        "valor_vaaf": valor_vaaf,
        "vaat_por_aluno": val_vaat,
        "fatores": {"nse": fator_nse, "drec": fator_drec},
    }


# ========== INTERFACE PRINCIPAL ==========

st.markdown('<div class="main-header">FUNDEB Fácil</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="sub-header">Sistema Inteligente para Compreensão e Projeção de '
    "Complementações Orçamentárias do FUNDEB</div>",
    unsafe_allow_html=True,
)

# ========== SIDEBAR - FILTRO CASCATA UF + MUNICÍPIO ==========

st.sidebar.header("Selecione o Município")

ufs = get_ufs()
uf_selecionada = st.sidebar.selectbox(
    "1. Estado (UF)",
    options=ufs,
    index=ufs.index("SP") if "SP" in ufs else 0,
    help="Selecione o estado primeiro",
)

municipios_uf = get_municipios_por_uf(uf_selecionada)
municipio_nomes = municipios_uf["nome"].tolist()
municipio_selecionado = st.sidebar.selectbox(
    f"2. Município ({uf_selecionada})",
    options=municipio_nomes,
    help=f"{len(municipio_nomes)} municípios em {uf_selecionada}",
)

# Obter dados do município selecionado
idx_sel = municipio_nomes.index(municipio_selecionado)
municipio_info = municipios_uf.iloc[idx_sel : idx_sel + 1]
codigo_ibge = municipio_info["codigo_ibge"].values[0]

# Informações na sidebar
st.sidebar.markdown("---")
st.sidebar.markdown("### Dados do Município")
col_a, col_b = st.sidebar.columns(2)
col_a.metric("Fator NSE", f"{municipio_info['fator_nse'].values[0]:.4f}")
col_b.metric("Fator DRec", f"{municipio_info['fator_drec'].values[0]:.4f}")

vaat = municipio_info["vaat_por_aluno"].values[0]
if vaat > 0:
    st.sidebar.metric("VAAT/Aluno (FNDE)", f"R$ {vaat:,.2f}")
else:
    st.sidebar.caption("Município sem VAAT estimado pelo FNDE")

st.sidebar.markdown("---")
st.sidebar.markdown("### Sobre")
st.sidebar.info(
    "**FUNDEB Fácil** — Dissertação MPPL/Cefor\n\n"
    "Câmara dos Deputados\n\n"
    "Autor: Rodrigo Santos Ramos"
)

# ========== CONTEÚDO PRINCIPAL - 4 ABAS ==========

tab1, tab2, tab3, tab4 = st.tabs(
    ["Calculadora", "Visualizações", "Assistente IA", "FAQ Educativo"]
)

# ---- TAB 1: CALCULADORA ----
with tab1:
    st.header("Calculadora de Complementações VAAT e VAAF")

    matriculas_df = get_matriculas(codigo_ibge)
    ponderadores_df = get_ponderadores()

    if matriculas_df.empty:
        st.warning(
            f"O município **{municipio_selecionado}/{uf_selecionada}** não possui "
            "matrículas na rede municipal registradas na Sinopse do Censo Escolar 2024."
        )
    else:
        st.subheader("Matrículas por Etapa e Modalidade")
        st.info("Edite os valores abaixo para simular cenários futuros.")

        matriculas_editavel = st.data_editor(
            matriculas_df,
            column_config={
                "etapa": st.column_config.TextColumn("Etapa", disabled=True),
                "modalidade": st.column_config.TextColumn("Modalidade", disabled=True),
                "quantidade": st.column_config.NumberColumn(
                    "Quantidade de Alunos", min_value=0, step=1, format="%d"
                ),
            },
            hide_index=True,
            use_container_width=True,
        )

        if st.button("Calcular Complementações", type="primary", use_container_width=True):
            with st.spinner("Calculando..."):
                resultado = calcular_complementacoes(
                    matriculas_editavel, municipio_info, ponderadores_df
                )
                st.session_state["resultado"] = resultado
                st.session_state["municipio_resultado"] = f"{municipio_selecionado}/{uf_selecionada}"

        if "resultado" in st.session_state:
            resultado = st.session_state["resultado"]

            st.markdown("---")
            st.subheader("Valores Estimados de Complementações")

            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric(
                    "VAAT (Valor Aluno Ano Total)",
                    f"R$ {resultado['valor_vaat']:,.2f}",
                    delta=f"{resultado['total_ajustadas_vaat']:,.0f} matrículas ajustadas",
                )

            with col2:
                st.metric(
                    "VAAF (Valor Aluno Ano Final)",
                    f"R$ {resultado['valor_vaaf']:,.2f}",
                    delta=f"{resultado['total_ajustadas_vaaf']:,.0f} matrículas ajustadas",
                )

            with col3:
                total = resultado["valor_vaat"] + resultado["valor_vaaf"]
                st.metric("Total Complementações", f"R$ {total:,.2f}", delta="Estimativa anual")

            # Nível 2: Fatores aplicados
            st.markdown("---")
            st.subheader("Fatores de Ajuste Aplicados")

            col1, col2, col3 = st.columns(3)
            with col1:
                st.markdown(
                    f'<div class="metric-card">'
                    f"<h4>Fator NSE</h4>"
                    f"<h2>{resultado['fatores']['nse']:.4f}</h2>"
                    f"<p>Nível Socioeconômico</p>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

            with col2:
                st.markdown(
                    f'<div class="metric-card">'
                    f"<h4>Fator DRec</h4>"
                    f"<h2>{resultado['fatores']['drec']:.4f}</h2>"
                    f"<p>Disponibilidade de Recursos</p>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

            with col3:
                st.markdown(
                    f'<div class="metric-card">'
                    f"<h4>VAAT/Aluno</h4>"
                    f"<h2>R$ {resultado['vaat_por_aluno']:,.2f}</h2>"
                    f"<p>Valor FNDE por matrícula ajustada</p>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

            # Nível 3: Detalhamento
            st.markdown("---")
            st.subheader("Detalhamento por Etapa/Modalidade")
            st.dataframe(resultado["resultados"], use_container_width=True, hide_index=True)

            csv = resultado["resultados"].to_csv(index=False).encode("utf-8")
            st.download_button(
                label="Exportar Detalhamento (CSV)",
                data=csv,
                file_name=f"fundeb_facil_{codigo_ibge}.csv",
                mime="text/csv",
            )

# ---- TAB 2: VISUALIZAÇÕES ----
with tab2:
    st.header("Visualizações Interativas")

    if "resultado" in st.session_state:
        resultado = st.session_state["resultado"]
        df_res = resultado["resultados"]
        mun_label = st.session_state.get("municipio_resultado", "")

        # Gráfico 1: Barras agrupadas
        st.subheader("Impacto dos Ponderadores nas Matrículas")

        df_bar = df_res.copy()
        df_bar["Categoria"] = df_bar["Etapa"] + " - " + df_bar["Modalidade"]

        fig1 = go.Figure()
        fig1.add_trace(
            go.Bar(
                name="Matrículas Brutas",
                x=df_bar["Categoria"],
                y=df_bar["Matrículas"],
                marker_color="#90CAF9",
            )
        )
        fig1.add_trace(
            go.Bar(
                name="Matrículas Ajustadas VAAT",
                x=df_bar["Categoria"],
                y=df_bar["Mat. Ajust. VAAT"],
                marker_color="#1565C0",
            )
        )
        fig1.update_layout(
            barmode="group",
            title=f"Matrículas Brutas vs Ajustadas — {mun_label}",
            xaxis_tickangle=-45,
            height=500,
            legend=dict(orientation="h", y=-0.25),
        )
        st.plotly_chart(fig1, use_container_width=True)

        # Gráfico 2: Pizza
        st.subheader("Composição das Matrículas Ajustadas")

        fig2 = px.pie(
            df_bar,
            values="Mat. Ajust. VAAT",
            names="Categoria",
            title=f"Distribuição por Etapa/Modalidade (VAAT) — {mun_label}",
        )
        fig2.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("Calcule as complementações na aba **Calculadora** para ver as visualizações.")

# ---- TAB 3: ASSISTENTE IA ----
with tab3:
    st.header("Assistente Inteligente FUNDEB")

    st.markdown(
        '<div class="info-box">'
        "<h4>Assistente Conversacional com IA</h4>"
        "<p>Tire dúvidas sobre o FUNDEB, peça explicações dos cálculos ou simule "
        f"cenários para <strong>{municipio_selecionado}/{uf_selecionada}</strong>.</p>"
        "</div>",
        unsafe_allow_html=True,
    )

    # Sugestões rápidas
    st.caption("Sugestões: \"Explique o cálculo do VAAT\" · \"Simule expandir creches em 30%\" · \"O que diz a lei sobre ponderadores?\"")

    if "messages" not in st.session_state:
        st.session_state.messages = []

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    if prompt := st.chat_input("Digite sua dúvida sobre o FUNDEB..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            api_key = os.getenv("ANTHROPIC_API_KEY") or _has_streamlit_secret("ANTHROPIC_API_KEY")
            if not CHAT_DISPONIVEL:
                resposta = (
                    "O módulo de IA não pôde ser carregado.\n\n"
                    f"**Erro:** `{CHAT_ERRO}`\n\n"
                    "Isso geralmente ocorre por falta de dependência no servidor. "
                    "Consulte a aba **FAQ Educativo** para informações detalhadas."
                )
            elif not api_key:
                resposta = (
                    "O assistente IA requer a chave `ANTHROPIC_API_KEY` para funcionar.\n\n"
                    "Configure nos **Secrets** do Streamlit Cloud (Settings > Secrets):\n\n"
                    '```toml\nANTHROPIC_API_KEY = "sk-ant-sua-chave-aqui"\n```\n\n'
                    "Enquanto isso, consulte a aba **FAQ Educativo** para informações detalhadas."
                )
            else:
                with st.spinner("Processando com IA..."):
                    try:
                        resultado_calc = st.session_state.get("resultado")
                        resposta = processar_mensagem(
                            mensagem=prompt,
                            codigo_ibge=codigo_ibge,
                            historico=st.session_state.messages[:-1],
                            resultado_calculo=resultado_calc,
                        )
                    except Exception as e:
                        resposta = f"Erro ao processar mensagem: `{e}`"
            st.markdown(resposta)
            st.session_state.messages.append({"role": "assistant", "content": resposta})

# ---- TAB 4: FAQ EDUCATIVO ----
with tab4:
    st.header("FAQ Educativo — Entendendo o FUNDEB")

    with st.expander("O que é o FUNDEB?", expanded=True):
        st.write("""
        O **FUNDEB** (Fundo de Manutenção e Desenvolvimento da Educação Básica) é o principal
        mecanismo de financiamento da educação básica no Brasil. Criado pela EC nº 108/2020 e
        regulamentado pela Lei nº 14.113/2020, o Novo FUNDEB tornou-se **permanente** e ampliou
        a participação da União.

        **Orçamento 2025:** R$ 339 bilhões (total), dos quais R$ 57,5 bilhões são complementações da União.
        """)

    with st.expander("O que são VAAT e VAAF?"):
        st.write("""
        São as duas principais modalidades de complementação da União ao FUNDEB:

        **VAAT (Valor Aluno Ano Total)** — R$ 24,2 bilhões em 2025
        - Objetivo: elevar redes municipais com menores recursos a um patamar mínimo
        - Beneficia diretamente cada rede municipal elegível
        - Critério: valor por aluno total (incluindo recursos próprios) abaixo do limite

        **VAAF (Valor Aluno Ano Final)** — R$ 26,9 bilhões em 2025
        - Objetivo: complementar fundos estaduais que ficam abaixo do mínimo nacional
        - Beneficia estados e municípios do fundo que não atinge o valor mínimo
        - Critério: valor por aluno do fundo estadual abaixo do limite

        Juntos, VAAT e VAAF representam **89% das complementações** e são as modalidades mais
        previsíveis para projeção de médio prazo.
        """)

    with st.expander("O que são ponderadores e como funcionam?"):
        st.write("""
        Os **ponderadores** são fatores definidos pela Resolução CIF nº 5/2024 que refletem os custos
        diferenciados de cada etapa e modalidade de ensino:

        | Etapa/Modalidade | Ponderador VAAT | Ponderador VAAF |
        |---|---|---|
        | Creche Integral | 1,90 | 1,55 |
        | Creche Parcial | 1,50 | 1,30 |
        | Pré-escola Integral | 1,88 | 1,50 |
        | Pré-escola Parcial | 1,40 | 1,25 |
        | EF Anos Iniciais Integral | 1,45 | 1,30 |
        | EF Anos Iniciais Parcial Urbano | 1,00 | 1,00 |
        | EF Anos Iniciais Parcial Rural | 1,20 | 1,15 |
        | EF Anos Finais Integral | 1,40 | 1,25 |
        | EF Anos Finais Parcial Urbano | 1,15 | 1,10 |
        | EF Anos Finais Parcial Rural | 1,30 | 1,20 |
        | Ensino Médio Integral | 1,55 | 1,40 |
        | Ensino Médio Parcial | 1,30 | 1,25 |
        | EJA Fundamental | 1,05 | 0,95 |
        | EJA Médio | 1,20 | 1,10 |

        **Referência base:** EF Anos Iniciais Parcial Urbano = 1,00 (VAAT)
        """)

    with st.expander("O que são NSE e DRec?"):
        st.write("""
        **NSE (Nível Socioeconômico)** — Fator: 0,95 a 1,05
        - Calculado pelo INEP com base em dados do Censo Escolar
        - Considera renda familiar, escolaridade dos pais, acesso a bens e serviços
        - Municípios com NSE mais baixo recebem fator maior (até 1,05)
        - Referência: Nota Técnica INEP nº 11/2024

        **DRec (Disponibilidade de Recursos)** — Fator: 0,965 a 1,035
        - Calculado pelo INEP com base em dados fiscais e tributários
        - Mede a capacidade do município de financiar educação com recursos próprios
        - Municípios com menor capacidade fiscal recebem fator maior
        - Referência: Nota Técnica INEP nº 11/2024

        **Na fórmula:** Matrículas Ajustadas = Matrículas × Ponderador × Fator NSE × Fator DRec
        """)

    with st.expander("Como interpretar os resultados da calculadora?"):
        st.write("""
        A calculadora apresenta os resultados em **3 níveis**:

        **Nível 1 — Valores Estimados:**
        - Complementação VAAT estimada (em R$)
        - Complementação VAAF estimada (em R$)
        - Total de complementações

        **Nível 2 — Fatores de Ajuste:**
        - Fator NSE do município (dados reais do INEP/FNDE)
        - Fator DRec do município (dados reais do INEP/FNDE)
        - VAAT por aluno (valor específico do município conforme FNDE)

        **Nível 3 — Detalhamento:**
        - Tabela com matrículas brutas e ajustadas por etapa/modalidade
        - Ponderadores aplicados em cada categoria
        - Exportação em CSV para análise externa
        """)

    with st.expander("Quais dados são utilizados?"):
        st.write("""
        O FUNDEB Fácil utiliza **dados oficiais** das seguintes fontes:

        - **Sinopse do Censo Escolar 2024** (INEP) — matrículas por município, etapa e modalidade
        - **Planilha FNDE** — fatores NSE e DRec por município, valor VAAT por aluno
        - **Resolução CIF nº 5/2024** — ponderadores oficiais por etapa/modalidade

        **Cobertura:** 5.570 municípios brasileiros, ~23 milhões de matrículas na rede municipal
        """)

    with st.expander("Como simular cenários futuros?"):
        st.write("""
        Na aba **Calculadora**, você pode editar os valores de matrículas para simular cenários:

        **Exemplos de simulação:**
        1. **Expansão de creches:** Aumente as matrículas de Creche Integral para ver o impacto
        2. **Universalização da pré-escola:** Ajuste as matrículas de Pré-escola
        3. **Expansão do tempo integral:** Mova matrículas de Parcial para Integral
        4. **Crescimento de EJA:** Projete aumento em Educação de Jovens e Adultos

        O sistema recalcula automaticamente as complementações com base nos novos valores.
        """)


# Footer
st.markdown("---")
st.markdown(
    '<div style="text-align: center; color: #666; font-size: 0.9rem;">'
    "<p><strong>FUNDEB Fácil</strong> — Sistema Inteligente para Compreensão e Projeção "
    "de Complementações Orçamentárias do FUNDEB</p>"
    "<p>Dissertação MPPL/Cefor — Câmara dos Deputados</p>"
    "</div>",
    unsafe_allow_html=True,
)
