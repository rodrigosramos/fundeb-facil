"""
FUNDEB Fácil - MVP
Sistema Inteligente para Compreensão e Projeção de Complementações Orçamentárias do FUNDEB
Prêmio SOF 2025 - Categoria: Soluções em dados orçamentários
"""

import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

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
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.2rem;
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

# Funções do banco de dados
@st.cache_resource
def get_db_connection():
    """Conectar ao banco SQLite"""
    return sqlite3.connect(str(DB_PATH), check_same_thread=False)

@st.cache_data
def get_municipios():
    """Obter lista de municípios"""
    conn = get_db_connection()
    query = "SELECT codigo_ibge, nome, uf, populacao, nse, drec FROM municipios ORDER BY nome"
    df = pd.read_sql_query(query, conn)
    return df

@st.cache_data
def get_matriculas(codigo_ibge):
    """Obter matrículas de um município"""
    conn = get_db_connection()
    query = f"""
        SELECT etapa, modalidade, quantidade
        FROM matriculas
        WHERE codigo_ibge = '{codigo_ibge}'
    """
    df = pd.read_sql_query(query, conn)
    return df

@st.cache_data
def get_ponderadores():
    """Obter ponderadores VAAT/VAAF"""
    conn = get_db_connection()
    query = "SELECT * FROM ponderadores"
    df = pd.read_sql_query(query, conn)
    return df

def calcular_fator_nse(nse):
    """Calcular fator de NSE (0,95 a 1,05)"""
    if nse < 40:
        return 1.05
    elif nse < 50:
        return 1.02
    elif nse < 60:
        return 1.00
    elif nse < 70:
        return 0.98
    else:
        return 0.95

def calcular_fator_drec(drec):
    """Calcular fator de DRec (0,965 a 1,035)"""
    if drec < 0.85:
        return 1.035
    elif drec < 0.95:
        return 1.015
    elif drec < 1.05:
        return 1.000
    elif drec < 1.15:
        return 0.985
    else:
        return 0.965

def calcular_complementacoes(matriculas_df, municipio_info, ponderadores_df):
    """Calcular complementações VAAT e VAAF"""
    nse = municipio_info['nse'].values[0]
    drec = municipio_info['drec'].values[0]

    fator_nse = calcular_fator_nse(nse)
    fator_drec = calcular_fator_drec(drec)

    resultados = []
    total_matriculas_brutas = 0
    total_matriculas_ajustadas_vaat = 0
    total_matriculas_ajustadas_vaaf = 0

    for idx, row in matriculas_df.iterrows():
        etapa = row['etapa']
        modalidade = row['modalidade']
        quantidade = row['quantidade']

        # Buscar ponderadores
        pond = ponderadores_df[
            (ponderadores_df['etapa'] == etapa) &
            (ponderadores_df['modalidade'] == modalidade)
        ]

        if not pond.empty:
            pond_vaat = pond['ponderador_vaat'].values[0]
            pond_vaaf = pond['ponderador_vaaf'].values[0]

            # Aplicar todos os fatores
            matriculas_ajustadas_vaat = quantidade * pond_vaat * fator_nse * fator_drec
            matriculas_ajustadas_vaaf = quantidade * pond_vaaf * fator_nse * fator_drec

            total_matriculas_brutas += quantidade
            total_matriculas_ajustadas_vaat += matriculas_ajustadas_vaat
            total_matriculas_ajustadas_vaaf += matriculas_ajustadas_vaaf

            resultados.append({
                'Etapa': etapa,
                'Modalidade': modalidade,
                'Matrículas Brutas': quantidade,
                'Ponderador VAAT': pond_vaat,
                'Ponderador VAAF': pond_vaaf,
                'Matrículas Ajustadas VAAT': round(matriculas_ajustadas_vaat, 2),
                'Matrículas Ajustadas VAAF': round(matriculas_ajustadas_vaaf, 2)
            })

    # Estimativa de valores (simplificada para MVP)
    # VAAT: R$ 24,2 bi / ~5,9 milhões de alunos = ~R$ 4.100/aluno ajustado
    # VAAF: R$ 26,9 bi / ~6,5 milhões de alunos = ~R$ 4.138/aluno ajustado
    valor_aluno_vaat = 4100
    valor_aluno_vaaf = 4138

    valor_vaat = total_matriculas_ajustadas_vaat * valor_aluno_vaat
    valor_vaaf = total_matriculas_ajustadas_vaaf * valor_aluno_vaaf

    return {
        'resultados': pd.DataFrame(resultados),
        'total_matriculas_brutas': total_matriculas_brutas,
        'total_ajustadas_vaat': total_matriculas_ajustadas_vaat,
        'total_ajustadas_vaaf': total_matriculas_ajustadas_vaaf,
        'valor_vaat': valor_vaat,
        'valor_vaaf': valor_vaaf,
        'fatores': {
            'nse': fator_nse,
            'drec': fator_drec
        }
    }

# ========== INTERFACE PRINCIPAL ==========

st.markdown('<div class="main-header">📊 FUNDEB Fácil</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="sub-header">Sistema Inteligente para Compreensão e Projeção de Complementações Orçamentárias do FUNDEB</div>',
    unsafe_allow_html=True
)

# Sidebar - Seleção de município
st.sidebar.header("🏛️ Selecione o Município")
municipios_df = get_municipios()

municipio_opcoes = [f"{row['nome']}/{row['uf']}" for _, row in municipios_df.iterrows()]
municipio_selecionado = st.sidebar.selectbox(
    "Município:",
    options=municipio_opcoes,
    index=5  # Apucarana como padrão
)

# Extrair código do município selecionado
idx_selecionado = municipio_opcoes.index(municipio_selecionado)
municipio_info = municipios_df.iloc[idx_selecionado:idx_selecionado+1]
codigo_ibge = municipio_info['codigo_ibge'].values[0]

# Informações do município
st.sidebar.markdown("---")
st.sidebar.markdown("### 📋 Informações do Município")
st.sidebar.metric("População", f"{municipio_info['populacao'].values[0]:,}")
st.sidebar.metric("NSE (Nível Socioeconômico)", f"{municipio_info['nse'].values[0]:.1f}")
st.sidebar.metric("DRec (Disponib. Recursos)", f"{municipio_info['drec'].values[0]:.2f}")

# Sobre o projeto
st.sidebar.markdown("---")
st.sidebar.markdown("### ℹ️ Sobre")
st.sidebar.info("""
**Prêmio SOF 2025**
Categoria: Soluções em dados orçamentários
Desafio: Item 2.4.1.4 - Estimar projeções orçamentárias de médio prazo

**Autor:** Rodrigo Santos Ramos
""")

# Main content - Tabs
tab1, tab2, tab3 = st.tabs(["📊 Calculadora", "📈 Visualizações", "💬 Chat Inteligente"])

with tab1:
    st.header("🧮 Calculadora de Complementações VAAT e VAAF")

    # Carregar dados
    matriculas_df = get_matriculas(codigo_ibge)
    ponderadores_df = get_ponderadores()

    # Exibir tabela editável de matrículas
    st.subheader("📚 Matrículas por Etapa e Modalidade")
    st.info("💡 **Dica:** Edite os valores abaixo para simular cenários futuros!")

    matriculas_editavel = st.data_editor(
        matriculas_df,
        column_config={
            "etapa": st.column_config.TextColumn("Etapa", disabled=True),
            "modalidade": st.column_config.TextColumn("Modalidade", disabled=True),
            "quantidade": st.column_config.NumberColumn(
                "Quantidade de Alunos",
                min_value=0,
                step=1,
                format="%d"
            )
        },
        hide_index=True,
        use_container_width=True
    )

    # Botão calcular
    if st.button("🧮 Calcular Complementações", type="primary", use_container_width=True):
        with st.spinner("Calculando..."):
            resultado = calcular_complementacoes(matriculas_editavel, municipio_info, ponderadores_df)

            # Armazenar no session_state
            st.session_state['resultado'] = resultado

    # Exibir resultados
    if 'resultado' in st.session_state:
        resultado = st.session_state['resultado']

        st.markdown("---")
        st.subheader("💰 Valores Estimados de Complementações")

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(
                "🎯 VAAT (Valor Aluno Ano Total)",
                f"R$ {resultado['valor_vaat']:,.2f}",
                delta=f"{resultado['total_ajustadas_vaat']:,.0f} alunos ajustados"
            )

        with col2:
            st.metric(
                "🎯 VAAF (Valor Aluno Ano Final)",
                f"R$ {resultado['valor_vaaf']:,.2f}",
                delta=f"{resultado['total_ajustadas_vaaf']:,.0f} alunos ajustados"
            )

        with col3:
            total = resultado['valor_vaat'] + resultado['valor_vaaf']
            st.metric(
                "💎 Total Complementações",
                f"R$ {total:,.2f}",
                delta="Anual"
            )

        # Fatores aplicados
        st.markdown("---")
        st.subheader("⚙️ Fatores de Ajuste Aplicados")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"""
            <div class="metric-card">
                <h4>Fator NSE (Nível Socioeconômico)</h4>
                <h2>{resultado['fatores']['nse']:.3f}</h2>
                <p>NSE do município: {municipio_info['nse'].values[0]:.1f}</p>
            </div>
            """, unsafe_allow_html=True)

        with col2:
            st.markdown(f"""
            <div class="metric-card">
                <h4>Fator DRec (Disponibilidade de Recursos)</h4>
                <h2>{resultado['fatores']['drec']:.3f}</h2>
                <p>DRec do município: {municipio_info['drec'].values[0]:.2f}</p>
            </div>
            """, unsafe_allow_html=True)

        # Detalhamento por etapa
        st.markdown("---")
        st.subheader("📑 Detalhamento por Etapa/Modalidade")
        st.dataframe(
            resultado['resultados'],
            use_container_width=True,
            hide_index=True
        )

        # Export
        csv = resultado['resultados'].to_csv(index=False).encode('utf-8')
        st.download_button(
            label="⬇️ Exportar Detalhamento (CSV)",
            data=csv,
            file_name=f"fundeb_facil_{codigo_ibge}.csv",
            mime="text/csv"
        )

with tab2:
    st.header("📈 Visualizações Interativas")

    if 'resultado' in st.session_state:
        resultado = st.session_state['resultado']
        df_resultado = resultado['resultados']

        # Gráfico 1: Matrículas Brutas vs Ajustadas
        st.subheader("🔄 Impacto dos Ponderadores")

        fig1 = go.Figure()
        fig1.add_trace(go.Bar(
            name='Matrículas Brutas',
            x=df_resultado['Etapa'] + ' - ' + df_resultado['Modalidade'],
            y=df_resultado['Matrículas Brutas'],
            marker_color='lightblue'
        ))
        fig1.add_trace(go.Bar(
            name='Matrículas Ajustadas VAAT',
            x=df_resultado['Etapa'] + ' - ' + df_resultado['Modalidade'],
            y=df_resultado['Matrículas Ajustadas VAAT'],
            marker_color='darkblue'
        ))
        fig1.update_layout(
            barmode='group',
            title="Comparação: Matrículas Brutas vs Ajustadas (VAAT)",
            xaxis_tickangle=-45,
            height=500
        )
        st.plotly_chart(fig1, use_container_width=True)

        # Gráfico 2: Pizza - Composição das matrículas ajustadas
        st.subheader("🥧 Composição das Matrículas Ajustadas")

        fig2 = px.pie(
            df_resultado,
            values='Matrículas Ajustadas VAAT',
            names=df_resultado['Etapa'] + ' - ' + df_resultado['Modalidade'],
            title="Distribuição das Matrículas Ajustadas por Etapa/Modalidade (VAAT)"
        )
        st.plotly_chart(fig2, use_container_width=True)

    else:
        st.info("👈 Calcule as complementações na aba **Calculadora** para ver as visualizações!")

with tab3:
    st.header("💬 Assistente Inteligente FUNDEB")

    st.markdown("""
    <div class="info-box">
        <h3>🤖 Chat com IA - Em Desenvolvimento</h3>
        <p>O assistente conversacional com inteligência artificial está sendo desenvolvido e estará disponível em breve!</p>
        <p><strong>Funcionalidades planejadas:</strong></p>
        <ul>
            <li>✅ Explicações passo-a-passo dos cálculos</li>
            <li>✅ Consultas sobre legislação do FUNDEB</li>
            <li>✅ Simulações de cenários futuros</li>
            <li>✅ Comparações com municípios similares</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

    # FAQ básico
    st.subheader("❓ Perguntas Frequentes")

    with st.expander("O que é VAAT?"):
        st.write("""
        **VAAT (Valor Aluno Ano Total)** é uma modalidade de complementação da União ao FUNDEB que busca
        elevar os municípios com os menores valores por aluno a um patamar mínimo nacional.

        **Valor em 2025:** R$ 24,2 bilhões
        **Municípios beneficiados:** Aproximadamente 2.425 municípios
        """)

    with st.expander("O que é VAAF?"):
        st.write("""
        **VAAF (Valor Aluno Ano Final)** complementa os fundos estaduais que não atingem o valor mínimo
        nacional por aluno.

        **Valor em 2025:** R$ 26,9 bilhões
        **Beneficiários:** 10 estados e 1.849 municípios
        """)

    with st.expander("Como são aplicados os ponderadores?"):
        st.write("""
        Os ponderadores refletem os custos diferenciados de cada etapa e modalidade:

        1. **Ponderador base** (etapa × modalidade): Exemplo - Creche integral VAAT = 1,90
        2. **Fator NSE** (Nível Socioeconômico): 0,95 a 1,05
        3. **Fator DRec** (Disponibilidade de Recursos): 0,965 a 1,035
        4. **Multiplicadores especiais**: Indígena/quilombola (×1,40), Rural (×1,15)

        **Fórmula:** Matrículas Ajustadas = Matrículas × Ponderador × NSE × DRec × Multiplicadores
        """)

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666; font-size: 0.9rem;">
    <p><strong>FUNDEB Fácil</strong> - MVP para Prêmio SOF 2025</p>
    <p>Desenvolvido por Rodrigo Santos Ramos |
    <a href="https://github.com/rodrigo-ramos" target="_blank">GitHub</a></p>
</div>
""", unsafe_allow_html=True)
