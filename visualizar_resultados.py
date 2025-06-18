import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go

# Configuração da página
st.set_page_config(
    page_title="Análise de Importadores",
    page_icon="📊",
    layout="wide"
)

# Título
st.title("Análise de Possíveis Importadores")

# Conexão com o banco
@st.cache_data
def carregar_dados():
    conn = sqlite3.connect('cnpj.db')
    df = pd.read_sql("""
        SELECT 
            i.*,
            e.RAZAO_SOCIAL,
            e.NATUREZA_JURIDICA,
            e.PORTE_EMPRESA,
            e.CAPITAL_SOCIAL,
            e.UF,
            e.MUNICIPIO
        FROM possiveis_importadores_ml_enriquecido i
        LEFT JOIN Empresas e ON i.CNPJ_ML_NORMALIZADO = e.CNPJ_BASICO || e.CNPJ_ORDEM || e.CNPJ_DV
    """, conn)
    conn.close()
    return df

# Carrega dados
try:
    df = carregar_dados()
    
    # Sidebar com filtros
    st.sidebar.header("Filtros")
    
    # Filtro por UF
    ufs = ['Todos'] + sorted(df['UF'].unique().tolist())
    uf_selecionada = st.sidebar.selectbox('UF', ufs)
    
    # Filtro por Porte
    portes = ['Todos'] + sorted(df['PORTE_EMPRESA'].unique().tolist())
    porte_selecionado = st.sidebar.selectbox('Porte da Empresa', portes)
    
    # Aplica filtros
    if uf_selecionada != 'Todos':
        df = df[df['UF'] == uf_selecionada]
    if porte_selecionado != 'Todos':
        df = df[df['PORTE_EMPRESA'] == porte_selecionado]
    
    # Métricas principais
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total de Registros", len(df))
    with col2:
        st.metric("Empresas Únicas", df['CNPJ_ML_NORMALIZADO'].nunique())
    with col3:
        st.metric("Valor Total FOB", f"R$ {df['VALOR FOB ESTIMADO TOTAL'].sum():,.2f}")
    
    # Gráficos
    st.subheader("Distribuição por UF")
    uf_counts = df['UF'].value_counts()
    fig_uf = px.bar(
        x=uf_counts.index,
        y=uf_counts.values,
        title="Quantidade de Importações por UF"
    )
    st.plotly_chart(fig_uf, use_container_width=True)
    
    # Top 10 Importadores
    st.subheader("Top 10 Importadores")
    top_importadores = df.groupby('RAZAO_SOCIAL').agg({
        'VALOR FOB ESTIMADO TOTAL': 'sum',
        'CNPJ_ML_NORMALIZADO': 'count'
    }).sort_values('VALOR FOB ESTIMADO TOTAL', ascending=False).head(10)
    
    fig_top = go.Figure(data=[
        go.Bar(
            x=top_importadores.index,
            y=top_importadores['VALOR FOB ESTIMADO TOTAL'],
            name='Valor FOB'
        )
    ])
    fig_top.update_layout(title="Top 10 Importadores por Valor FOB")
    st.plotly_chart(fig_top, use_container_width=True)
    
    # Tabela de dados
    st.subheader("Dados Detalhados")
    st.dataframe(
        df[[
            'CNPJ_ML_NORMALIZADO', 'RAZAO_SOCIAL', 'UF', 'MUNICIPIO',
            'PORTE_EMPRESA', 'VALOR FOB ESTIMADO TOTAL', 'NCM',
            'Descrição produto', 'ORIGEM_DADOS_CADASTRAIS'
        ]].sort_values('VALOR FOB ESTIMADO TOTAL', ascending=False),
        use_container_width=True
    )
    
    # Download dos dados
    st.download_button(
        "Download CSV",
        df.to_csv(index=False).encode('utf-8'),
        "importadores_analise.csv",
        "text/csv",
        key='download-csv'
    )

except Exception as e:
    st.error(f"Erro ao carregar dados: {str(e)}")
    st.info("Verifique se o banco de dados está acessível e se a tabela 'possiveis_importadores_ml_enriquecido' existe.") 