import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

st.set_page_config(page_title="Data Road BR", layout="wide")

st.title("📊 Painel de Frota e Acidentes (TCC)")

data_path = Path("data/processed/base_analitica.csv")
if not data_path.exists():
    st.warning("⚠️ Base analítica não encontrada. Rode o script de transformação primeiro.")
else:
    df = pd.read_csv(data_path)

    st.sidebar.header("Filtros")
    ano = st.sidebar.selectbox("Ano", sorted(df["ano"].unique()))
    df_filtro = df[df["ano"] == ano]

    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(df_filtro.groupby("uf")["frota_per_capita"].mean().reset_index(),
                     x="uf", y="frota_per_capita", title="Frota per capita por UF")
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig2 = px.scatter(df_filtro, x="populacao", y="total_veiculos",
                          title="Relação População x Frota", color="uf")
        st.plotly_chart(fig2, use_container_width=True)
