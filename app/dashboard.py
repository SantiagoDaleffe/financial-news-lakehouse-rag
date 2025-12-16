import streamlit as st
import chromadb
from chromadb.utils import embedding_functions
import pandas as pd
import os
import plotly.express as px
import plotly.graph_objects as go
import yfinance as yf
from datetime import datetime, timedelta

# Configuración
st.set_page_config(page_title="Financial Intelligence AI", layout="wide", page_icon="🧠")

@st.cache_resource
def get_chroma_client():
    host = os.getenv("CHROMA_HOST", "chromadb")
    port = os.getenv("CHROMA_PORT", "8000")
    return chromadb.HttpClient(host=host, port=int(port))

# --- FUNCIONES AUXILIARES ---
def get_crypto_price(ticker="BTC-USD"):
    """Baja precio actual y cambio 24h de Yahoo Finance"""
    try:
        data = yf.Ticker(ticker)
        hist = data.history(period="2d")
        if len(hist) >= 1:
            current = hist['Close'].iloc[-1]
            prev = hist['Close'].iloc[0] if len(hist) > 1 else current
            delta = ((current - prev) / prev) * 100
            return current, delta
    except:
        return 0.0, 0.0
    return 0.0, 0.0

# --- INICIALIZACIÓN ---
try:
    client = get_chroma_client()
    ef = embedding_functions.SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")
    collection = client.get_collection(name="financial_news", embedding_function=ef)
    db_status = "🟢 Online"
except Exception as e:
    collection = None
    db_status = f"🔴 Offline"

# --- SIDEBAR ---
with st.sidebar:
    st.title("🎛️ Panel de Control")
    st.write(f"Estado Vector DB: {db_status}")
    
    # Selector de activo para precio
    ticker = st.selectbox("Activo de Referencia", ["BTC-USD", "ETH-USD", "SOL-USD", "^GSPC"])
    
    # Precio en vivo
    price, delta = get_crypto_price(ticker)
    st.metric(label=f"Precio {ticker}", value=f"${price:,.2f}", delta=f"{delta:.2f}%")
    
    st.divider()
    if collection:
        st.caption(f"Noticias indexadas: {collection.count()}")

# --- HEADER ---
st.title("🧠 Financial News Intelligence")
st.markdown("Plataforma de análisis de sentimiento y búsqueda semántica para mercados cripto/financieros.")

# --- BÚSQUEDA ---
col1, col2 = st.columns([3, 1])
with col1:
    query = st.text_input("🔍 Búsqueda Semántica", placeholder="Ej: Regulatory crackdown on stablecoins...")
with col2:
    sentiment_filter = st.multiselect("Filtrar Sentimiento", ["positive", "negative", "neutral"], default=["positive", "negative", "neutral"])

if query and collection:
    with st.spinner("Analizando vectores y sentimiento..."):
        # Query a Chroma con filtrado por metadatos (where)
        # Nota: Chroma 'where' filter es limitado, filtramos en Python post-query para flexibilidad
        results = collection.query(
            query_texts=[query],
            n_results=20, # Traemos más para filtrar después
            include=["documents", "metadatas", "distances"]
        )
        
        items = []
        for i in range(len(results['ids'][0])):
            meta = results['metadatas'][0][i]
            doc = results['documents'][0][i]
            score = 1 - results['distances'][0][i]
            
            # Verificar si tiene sentimiento (por si hay datos viejos)
            sentiment = meta.get('sentiment', 'neutral')
            
            if sentiment in sentiment_filter:
                items.append({
                    "Fecha": meta.get('published_at', 'N/A'),
                    "Fuente": meta.get('source', 'Unknown'),
                    "Título": meta.get('title', 'Sin título'),
                    "Sentimiento": sentiment,
                    "Confianza": meta.get('sentiment_score', 0.0),
                    "Relevancia": score,
                    "URL": meta.get('url', '#'),
                    "Resumen": doc[:200] + "..."
                })
        
        df = pd.DataFrame(items)
        
        if not df.empty:
            # --- KPI ROW ---
            kpi1, kpi2, kpi3 = st.columns(3)
            avg_sentiment = df[df['Sentimiento'] == 'positive'].shape[0] / len(df) * 100
            
            kpi1.metric("Noticias Relevantes", len(df))
            kpi2.metric("Dominancia Positiva", f"{avg_sentiment:.1f}%")
            
            top_source = df['Fuente'].mode()[0] if not df.empty else "N/A"
            kpi3.metric("Fuente Principal", top_source)

            # --- GRÁFICOS ---
            st.subheader("📊 Análisis de Mercado")
            row1_col1, row1_col2 = st.columns(2)
            
            with row1_col1:
                # Torta de Sentimiento
                fig_pie = px.pie(df, names='Sentimiento', title='Distribución de Sentimiento', 
                                 color='Sentimiento',
                                 color_discrete_map={'positive':'#00cc96', 'negative':'#ef553b', 'neutral':'#636efa'})
                st.plotly_chart(fig_pie, use_container_width=True)
            
            with row1_col2:
                # Barras de Fuente y Sentimiento
                fig_bar = px.bar(df, x='Fuente', color='Sentimiento', title='Sentimiento por Fuente',
                                 color_discrete_map={'positive':'#00cc96', 'negative':'#ef553b', 'neutral':'#636efa'})
                st.plotly_chart(fig_bar, use_container_width=True)

            # --- LISTADO ---
            st.subheader("📰 Noticias Detalladas")
            for idx, row in df.iterrows():
                color = "green" if row['Sentimiento'] == 'positive' else "red" if row['Sentimiento'] == 'negative' else "grey"
                emoji = "🚀" if row['Sentimiento'] == 'positive' else "🔻" if row['Sentimiento'] == 'negative' else "😐"
                
                with st.expander(f"{emoji} {row['Título']} | {row['Fuente']}"):
                    st.markdown(f"**Relevancia:** {row['Relevancia']:.2f} | **Confianza Modelo:** {row['Confianza']:.2f}")
                    st.caption(f"Publicado: {row['Fecha']}")
                    st.write(row['Resumen'])
                    st.markdown(f"[:link: Leer nota original]({row['URL']})")
        else:
            st.warning("No se encontraron noticias con esos filtros.")

elif not collection:
    st.error("Error crítico: No hay conexión con la base de datos.")
else:
    st.info("Esperando consulta...")