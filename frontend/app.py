import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import os

st.set_page_config(page_title="Financial AI Agent", layout="wide")
API_URL = os.getenv("API_URL", "http://api:8000")

st.title("Financial AI Agent Dashboard")

with st.sidebar:
    st.header("System Check")
    if st.button("Check API Status"):
        try:
            r = requests.get(f"{API_URL}/health")
            if r.status_code == 200:
                st.success("API Online")
            else:
                st.error(f"API Error: {r.status_code}")
        except:
            st.error("API Offline")

query = st.text_input(
    "Ask about your financial news (e.g. 'What are the latest Bitcoin news?')"
)
if query:
    with st.spinner("Consulting gemini..."):
        try:
            response = requests.get(f"{API_URL}/search", params={"query": query})

            if response.status_code == 200:
                data = response.json()

                st.markdown("### Gemini's Response")
                st.write(data["response"])

                st.markdown("---")
                st.subheader("Sources, Metadata and Sentiment Analysis (FinBERT)")

                sources = data.get("sources", [])

                for i, source in enumerate(sources):
                    sentiment = source.get("sentiment", "neutral").lower()
                    score = source.get("sentiment_score", 0.0)
                    if sentiment == "positive":
                        sentiment_label = f"Positive ({score:.2f})"
                        sentiment_color = "green"
                    elif sentiment == "negative":
                        sentiment_label = f"Negative ({score:.2f})"
                        sentiment_color = "red"
                    else:
                        sentiment_label = f"Neutral ({score:.2f})"
                        sentiment_color = "gray"
                        
                        
                    with st.expander(f"Source {i + 1} | Sentiment: {sentiment_label}", expanded=False):
                        st.markdown(f"**Score de Confianza:** `{score:.2f}`")
                        st.info(source.get("text", ""))

            else:
                st.error(f"Error from API: {response.text}")

        except Exception as e:
            st.error(f"Error processing API response: {e}")
