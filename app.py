import streamlit as st
from src.views.screener import render_screener

st.set_page_config(page_title="vDelta Screener", layout="wide")
st.sidebar.title("vDelta")
PAGES = {"Screener": render_screener}
PAGES[st.sidebar.selectbox("Pages", list(PAGES.keys()))]()
