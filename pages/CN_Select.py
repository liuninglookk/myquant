

import streamlit as st

st.markdown("# Page 3 🎉")
st.sidebar.markdown("# CN Stock Panel 🎉")

days_rng = st.sidebar.number_input('Recent days', min_value=5, step=1)
vol_low_pct = st.sidebar.number_input('Volume(%) increase low limit', min_value=5.0, step=0.5)
price_low_pct = st.sidebar.number_input('Price(%) increase low limit', min_value=1.1, step=0.1)
vol_low_rate = st.sidebar.number_input('Volume increase rate(%)', min_value=40, step=5)
price_low_rate = st.sidebar.number_input('Price increase rate(%)', min_value=40, step=5)