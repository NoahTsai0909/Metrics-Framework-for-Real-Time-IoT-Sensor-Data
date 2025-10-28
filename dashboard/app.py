import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

#DATABASE CONNECTION
user = "postgres"
password = "12345Yh890909'."
host = "localhost"
port = 5432
database = "sensor_data"

engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

#STREAMLIT PAGE CONFIG
st.set_page_config(page_title="IoT Sensor Dashboard", layout="wide")

st.title("📊 IoT Sensor Data Dashboard")

#LOAD DATA
@st.cache_data(ttl=60)
def load_data():
    query = "SELECT * FROM sensor_readings ORDER BY timestamp DESC LIMIT 500;"
    df = pd.read_sql(query, engine)
    return df

df = load_data()

if df.empty:
    st.warning("No data found in the database yet.")
    st.stop()

#METRICS SECTION
col1, col2, col3 = st.columns(3)
col1.metric("Average Temperature (°C)", f"{df['temperature'].mean():.2f}")
col2.metric("Average Humidity (%)", f"{df['humidity'].mean():.2f}")
col3.metric("Latest Reading Time", df['timestamp'].max())

st.divider()

#TIME SERIES CHART
st.subheader("📈 Temperature and Humidity Over Time")

fig = px.line(df, x="timestamp", y=["temperature", "humidity"], title="Sensor Trends")
st.plotly_chart(fig, use_container_width=True)

#TABLE
st.subheader("🧾 Recent Sensor Readings")
st.dataframe(df.head(20))
