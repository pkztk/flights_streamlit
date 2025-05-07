import streamlit as st
import pandas as pd
import duckdb
import requests
from datetime import datetime

# Load data from OpenSky
@st.cache_data(ttl=300)
def fetch_opensky():
    try:
        response = requests.get("https://opensky-network.org/api/states/all")
        data = response.json()
        columns = [
            "icao24", "callsign", "origin_country", "time_position", "last_contact",
            "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
            "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
            "spi", "position_source"
        ]
        df = pd.DataFrame(data["states"], columns=columns)
        df = df.dropna(subset=["latitude", "longitude"])  # Drop missing coordinates
        df["timestamp"] = datetime.utcnow()
        return df
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()

# Store data
def store_in_duckdb(df):
    con = duckdb.connect("flights.duckdb")
    con.execute("CREATE TABLE IF NOT EXISTS flights AS SELECT * FROM df LIMIT 0")
    con.execute("INSERT INTO flights SELECT * FROM df")

# Title
st.title("Real-Time Flight Tracker (OpenSky + DuckDB)")

# Fetch button
if st.button("Fetch Live Flight Data"):
    df = fetch_opensky()
    if not df.empty:
        store_in_duckdb(df)
        st.success(f"Fetched and stored {len(df)} records.")

# Load recent data for display
con = duckdb.connect("flights.duckdb")
recent_df = con.execute("SELECT * FROM flights ORDER BY timestamp DESC LIMIT 500").fetchdf()

if not recent_df.empty:
    st.subheader("Filter Flights")

    # Filters
    country = st.selectbox("Origin Country", ["All"] + sorted(recent_df["origin_country"].dropna().unique().tolist()))
    min_alt = st.slider("Minimum Altitude (m)", 0, 12000, 0)
    airborne_only = st.checkbox("Airborne Only", value=True)

    filtered_df = recent_df.copy()
    if country != "All":
        filtered_df = filtered_df[filtered_df["origin_country"] == country]
    filtered_df = filtered_df[filtered_df["baro_altitude"] >= min_alt]
    if airborne_only:
        filtered_df = filtered_df[filtered_df["on_ground"] == False]

    # Map display
    st.subheader("Flight Map")
    if not filtered_df.empty:
        st.map(filtered_df[["latitude", "longitude"]])
        st.dataframe(filtered_df[["callsign", "origin_country", "latitude", "longitude", "baro_altitude"]])
    else:
        st.info("No flights match your filters.")
else:
    st.info("No flight data yet. Click 'Fetch Live Flight Data' above.")
    
