import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

st.set_page_config(
    page_title="Cape Cod STR Tracker",
    page_icon="",
    layout="wide"
)



@st.cache_data
def load_data():
    engine = create_engine(
    os.getenv('DATABASE_URL'),
    connect_args={"connect_timeout": 30},
    pool_pre_ping=True
)
    str_by_town = pd.read_sql("SELECT * FROM mart.mart_str_by_town", engine)
    str_growth = pd.read_sql("SELECT * FROM mart.mart_str_growth", engine)
    owner_analysis = pd.read_sql("SELECT * FROM mart.mart_owner_analysis", engine)
    return str_by_town, str_growth, owner_analysis

str_by_town, str_growth, owner_analysis = load_data()

# keep only latest snapshot per town
str_by_town = str_by_town.sort_values('snapshot_date').groupby('town').last().reset_index()
str_by_town = str_by_town.dropna(subset=['str_pct_of_total'])  # add this
st.sidebar.title("Filters")
selected_towns = st.sidebar.multiselect(
    "Select Towns",
    options=sorted(str_by_town['town'].unique()),
    default=sorted(str_by_town['town'].unique())
)

# apply filter
str_by_town = str_by_town[str_by_town['town'].isin(selected_towns)]
owner_analysis = owner_analysis[owner_analysis['town'].isin(selected_towns)]
str_growth = str_growth[str_growth['town'].isin(selected_towns)]

# Header
st.title("Cape Cod Short-Term Rental Tracker")
st.caption("Data: MA DOR STR Registry · US Census ACS 5-Year · MassGIS Level 3 Parcels")

st.divider()

# KPI Cards
total_strs = str_by_town['str_count'].sum()
total_units = str_by_town['total_housing_units'].sum()
cape_wide_pct = round(total_strs / total_units * 100, 1)
most_affected = str_by_town.loc[str_by_town['str_pct_of_total'].idxmax(), 'town']
most_affected_pct = str_by_town['str_pct_of_total'].max()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total STR Certificates", f"{total_strs:,}")
col2.metric("Cape-Wide Penetration", f"{cape_wide_pct}%")
col3.metric("Most Affected Town", most_affected, f"{most_affected_pct}% penetration")
col4.metric("Housing Units Tracked", f"{total_units:,}")

st.divider()

# STR Penetration by Town
st.subheader("STR Penetration by Town")

fig = px.bar(
    str_by_town.sort_values('str_pct_of_total', ascending=True),
    x='str_pct_of_total',
    y='town',
    orientation='h',
    color='str_pct_of_total',
    color_continuous_scale=['#27ae60', '#f1c40f', '#e67e22', '#c0392b'],
    labels={'str_pct_of_total': 'STR % of Total Housing', 'town': ''},
    hover_data=['str_count', 'total_housing_units', 'vacant_units']
)
fig.update_layout(coloraxis_showscale=False, height=500)
st.plotly_chart(fig, use_container_width=True)

st.divider()

# Owner Analysis
st.subheader("Owner Residency by Town")

fig2 = px.bar(
    owner_analysis.sort_values('outofstate_pct', ascending=True),
    x=['instate_count', 'outofstate_count', 'unknown_count'],
    y='town',
    orientation='h',
    labels={'value': 'STR Certificates', 'town': '', 'variable': 'Owner'},
    color_discrete_map={
        'instate_count': '#2980b9',
        'outofstate_count': '#c0392b',
        'unknown_count': '#7f8c8d'
    }
)
fig2.update_layout(height=500, barmode='stack')
st.plotly_chart(fig2, use_container_width=True)

st.divider()

# Property Characteristics
st.subheader("STR Property Characteristics")

st.divider()

# Growth Over Time
st.subheader("STR Growth Over Time")
st.caption("Chart will populate as monthly snapshots accumulate.")

fig5 = px.line(
    str_growth.sort_values('snapshot_year'),
    x='snapshot_year',
    y='str_pct_of_total',
    color='town',
    markers=True,
    labels={'snapshot_year': 'Year', 'str_pct_of_total': 'STR % of Total Housing', 'town': 'Town'}
)
fig5.update_layout(height=500)
st.plotly_chart(fig5, use_container_width=True)
