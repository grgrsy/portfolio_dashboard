import streamlit as st
from datetime import datetime, timedelta, date

## A faire en OOP

def page_config():
    st.set_page_config(
        page_title = "Portfolio Dashboard",
        layout="wide",
        initial_sidebar_state="expanded"
        )

def add_time_period():

    # Add a time period selection
    st.markdown("Select Timeperiod")
    period = st.radio(
        label="Select Timeperiod",
        options=["all", "1y", "YTD", "6m", '1m'],
        horizontal=True,
        label_visibility='collapsed',
    )
    today = date.today()
    period_start_date = {
        "all" : date.min,
        "1y" : today-timedelta(days=365),
        'YTD' : date(today.year, 1, 1),
        '6m' : today-timedelta(365//2),
        '1m' : today-timedelta(30),
    }
    return period_start_date[period]