import numpy as np
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine
import streamlit as st
from datetime import datetime

@st.cache
def get_connection():
    cnxn = create_engine('sqlite:///vn.db')
    return cnxn

@st.cache
def get_ticker(floor='HSX'):
    cnxn = create_engine('sqlite:///vn.db')
    query = f'''
    SELECT
        DISTINCT ticker
    FROM {floor}
    WHERE
        LENGTH(ticker) = 3
    ORDER BY ticker
    '''
    df = pd.read_sql(query, cnxn)
    return df

@st.cache
def get_data(ticker='PVT', floor='HSX', all=False):
    cnxn = create_engine('sqlite:///vn.db')
    query = f'''
    SELECT
        *
    FROM {floor}
    WHERE
        ticker = '{ticker}'
        AND LENGTH(ticker) = 3
    ORDER BY date
    '''
    df = pd.read_sql(query, cnxn, parse_dates=['date'])
    return df

def plot_candlestick(df):
    fig = go.Figure()
    candle_stick = go.Candlestick(
        x=df['date'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close']
    )
    fig.add_trace(candle_stick)
    fig.update_layout(
        autosize=True,
        height=700,
        width=700,
        margin=dict(
            l=0,
            r=0,
            t=0,
            b=0,
            pad=0
        )
    )
    return fig

st.title('Vietnam Stock')

floor = st.sidebar.radio('Floor', ['HSX', 'HNX', 'UPCOM'], index=0)

all_ticker = get_ticker(floor)

def preferred_ticker(floor):
    my_pref = {
        'HSX':'PVT',
        'HNX':'ACB',
        'UPCOM':'VNP'
    }
    return my_pref[floor]

ticker = st.sidebar.selectbox(
    'Ticker',
    all_ticker['ticker'].values,
    index=int(all_ticker.loc[all_ticker['ticker']==preferred_ticker(floor)].index.values[0]))

start_date = st.sidebar.date_input('Start', datetime.strptime('2020-06-01', '%Y-%m-%d'))
end_date = st.sidebar.date_input('End', datetime.strptime('2020-08-23', '%Y-%m-%d'))

df = get_data(ticker, floor)
st.subheader('Raw data')
st.write(df.tail())

st.subheader('Candlestick')

filtered_df = df.loc[
    (df['date'] >= pd.to_datetime(start_date))
    & (df['date'] <= pd.to_datetime(end_date))
]

st.plotly_chart(plot_candlestick(filtered_df), use_container_width=True)