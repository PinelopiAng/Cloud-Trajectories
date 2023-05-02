import streamlit as st
import datetime

import pandas as pd
import pandas.io.sql as sqlio

import os

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# ----------------------------

# TODO: add page_icon in set_page_config arguments

st.set_page_config(layout="wide", initial_sidebar_state="expanded")

st.title("Cloud Trajectories Project")

st.write("""This is the frontend for the Cloud Trajectories Project. Using the sidebar, we input values for the fields we are interested in. Then, a query is performed to retrieve the relevant documents from the PostgreSQL database.""")

# the sidebar is used for parameter setting

with st.sidebar:
    st.markdown("""## featX""")
    use_featx = st.checkbox("Include featX in query?")
    if use_featx == True:
        featx = st.slider("Select a range of values for `featX`", 0, 10000, (100, 5000))

    st.markdown("""## Lat/Lon """)
    use_latlon = st.checkbox("Include spatial information in query?")
    if use_latlon == True:
        lat_min = st.number_input('Insert Min Lat')
        lat_max = st.number_input(label='Insert Max Lat', min_value=lat_min)

        lon_min = st.number_input('Insert Min Lon')
        lon_max = st.number_input(label='Insert Max Lon', min_value=lon_min)

    st.markdown("""## featK""")
    use_featk = st.checkbox("Include featK in query?")
    if use_featk == True:
        featk = st.slider("Select a range of values for `featK`", 200, 300, (220, 230))

    st.markdown("""## direction""")
    use_direction = st.checkbox("Include direction in query?")
    if use_direction == True:
        dir_N = st.checkbox("N")
        dir_M = st.checkbox("M")
        dir_MS = st.checkbox("MS")
        dir_S = st.checkbox("S")

    st.markdown("""## timestamp""")
    use_datetime = st.checkbox("Include datetime information in query?")
    if use_datetime == True:
        start_date = st.date_input(label="Input start date", value=datetime.date(2020, 2, 1))
        start_time = st.time_input('Input start time', datetime.time(0, 0))
        
        end_date = st.date_input(label="Input end date", value=datetime.date(2020, 2, 1), min_value=start_date)
        end_time = st.time_input('Input end time', datetime.time(6, 0))

# Here starts the SQL querying part

st.markdown("""After selecting the features on which the queries will be based, click on the following button to get results. The results correspond to the cloud names that satisfy the given conditions.""")

# ------------------ Setup DB connection ---------------------------------------------------------------
# Set parameters for initial connection to new-built database server
host = 'postgres-trajectories-server.postgres.database.azure.com'
database = 'clouddb'
user = 'cloudadmin'
password = os.environ.get('SQLPASSWORD')
sslmode = 'require'

# Connect to the PostgreSQL server
conn_string = f"host={host} user={user} dbname={database} password={password} sslmode={sslmode}"
conn = psycopg2.connect(conn_string)

# We have to add this here
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
# ------------------------------------------------------------------------------------------------------

if st.button('Calculate Clouds'):

    st.write("The filenames that respect the conditions you set in the sidebar's parameters are the following:")

    query_a = "SELECT * FROM dataseta"
    df_a = sqlio.read_sql_query(query_a, conn)

    # Set a generic query that asks for the parameter values set in the sidebar and returns relevant cloud ids
    adf = df_a.copy()
    if use_featx == True:
        adf = adf[(df_a['featx'] >= featx[0]) & (adf['featx'] <= featx[1])]
    
    if use_latlon == True:
        adf = adf[(df_a['lat'] >= lat_min) & (adf['lat'] <= lat_max)]
        adf = adf[(df_a['lon'] >= lon_min) & (adf['lon'] <= lon_max)]

    if use_featk == True:
        adf = adf[(df_a['featk'] >= featk[0]) & (adf['featk'] <= featk[1])]

    if use_direction == True:
        vals_list = []
        if dir_N == True:
            vals_list.append('N')
        if dir_M == True:
            vals_list.append('M')
        if dir_S == True:
            vals_list.append('S')
        if dir_MS == True:
            vals_list.append('MS')

        adf = adf[adf['direction'].isin(vals_list)]

    if use_datetime == True:
        start_ts = pd.Timestamp(datetime.datetime.combine(start_date, start_time))
        end_ts = pd.Timestamp(datetime.datetime.combine(end_date, end_time))

        adf = adf[adf['time'].between(start_ts, end_ts)]

    good_cloud_ids = adf['cloudid'].unique().tolist()

    query_ids = "SELECT * FROM cloudids"
    df_ids = sqlio.read_sql_query(query_ids, conn)

    df_ids = df_ids[df_ids['cloudid'].isin(good_cloud_ids)]

    df_ids = df_ids['filenames']

    st.dataframe(df_ids)

# Close connection
conn.close()
