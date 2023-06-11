import streamlit as st
import datetime

import pandas as pd
import pandas.io.sql as sqlio

import os
import base64

# -------- for blobs download -------------------------
from azure.storage.blob import BlobServiceClient
import zipfile
import io
# -----------------------------------------------------

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# ----------------------------

st.set_page_config(layout="wide", initial_sidebar_state="expanded")

st.title("Cloud Trajectories Project")

st.write("""This is the frontend for the Cloud Trajectories Project. Using the sidebar, we input values for the fields we are interested in. Then, a query is performed to retrieve the relevant documents from the PostgreSQL database.""")

# ------------------ Setup DB connection ---------------------------------------------------------------
# Set parameters for initial connection to new-built database server
host = 'postgresbase-trajectories-server.postgres.database.azure.com'
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

# ------------------ Setup Azure Blob Storage connection -----------------------------------------------

connection_string = os.environ.get('BLOBCONNSTR')
container_name = "historical"

# ------------------------------------------------------------------------------------------------------

query_a = "SELECT * FROM dataset"
df_a = sqlio.read_sql_query(query_a, conn)

# the sidebar is used for parameter setting

with st.sidebar:
    st.markdown("""## Area Size""")
    use_areasize = st.checkbox("Include Area Size in query?")
    if use_areasize == True:
        areasize = st.slider("Select a range of values for `Area Size`", 0, int(df_a['area_size'].max()),(0,int(df_a['area_size'].max())))

    st.markdown("""## Xg_Cloud """)
    use_xcoordinate = st.checkbox("Include Xg coordinate in query?")
    if use_xcoordinate == True:
        xg_min = st.number_input('Insert Min Xg')
        xg_max = st.number_input(label='Insert Max Xg', min_value=xg_min)

    st.markdown("""## Yg_Cloud """)
    use_ycoordinate = st.checkbox("Include Yg coordinate in query?")
    if use_ycoordinate == True:
        yg_min = st.number_input('Insert Min Yg')
        yg_max = st.number_input(label='Insert Max Yg', min_value=yg_min)

    st.markdown("""## Tmin_B9 """)
    use_temperature = st.checkbox("Include Temperature in query?")
    if use_temperature == True:
        temperature = st.slider("Select a range of values for `Tmin_B9`", int(df_a['t_min_b9'].min()), int(df_a['t_min_b9'].max()), (int(df_a['t_min_b9'].min()),int(df_a['t_min_b9'].max())))

    st.markdown("""## Timestamp """)
    use_datetime = st.checkbox("Include datetime information in query?")
    if use_datetime == True:
        start_date = st.date_input(label="Input start date", value= df_a['timestamp'].min().date())
        start_time = st.time_input('Input start time', value = df_a['timestamp'].min().time())
        
        end_date = st.date_input(label="Input end date", value = df_a['timestamp'].max().date(), min_value=start_date)
        end_time = st.time_input('Input end time', value = df_a['timestamp'].max().time())

# Here starts the SQL querying part

st.markdown("""After selecting the features on which the queries will be based, click on the following button to get results. The results correspond to the cloud names that satisfy the given conditions.""")

if st.button('Calculate Clouds'):

    st.write("The filenames that respect the conditions you set in the sidebar's parameters are the following:")

    # Set a generic query that asks for the parameter values set in the sidebar and returns relevant cloud ids
    adf = df_a.copy()
    if use_areasize == True:
        adf = adf[(df_a['area_size'] >= areasize[0]) & (adf['area_size'] <= areasize[1])]
    
    if use_xcoordinate == True:
        adf = adf[(df_a['xg_cloud'] >= xg_min) & (adf['xg_cloud'] <= xg_max)]

    if use_ycoordinate == True:
        adf = adf[(df_a['yg_cloud'] >= yg_min) & (adf['lon'] <= yg_max)]

    if use_temperature == True:
        adf = adf[(df_a['t_min_b9'] >= temperature[0]) & (adf['t_min_b9'] <= temperature[1])]

    if use_datetime == True:
        start_ts = pd.Timestamp(datetime.datetime.combine(start_date, start_time))
        end_ts = pd.Timestamp(datetime.datetime.combine(end_date, end_time))

        adf = adf[adf['timestamp'].between(start_ts, end_ts)]

    good_cloud_ids = adf['cloudid'].unique().tolist()

    query_ids = "SELECT * FROM cloudids"
    df_ids = sqlio.read_sql_query(query_ids, conn)

    df_ids = df_ids[df_ids['cloudid'].isin(good_cloud_ids)]

    df_ids = df_ids['filenames']

    st.dataframe(df_ids)

    # At this point we have the file names.
    # From here on, we proceed with their download

    # Get the names of the files that have been calculated
    blob_names = df_ids.values.tolist()

    # Create a BlobServiceClient object by providing the connection string for Azure Blob storage.
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Get a reference to the container.
    container_client = blob_service_client.get_container_client(container_name)

    # Create a dictionary to store the downloaded blob content.
    blob_contents = {}

    # Download each blob and add the stream to the dictionary.
    for blob_name in blob_names:
        blob_client = container_client.get_blob_client(blob_name)
        blob_download_stream = blob_client.download_blob().content_as_bytes()
        blob_contents[blob_name] = io.BytesIO(blob_download_stream)

    # Create the zip file in memory.
    in_memory_zip = io.BytesIO()
    with zipfile.ZipFile(in_memory_zip, mode='w', compression=zipfile.ZIP_DEFLATED) as zip_file:
        # Add each file to the zip archive.
        for blob_name, blob_content in blob_contents.items():
            zip_file.writestr(blob_name, blob_content.getvalue())

    # Get the byte contents of the zip file.
    zip_contents = in_memory_zip.getvalue()

    st.markdown("""Would you like to download these files in a .zip?""")

    st.download_button(
        label="Download files",
        data=zip_contents,
        file_name='clouds.zip',
        mime='application/zip'
    )
    
    # Alternative way to download files, so as not to reload the page after downloading
    #download_url = create_download_link(zip_contents, 'clouds')
    #st.markdown(download_url, unsafe_allow_html=True)

# Close connection
conn.close()