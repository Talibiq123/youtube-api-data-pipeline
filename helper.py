from datetime import datetime
import os
import logging
import json
import config
import requests
import pyodbc
import pandas as pd
import sqlalchemy
from youtube_api import youtube_v3_api


def convert_object_to_json(value) -> str:
    if isinstance(value, dict) or isinstance(value, list):
        value = json.dumps(value)
    return value

def convert_columns_to_json(report_dataframe: pd.DataFrame) -> pd.DataFrame:
    for column in report_dataframe.select_dtypes(include=['object']).columns:
        report_dataframe[column] = report_dataframe[column].apply(convert_object_to_json)
    
    return report_dataframe
    
def load_data_into_staging_table(data, table_name):
    try:
        if isinstance(data, dict):
            data = [data]

        df = pd.DataFrame(data, index=None)

        df['_elt_timestamp'] = datetime.now()
        df = convert_columns_to_json(df)

        engine = sqlalchemy.create_engine(
            "mssql+pyodbc://@localhost:1433/youtube_database"
            "?driver=ODBC+Driver+17+for+SQL+Server"
            "&trusted_connection=yes"
        )

        df.to_sql(
            name=f"stg_{table_name}",
            con=engine,
            schema='yt',
            if_exists='replace',
            index=None,
            dtype=sqlalchemy.types.NVARCHAR(length='max')
        )

    except Exception as e:
        logging.error(f"error : {e}")

def generate_destination_path(endpoint):
    today = datetime.now()
    year = today.strftime("%Y")
    month = today.strftime("%B")
    day = today.strftime("%d")

    base_dir = os.path.dirname(__file__)
    path = f"response\\{endpoint}\\{year}\\{month}\\{day}"
    file_name = f"{endpoint}.json"
    destination_path = os.path.join(base_dir, path, file_name)
    os.makedirs(os.path.dirname(destination_path), exist_ok=True)
    return destination_path

def get_channel_id(channel_handler):
    endpoint = "channels"

    destination_path = generate_destination_path(endpoint)

    # File exists locally
    if os.path.exists(destination_path):
        logging.info("Reading channel data from local file.")

        with open(destination_path, "r") as file:
            data = json.load(file)

    # CASE 2: File does not exist → Call API
    else:
        logging.info("File not found. Calling API.")

        url = config.base_url + endpoint
        params = {
            "part": "contentDetails,id,statistics,snippet",
            "forHandle": channel_handler,  # corrected param
            "key": config.api_key,
        }

        yt_api_obj = youtube_v3_api()
        data = yt_api_obj.get_data(endpoint, params)


    channel_id = data["items"][0]["id"]
    logging.info(f"Channel ID: {channel_id}")
    load_data_into_staging_table(data, endpoint)
    execute_merge_query(endpoint)

    return channel_id


# def load_data_into_staging_table_v2(data, table_name):
#     df = pd.DataFrame(data, index=None)
#     logging.info(f"response data df {df}")
#     df['update_timestamp'] = datetime.now()
#     logging.info(f" updated response data df {df}")



def get_playlist(channel_id: str):

    endpoint = "playlists"

    destination_path = generate_destination_path(endpoint)

    # File exists locally
    if os.path.exists(destination_path):
        logging.info("Reading playlist data from local file.")

        with open(destination_path, "r") as file:
            data = json.load(file)

    # CASE 2: File does not exist → Call API
    else:
        logging.info("File not found. Calling API.")

        url = config.base_url + endpoint
        params = {
            "part": "snippet,contentDetails,id",
            "channelId": channel_id,
            "key": config.api_key,
        }

        yt_api_obj = youtube_v3_api()
        data = yt_api_obj.get_data(endpoint, params)

    # Extract playlist IDs
    playlist_ids = [item["id"] for item in data.get("items", [])]
    logging.info(f"Playlist IDs: {playlist_ids}")

    # Standardized pipeline steps (same as get_channel_id)
    load_data_into_staging_table(data, endpoint)
    execute_merge_query(endpoint)

    return playlist_ids

def get_playlist_item(playlist_id: str):

    endpoint = "playlistItems"

    destination_path = generate_destination_path(endpoint)


    # File exists locally
    if os.path.exists(destination_path):
        logging.info("Reading playlist item data from local file.")

        with open(destination_path, "r") as file:
            data = json.load(file)

    # File does not exist → Call API
    else:
        logging.info("Playlist item file not found. Calling API.")

        url = config.base_url + endpoint
        params = {
            "part": "contentDetails,id,status,snippet",
            "playlistId": playlist_id,
            "key": config.api_key,
        }

        yt_api_obj = youtube_v3_api()
        data = yt_api_obj.get_data(endpoint, params)

    # Extract video IDs
    video_ids = [item["contentDetails"]["videoId"] for item in data.get("items", [])]

    logging.info(f"Video IDs extracted: {video_ids}")

    # create_staging_table(schema_name='yt', table_name='stg_playlist_items')
    # logging.info('Staging table for playlist items created.')

    string_json_data = json.dumps(data)
    string_json_data = string_json_data.replace("'", "''")

    # insert_data_into_staging_table(
    #     schema_name='yt',
    #     staging_table_name='stg_playlist_items',
    #     string_json_data=string_json_data
    # )
    load_data_into_staging_table(data, endpoint)
    logging.info('Playlist item data inserted into staging table.')

    execute_merge_query(endpoint=endpoint)

    logging.info('Playlist item merge query executed.')

    return video_ids

    
def get_videos(video_ids: list):

    endpoint = "videos"

    destination_path = generate_destination_path(endpoint)

    # File exists locally
    if os.path.exists(destination_path):
        logging.info("Reading videos data from local file.")

        with open(destination_path, "r") as file:
            data = json.load(file)

    # File does not exist → Call API
    else:
        logging.info("Videos file not found. Calling API.")

        url = config.base_url + endpoint

        params = {
            "part": "snippet,contentDetails,statistics",
            "id": ",".join(video_ids),
            "key": config.api_key,
        }

        yt_api_obj = youtube_v3_api()
        yt_api_obj.get_data(endpoint, params)

    # Extract video IDs (safe way)
    extracted_video_ids = [
        item.get("id") for item in data.get("items", [])
    ]

    logging.info(f"Video IDs extracted: {extracted_video_ids}")

    # Load into staging (standardized)
    load_data_into_staging_table(data, endpoint)
    logging.info('Videos data inserted into staging table.')

    # Execute merge
    execute_merge_query(endpoint=endpoint)
    logging.info('Videos merge query executed.')

    return extracted_video_ids
    

# def create_staging_table(schema_name: str, table_name: str):

    conn = pyodbc.connect(
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=localhost,1433;"
        "Database=youtube_database;"
        "Trusted_Connection=yes;"
    )

    cursor = conn.cursor()

    create_query = f"""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema_name}'
            AND TABLE_NAME = '{table_name}'
        )
        BEGIN
            CREATE TABLE {schema_name}.{table_name}(
                json_data NVARCHAR(MAX),
                _elt_insert_datetime DATETIME2 DEFAULT GETDATE()
            )
        END;
    """

    cursor.execute(create_query)
    conn.commit()


# def insert_data_into_staging_table(schema_name: str, staging_table_name: str, string_json_data: str):

    logging.info('started: insert_data_into_staging_table')

    conn = pyodbc.connect(
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=localhost,1433;"
        "Database=youtube_database;"
        "Trusted_Connection=yes;"
    )

    cursor = conn.cursor()

    # Optional full refresh
    delete_query = f"DELETE FROM {schema_name}.{staging_table_name};"
    cursor.execute(delete_query)
    conn.commit()

    insert_query = f"""
        INSERT INTO {schema_name}.{staging_table_name} (json_data)
        VALUES ('{string_json_data}')
    """

    cursor.execute(insert_query)
    conn.commit()
    conn.close()

    logging.info("Data inserted successfully")


def create_database_table_query():
    conn = pyodbc.connect(f'Driver={{ODBC Driver 17 for SQL Server}};Server=localhost;Port=1433;Database=youtube_database;Trusted_Connection=yes;')

    cursor = conn.cursor()
    with open('database.sql', 'r')as database_file:
        table_creation_query = database_file.read()
    cursor.execute(table_creation_query)
    conn.commit()


def execute_merge_query(endpoint: str):
    merge_qurey_file_name = f"{os.path.dirname(__file__)}/merge/{endpoint}.sql"
    with open(merge_qurey_file_name, 'r') as merge_file:
        merge_query = merge_file.read()
    conn = pyodbc.connect(
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=localhost,1433;"
        "Database=youtube_database;"
        "Trusted_Connection=yes;"
    )

    cursor = conn.cursor()
    cursor.execute(merge_query)
    conn.commit()
    cursor.close()
    conn.close()
