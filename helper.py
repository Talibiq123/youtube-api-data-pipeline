from datetime import datetime
import os
import logging
import json
import config
import requests
import pyodbc

def get_channel_id(channel_handler):
    endpoint = "channels"

    today = datetime.now()
    year = today.strftime("%Y")
    month = today.strftime("%B")
    day = today.strftime("%d")

    base_dir = os.path.dirname(__file__)
    path = f"response\\{endpoint}\\{year}\\{month}\\{day}"
    file_name = f"{endpoint}.json"
    destination_path = os.path.join(base_dir, path, file_name)

    os.makedirs(os.path.dirname(destination_path), exist_ok=True)

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

        response = requests.get(url, params=params)
        response.raise_for_status()

        data = response.json()

        with open(destination_path, "w") as file:
            json.dump(data, file, indent=4)


    channel_id = data["items"][0]["id"]
    logging.info(f"Channel ID: {channel_id}")
    create_staging_table(schema_name='yt', table_name='stg_channels')
    logging.info('Staging Table Created.')
    string_json_data = json.dumps(data)
    string_json_data = string_json_data.replace("'", "''")
    insert_data_into_staging_table(schema_name='yt', staging_table_name='stg_channels', string_json_data= string_json_data)
    logging.info(f'data insert into staging table')
    execute_merge_query(endpoint=endpoint)
    logging.info(f'merge query executed')
    return channel_id


def get_playlist(channel_id: str):

    endpoint = "playlists"

    today = datetime.now()
    year = today.strftime("%Y")
    month = today.strftime("%B")
    day = today.strftime("%d")

    base_dir = os.path.dirname(__file__)
    path = f"response\\{endpoint}\\{year}\\{month}\\{day}"
    file_name = f"{endpoint}.json"
    destination_path = os.path.join(base_dir, path, file_name)

    os.makedirs(os.path.dirname(destination_path), exist_ok=True)

    # File exists locally
    if os.path.exists(destination_path):
        logging.info("Reading playlist data from local file.")

        with open(destination_path, "r") as file:
            data = json.load(file)

    # File does not exist → Call API
    else:
        logging.info("Playlist file not found. Calling API.")

        url = config.base_url + endpoint
        params = {
            "part": "contentDetails,id,snippet",
            "channelId": channel_id,
            "key": config.api_key,
        }

        response = requests.get(url, params=params)
        response.raise_for_status()

        data = response.json()

        with open(destination_path, "w") as file:
            json.dump(data, file, indent=4)

        logging.info("Playlist response saved locally.")

    playlist_ids = [item["id"] for item in data.get("items", [])]
    logging.info(f"Playlist IDs extracted: {playlist_ids}")

    create_staging_table(schema_name='yt', table_name='stg_playlists')
    logging.info('Staging table for playlists created.')

    string_json_data = json.dumps(data)
    string_json_data = string_json_data.replace("'", "''")

    insert_data_into_staging_table(
        schema_name='yt',
        staging_table_name='stg_playlists',
        string_json_data=string_json_data
    )

    logging.info('Playlist data inserted into staging table.')

    execute_merge_query(endpoint=endpoint)

    logging.info('Playlist merge query executed.')

    return playlist_ids

def get_playlist_item(playlist_id: str):

    endpoint = "playlistItems"

    today = datetime.now()
    year = today.strftime("%Y")
    month = today.strftime("%B")
    day = today.strftime("%d")

    base_dir = os.path.dirname(__file__)
    path = f"response\\{endpoint}\\{year}\\{month}\\{day}"
    file_name = f"{endpoint}.json"
    destination_path = os.path.join(base_dir, path, file_name)

    os.makedirs(os.path.dirname(destination_path), exist_ok=True)

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

        response = requests.get(url, params=params)
        response.raise_for_status()

        data = response.json()

        with open(destination_path, "w") as file:
            json.dump(data, file, indent=4)

        logging.info("Playlist item response saved locally.")

    # Extract video IDs
    video_ids = [item["contentDetails"]["videoId"] for item in data.get("items", [])]

    logging.info(f"Video IDs extracted: {video_ids}")

    create_staging_table(schema_name='yt', table_name='stg_playlist_items')
    logging.info('Staging table for playlist items created.')

    string_json_data = json.dumps(data)
    string_json_data = string_json_data.replace("'", "''")

    insert_data_into_staging_table(
        schema_name='yt',
        staging_table_name='stg_playlist_items',
        string_json_data=string_json_data
    )

    logging.info('Playlist item data inserted into staging table.')

    execute_merge_query(endpoint=endpoint)

    logging.info('Playlist item merge query executed.')

    return video_ids

    
def get_videos(video_ids: list):

    endpoint = "videos"

    today = datetime.now()
    year = today.strftime("%Y")
    month = today.strftime("%B")
    day = today.strftime("%d")

    base_dir = os.path.dirname(__file__)
    path = f"response\\{endpoint}\\{year}\\{month}\\{day}"
    file_name = f"{endpoint}.json"
    destination_path = os.path.join(base_dir, path, file_name)

    os.makedirs(os.path.dirname(destination_path), exist_ok=True)

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

        response = requests.get(url, params=params)
        response.raise_for_status()

        data = response.json()

        with open(destination_path, "w") as file:
            json.dump(data, file, indent=4)

        logging.info("Videos response saved locally.")

    video_ids = [item["id"] for item in data.get("items", [])]
    logging.info(f"Video IDs extracted: {video_ids}")

    create_staging_table(schema_name='yt', table_name='stg_videos')
    logging.info('Staging table for videos created.')

    string_json_data = json.dumps(data)
    string_json_data = string_json_data.replace("'", "''")

    insert_data_into_staging_table(
        schema_name='yt',
        staging_table_name='stg_videos',
        string_json_data=string_json_data
    )

    logging.info('Videos data inserted into staging table.')

    execute_merge_query(endpoint=endpoint)

    logging.info('Videos merge query executed.')

    return video_ids
    

def create_staging_table(schema_name: str, table_name: str):

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


def insert_data_into_staging_table(schema_name: str, staging_table_name: str, string_json_data: str):

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
