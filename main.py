import logging
import json
import helper
import config
import requests

if __name__ == '__main__':

    logging.basicConfig(
        filename='log.txt',
        level=logging.INFO,
        filemode='w',
        format='-- %(levelname)s -- %(asctime)s, -- %(message)s'
    )

    logging.info("Pipeline started")
    helper.create_database_table_query()

    channel_handler = '@ThomDefiletHalalBusiness'

    logging.info("Fetching channel id for handler: %s", channel_handler)
    channel_id = helper.get_channel_id(channel_handler)

    logging.info("Channel id fetched successfully: %s", channel_id)


    playlist = helper.get_playlist(channel_id)
    print(playlist)
    print(playlist[0])

    playlist_item = helper.get_playlist_item(playlist[0])
    print(playlist_item)





    logging.info("YouTube channel ETL pipeline completed successfully")