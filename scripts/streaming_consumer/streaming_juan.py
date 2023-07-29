import os
# import logging
from datetime import datetime

from dotenv import load_dotenv
from azure.eventhub import EventHubConsumerClient
# import google.cloud.logging
# import gcsfs
import pandas as pd


load_dotenv()

connection_str = os.getenv('CONNECTION_STR')
consumer_group = os.getenv('CONSUMER_GROUP')
eventhub_name = os.getenv('EVENTHUB_NAME')
team_data_lake = os.getenv('TEAM_DATALAKE')

month = str(datetime.now().month)
year = str(datetime.now().year)
current_date = month + '_' + year

client = EventHubConsumerClient.from_connection_string(
    connection_str,
    consumer_group,
    eventhub_name=eventhub_name
)

column_names = [
    'vote',
    'style',
    'offset',
    'timestamp',
    'asin',
    'overall',
    'reviewText',
    'reviewerID',
    'reviewerName',
    'summary',
    'verified',
    'internal_partition',
    'partition_number',
    'unixReviewTime',
    'image'
]
# Local path for testing
file_path = 'reviews_' + f"{current_date}.parquet"
# file_path = team_data_lake + f"{current_date}.parquet"


# Configure logging

# logging_client = google.cloud.logging.Client()
# logging_client.setup_logging()

# === This logger prints too much information ===
# logger = logging.getLogger("azure.eventhub")
# logging.basicConfig(level=logging.INFO)

def on_event(partition_context, event):
    # logger.info(
    #     f"Received event from partition {partition_context.partition_id}"
    # )
    # logger.info(f"___Event\n {event}")
    print(f"Received event from partition {partition_context.partition_id}")
    print(f"___Event\n {event}")  # event is EventData

    jsonbody = event.body_as_json(encoding='UTF-8')  # jsonbody is a dict
    # print(f"======== Received jsonbody =======\n {jsonbody}")
    print()
    print()

    for col in column_names:
        if col not in jsonbody.keys():
            jsonbody[col] = None

    df = pd.DataFrame(jsonbody, index=[0])

    print()
    print('creating file...')
    print()
    try:
        df.to_parquet(file_path, engine='fastparquet', append=True)
        # df.to_parquet(file_path, mode="append")
    except FileNotFoundError:
        df.to_parquet(file_path, engine='fastparquet')

    partition_context.update_checkpoint(event)


with client:
    print('starting...')
    client.receive(
        on_event=on_event,
        starting_position="-1",
    )
    # receive events from specified partition:
    # client.receive(on_event=on_event, partition_id='0')
