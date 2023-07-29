import os
import logging
from datetime import datetime

from dotenv import load_dotenv
from azure.eventhub import EventHubConsumerClient
import google.cloud.logging
import gcsfs
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

# Configure logging

logging_client = google.cloud.logging.Client()
logging_client.setup_logging()


logger = logging.getLogger("azure.eventhub")
logging.basicConfig(level=logging.INFO)

def on_event(partition_context, event):
    logger.info(
        f"Received event from partition {partition_context.partition_id}"
    )
    print(f"Received event from partition {partition_context.partition_id}")
    
    logger.info(event)
    print(event)


    df = pd.read_json(event)

    file_path = team_data_lake + f"{current_date}.parquet"

    try:
        df.to_parquet(file_path, engine='fastparquet', append=True)
    except AttributeError as e:
        df.to_parquet(file_path, engine='fastparquet')
        
    # df.to_parquet(file_path, mode="append")
    partition_context.update_checkpoint(event)

with client:
    client.receive(
        on_event=on_event
    )
    # receive events from specified partition:
    # client.receive(on_event=on_event, partition_id='0')
