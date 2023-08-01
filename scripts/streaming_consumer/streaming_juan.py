import os
# import logging
from datetime import datetime

from dotenv import load_dotenv
from azure.eventhub import EventHubConsumerClient
from azure.eventhub._common import EventData
from azure.eventhub._eventprocessor.partition_context import PartitionContext
# import google.cloud.logging
import gcsfs
import pandas as pd


load_dotenv()

connection_str = os.getenv('CONNECTION_STR')
consumer_group = os.getenv('CONSUMER_GROUP')
eventhub_name = os.getenv('EVENTHUB_NAME')

# Get the current date and time
current_date_time = datetime.now()

# Format the current date as a string using the desired format
current_date = current_date_time.strftime("%Y%m%d")

client = EventHubConsumerClient.from_connection_string(
    connection_str,
    consumer_group,
    eventhub_name=eventhub_name
)

body_fields = [
    'asin',
    'image',
    'overall',
    'reviewText',
    'reviewerID',
    'reviewerName',
    'style',
    'summary',
    'unixReviewTime',
    'verified',
    'vote',
    'internal_partition'
]

# Configure logging

# logging_client = google.cloud.logging.Client()
# logging_client.setup_logging()

# === This logger prints too much information ===
# logger = logging.getLogger("azure.eventhub")
# logging.basicConfig(level=logging.INFO)

def on_event(partition_context: PartitionContext, event: EventData):
    # logger.info(
    #     f"Received event from partition {partition_context.partition_id}"
    # )
    # logger.info(f"___Event\n {event}")
    #print(f"Received event from partition {partition_context.partition_id}")
    #print(f"___Event\n {event}")  # event is EventData

    jsonbody = event.body_as_json(encoding='UTF-8')  # jsonbody is a dict
    # print(f"======== Received jsonbody =======\n {jsonbody}")
    #print()
    #print()

    df_event = pd.DataFrame(index=[0])

    # Json body fields
    for field in body_fields:
      df_event[field] = jsonbody[field] if field in jsonbody else None
    
    # Metadata fields
    df_event['offset'] = event.offset
    df_event['sequence_number'] = event.sequence_number
    df_event['enqueued_time'] = event.enqueued_time
    
    df_event = df_event.astype('string')
    
    # Cloud path
    bucket_name = 'kakashi_data_lake'
    gcs_folder_path = 'streaming/amazon_reviews_data/'
    file_name = f"{current_date}_{event.offset}.parquet"
    file_path = f"gs://{bucket_name}/{gcs_folder_path}{file_name}"

    #print()
    #print('creating file...')
    print()
    df_event.to_parquet(file_path, engine='pyarrow', index=False)
    print(f'file {file_name} created')

    partition_context.update_checkpoint(event)


with client:
    print('starting...')
    client.receive(
        on_event=on_event,
        starting_position="-1",
    )