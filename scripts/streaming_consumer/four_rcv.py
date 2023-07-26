'''
Referencia suministrada por la datathon:
https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-python-get-started-send?tabs=passwordless%2Croles-azure-portal

Documentación:
https://pypi.org/project/azure-eventhub/
'''

import os
import logging
import asyncio

from dotenv import load_dotenv
from azure.eventhub.aio import EventHubConsumerClient

load_dotenv()

connection_str = os.getenv('CONNECTION_STR')
consumer_group = os.getenv('CONSUMER_GROUP')  # como venía en los ejemplos
eventhub_name = os.getenv('EVENTHUB_NAME')

logger = logging.getLogger("azure.eventhub")
logging.basicConfig(level=logging.INFO) # niveles ERROR o CRITICAL no muestran nada

async def on_event(partition_context, event):
    logger.info("_______Received event from partition {}".format(partition_context.partition_id))
    await partition_context.update_checkpoint(event)

## ===== Without logging INFO messages ======
# async def on_event(partition_context, event):
#     # Print the event data.
#     print(
#         '________Received the event: "{}" from the partition with ID: "{}"'.format(
#             event.body_as_str(encoding="UTF-8"), partition_context.partition_id
#         )
#     )

#     # Update the checkpoint so that the program doesn't read the events
#     # that it has already read when you run it next time.
#     await partition_context.update_checkpoint(event)

async def receive():
    client = EventHubConsumerClient.from_connection_string(
        connection_str,
        consumer_group,
        eventhub_name=eventhub_name,
    )
    async with client:
        await client.receive(
            on_event=on_event,
            starting_position="-1",  # "-1" is from the beginning of the partition.
        )
        # receive events from specified partition:
        # await client.receive(on_event=on_event, partition_id='0')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(receive())
