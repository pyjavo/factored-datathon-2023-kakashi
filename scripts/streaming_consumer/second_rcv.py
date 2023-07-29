#!/usr/bin/env python

import asyncio
import os
import logging

from azure.eventhub.aio import EventHubConsumerClient
from dotenv import load_dotenv


load_dotenv()


CONNECTION_STR = os.getenv('CONNECTION_STR')
CONSUMER_GROUP = os.getenv('CONSUMER_GROUP')  # como venía en los ejemplos
EVENTHUB_NAME = os.getenv('EVENTHUB_NAME')

async def on_event(partition_context, event):
    # Put your code here.
    # logging.info(
    #     f"_____________Received event from partition: {partition_context.partition_id}."
    # )
    # logging.info(event)
    print(f"_____________Received event from partition: {partition_context.partition_id}.")
    # print(event)
    # print()
    await partition_context.update_checkpoint(event)


async def receive(client):
    """
    Without specifying partition_id, the receive will try to receive events from all partitions and if provided with
    a checkpoint store, the client will load-balance partition assignment with other EventHubConsumerClient instances
    which also try to receive events from all partitions and use the same storage resource.
    """
    await client.receive(
        on_event=on_event,
        starting_position="-1",  # "-1" is from the beginning of the partition.
    )
    # With specified partition_id, load-balance will be disabled, for example:
    # await client.receive(on_event=on_event, partition_id='0'))


async def main():
    # checkpoint_store = BlobCheckpointStore.from_connection_string(STORAGE_CONNECTION_STR, BLOB_CONTAINER_NAME)
    client = EventHubConsumerClient.from_connection_string(
        CONNECTION_STR,
        consumer_group=CONSUMER_GROUP,
        eventhub_name=EVENTHUB_NAME,
    )
    async with client:
        await receive(client)


if __name__ == '__main__':
    asyncio.run(main())
