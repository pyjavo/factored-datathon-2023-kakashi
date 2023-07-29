# Start

- Create .env file with these variables:
```
CONNECTION_STR=Endpoint=sb://dummy-data.servicebus.windows.net/;SharedAccessKeyName=dummy-data;SharedAccessKey=dummy-data;EntityPath=dummy-data
CONSUMER_GROUP=dummy-data
EVENTHUB_NAME=dummy-data
```
- Create virtual environment
- Activate virtual environment
- Install requirements with `pip install -r requirements.txt`
- Run python script (there are 2 options for testing functionality)
    - `python four_rcv.py`
    - `python second_rcv.py`


## Fastparquet

How to append from file using `append=True`
https://fastparquet.readthedocs.io/en/latest/api.html#fastparquet.write

## Error log

Currently producer is not enable according to timeline

- Project `four_rcv` gets error `CBS Put token error: b'amqp:not-found'`
```
INFO:azure.eventhub._pyamqp.aio._management_link_async:Management link receiver state changed: <LinkState.ATTACH_SENT: 1> -> <LinkState.ATTACHED: 3>
INFO:azure.eventhub._pyamqp.aio._link_async:Link state changed: <LinkState.ATTACH_SENT: 1> -> <LinkState.ATTACHED: 3>
INFO:azure.eventhub._pyamqp.aio._management_link_async:Management link sender state changed: <LinkState.ATTACH_SENT: 1> -> <LinkState.ATTACHED: 3>
INFO:azure.eventhub._pyamqp.aio._cbs_async:CBS completed opening with status: <ManagementOpenResult.OK: 1>
INFO:azure.eventhub._pyamqp.aio._cbs_async:CBS Put token error: b'amqp:not-found'
```


- Projecy `second_rcv.py` gets an error. `client-error: CBS Token authentication failed`

```
EventProcessor instance 'ad4bbc15-cf93-41fe-93c7-57813e5118dc' of eventhub 'factored-datathon' 
consumer group '$Default'. 
An error occurred while load-balancing and claiming ownership. 
The exception is AuthenticationError('CBS Token authentication failed.
Status code: None
Error: client-error
CBS Token authentication failed.
Status code: None'). 
Retrying after 32.11649362261099 seconds
```