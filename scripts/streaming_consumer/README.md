# Running locally

- Create .env file with these variables:
```
CONNECTION_STR=Endpoint=sb://dummy-data.servicebus.windows.net/;SharedAccessKeyName=dummy-data;SharedAccessKey=dummy-data;EntityPath=dummy-data
CONSUMER_GROUP=dummy-data
EVENTHUB_NAME=dummy-data
```
- Create virtual environment
- Activate virtual environment
- Install requirements with `pip install -r requirements.txt`
- Run python script (there are 2 options for testing the functionality)
    - `python streaming_juan.py.py`
    - `python async_receiver.py`


## Fastparquet

How to append from file using `append=True`
https://fastparquet.readthedocs.io/en/latest/api.html#fastparquet.write
