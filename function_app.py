import logging
import azure.functions as func
import requests
from azure.communication.email import EmailClient
from azure.core.credentials import AzureKeyCredential
import json
from azure.storage.blob import BlobServiceClient
import os


BLOB_CONTAINER = "state"
BLOB_NAME = "master_dict.json"

blob_service = BlobServiceClient.from_connection_string(
    os.environ["AZURE_STORAGE_CONNECTION_STRING"]
)
container_client = blob_service.get_container_client(BLOB_CONTAINER)

def load_master_dict():
    try:
        blob = container_client.get_blob_client(BLOB_NAME)
        data = blob.download_blob().readall()
        return json.loads(data)
    except Exception:
        # First run fallback
        return {"BKK":[3,"2026-09-15"],"RUH":[1,"2026-07-04"],"SGN": [1,"2026-08-10"],"LHR":[3,"2026-12-14"],"HAN":[1,"2026-09-05"],"PEN":[0,"2026-06-01"],"FRA":[2,"2026-10-20"]}

def save_master_dict(data):
    blob = container_client.get_blob_client(BLOB_NAME)
    blob.upload_blob(json.dumps(data), overwrite=True)


app = func.FunctionApp()

def emailup(arr,old,new):
    credential = AzureKeyCredential(os.environ["ACS_EMAIL_KEY"])
    endpoint=os.environ["ACS_ENDPOINT"]
    client = EmailClient(endpoint,credential)

    message = {
            "senderAddress": "DoNotReply@b69c3249-d05b-47d9-a9a3-9fc4b60755d6.azurecomm.net",
            "recipients": {
                "to": [{"address": "autoalpha72110@gmail.com"},{"address": "arfathahmed380@gmail.com"},{"address": "rumaizankhan123456@gmail.com"}]
            },
            "content": {
                "subject": f'Schedule change detected to {arr} from Bengaluru',
                "plainText": f'Frequency increasing to {arr} from {old} to {new}x daily',
            },
            
        }
    logging.info(f"Email sent for {arr}")
    poller = client.begin_send(message)

def emaildown(arr,old,new):
    credential = AzureKeyCredential(os.environ["ACS_EMAIL_KEY"])
    endpoint=os.environ["ACS_ENDPOINT"]
    client = EmailClient(endpoint,credential)

    message = {
            "senderAddress": "DoNotReply@b69c3249-d05b-47d9-a9a3-9fc4b60755d6.azurecomm.net",
            "recipients": {
                "to": [{"address": "autoalpha72110@gmail.com"},{"address": "arfathahmed380@gmail.com"},{"address": "rumaizankhan123456@gmail.com"}]
            },
            "content": {
                "subject": f'Schedule change detected to {arr}',
                "plainText": f'Frequency reducing to {arr} from {old} to {new}x daily',
            },
            
        }
    logging.info(f"Email sent for {arr}")
    poller = client.begin_send(message)

def error():
    credential = AzureKeyCredential(os.environ["ACS_EMAIL_KEY"])
    endpoint=os.environ["ACS_ENDPOINT"]
    client = EmailClient(endpoint,credential)

    message = {
            "senderAddress": "DoNotReply@b69c3249-d05b-47d9-a9a3-9fc4b60755d6.azurecomm.net",
            "recipients": {
                "to": [{"address": "autoalpha72110@gmail.com"}]
            },
            "content": {
                "subject": "API fault",
                "plainText": 'API call returned an error! Please check immediately.',
            },
            
        }

    poller = client.begin_send(message)



def search (arr_id, date):
    dep_id="BLR"
    api_key= os.environ["SERPAPI_KEY"]
    response = requests.get("https://serpapi.com/search.json?engine=google_flights&departure_id="+dep_id+"&arrival_id="+arr_id+"&gl=in&hl=en&currency=INR&type=2&outbound_date="+date+"&show_hidden=true&adults=1&stops=1&api_key="+api_key)

    data=response.json()

    if response.status_code != 200:
        error()

    best_flights = data.get("best_flights", [])
    other_flights = data.get("other_flights", [])

    if best_flights or other_flights:
        return(len(best_flights)+len(other_flights))
    else:
        return (0)

def dictcheck():
    master_dict = load_master_dict()
    logging.info(f"Checking {len(master_dict)} routes")
    for k,v in master_dict.items():
        new=search(k,v[1])
        str(new)    
        if (new>v[0]):
            emailup(k,v[0],new)
            master_dict[k][0]=new
            
        elif (new<v[0]):
            emaildown(k,v[0],new)
            master_dict[k][0]=new
            
    save_master_dict(master_dict) 

@app.timer_trigger(
    schedule="0 30 3 * * *",  
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=True
)
def flight_monitor(myTimer: func.TimerRequest) -> None:
    logging.info("Flight schedule check started")

    if myTimer.past_due:
        logging.warning("Timer trigger was past due")

    dictcheck()

    logging.info("Flight schedule check completed")