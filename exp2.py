import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import pandas as pd
import re

import urllib.request, json 
data=""
with urllib.request.urlopen("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json") as url:
    data = json.load(url)
eventsdata=pd.DataFrame(data)

eventsdata.info()