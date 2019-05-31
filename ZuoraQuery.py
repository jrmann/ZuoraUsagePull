import requests
import json
import pandas as pd
import datetime
import time
import credentials
import Zuora

client_id = credentials.login['client_id']
client_secret = credentials.login['client_secret']

body = {
    'grant_type': 'client_credentials',
    'client_id': f'{client_id}',
    'client_secret': f'{client_secret}',
}

r = requests.post('https://rest.zuora.com/oauth/token', data=body)
access_token = r.json().get('access_token')

headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {access_token}',
}

zuora = Zuora(headers=headers)
usage_records = []
for record in zuora.query_all(
    '''
    select
    name,
    AccountNumber,
    CrmId
    from
    Account
    Where Status = 'Active' and ParentId = '2c92a0f946faaf520146fe301f4f3bc0'
    '''):

    account_number = record['AccountNumber']
    usages = zuora.query(
    f'''
    select
    Quantity,
    UOM,
    StartDateTime,
    RbeStatus
    from
    Usage
    Where RbeStatus = 'Processed' and AccountNumber = '{account_number}' and StartDateTime >= '2019-04-01T00:00:00-08:00'
    ''')
    for i in usages['records']:
        i['account_number'] = record['AccountNumber']
        i['account_name'] = record['Name']
        usage_records.append(i)

        df = pd.DataFrame(usage_records)
df['usage_month'] = df['StartDateTime'].astype('datetime64[M]')
grouped = df.groupby(['account_name','account_number','usage_month','UOM']).Quantity.sum().reset_index()
df.head(10)
