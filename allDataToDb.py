import json
import boto3
from decimal import Decimal
import datetime
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('FundMetrics-65e776yxknfzlddagx47d36qku-staging')
with open('./allAllianzData.json') as f:
  data = json.load(f)
count = 0
for fund in data:
    for key in fund.keys():
        if(len(str(key))>4):
            date_delimeted = key.split('.')
            table.put_item(
            Item={
            'id':str(count),
            'date': int(datetime.datetime(int(date_delimeted[2]),int(date_delimeted[1]),int(date_delimeted[0]),0,0,0).strftime('%s')),
            'code': fund["code"],
            'price': Decimal(str(fund[key])[0:8]),
            }
            )
            count = count+1
        else:
           print(key)