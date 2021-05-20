import pandas as pd
from pypfopt.expected_returns import mean_historical_return
from pypfopt.risk_models import CovarianceShrinkage
from pypfopt.efficient_frontier import EfficientFrontier
import matplotlib.pylab as plt
import requests
import numpy as np
import logging
from decimal import Decimal
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime, timedelta

def addModelPrediction(prediction_type, sharpe_ratio, annual_return, volatility, weights):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('ModelPredictions')
    for key in weights.keys():
        weights[key] = Decimal(str(weights[key]))
        
    table.put_item(
            Item={
            'model_type': prediction_type,
            'date': todaysDate,
            'sharpe_ratio':Decimal(str(sharpe_ratio)),
            'annual_return':Decimal(str(annual_return)),
            'volatility':Decimal(str(volatility)),
            'weights':weights
            }
            )
    

dynamodb=boto3.resource("dynamodb",region_name=None)
table = dynamodb.Table('FundMetrics-65e776yxknfzlddagx47d36qku-staging')
todaysDate = int(datetime.today().replace(hour=0, minute=0, second=0, microsecond=0).strftime("%s"))
d = datetime.today() - timedelta(days=240)
prev = int(d.strftime('%s'))
response = table.scan(FilterExpression=Attr('date').gt(prev))
items = response['Items']
while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])
dfitems=pd.DataFrame(items,columns=['code','date','price'])

f= dfitems.drop_duplicates(subset=['date','code'],keep='last')
filtered = dfitems.pivot(index='date', columns='code', values='price')

cols = filtered.columns
filtered[cols] = filtered[cols].apply(pd.to_numeric, errors='coerce')
filtered = filtered.dropna()
print(filtered)
mu = mean_historical_return(filtered)
S = CovarianceShrinkage(filtered).ledoit_wolf()
ef = EfficientFrontier(mu, S)
max_sharpe = ef.max_sharpe()
max_sharpe_performance = ef.portfolio_performance()


minVolFrontier = EfficientFrontier(mu, S)
min_volatility = minVolFrontier.min_volatility()
min_vol_performance = minVolFrontier.portfolio_performance()

print(max_sharpe_performance, "MAX SHARPE PERFORMANCE")
print(min_vol_performance, "MIN VOL PERFORMANCE")

addModelPrediction("max_sharpe_ratio", max_sharpe_performance[2], max_sharpe_performance[0], max_sharpe_performance[1], max_sharpe)
addModelPrediction("min_volatility", min_vol_performance[2], min_vol_performance[0], min_vol_performance[1], min_volatility)


url = "https://4lm8nt9c67.execute-api.eu-central-1.amazonaws.com/dev/scrape/daily"
codes = ["AEC","AEE","AEN","AEP","AEU","AEZ","AGL","ALH","ALI","ALR","ALS","ALU","AMA","AMB","AMF","AMG","AMP","AMR","AMS","AMY","AMZ","APG","AUA","AUG","AZA","AZB","AZD","AZH","AZK","AZL","AZM","AZN","AZO","AZS","AZT","AZY","FYU","FYY","KOE"]
payload = {"codes": codes}
headers = {"Content-Type": "application/json"}
response = requests.request("POST", url, json=payload, headers=headers)
print(response)