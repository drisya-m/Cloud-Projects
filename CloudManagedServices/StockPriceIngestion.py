import json
import boto3
import sys
import yfinance as yf

import time
import random
import datetime


# Your goal is to get per-hour stock price data for a time range for the ten stocks specified in the doc. 
# Further, you should call the static info api for the stocks to get their current 52WeekHigh and 52WeekLow values.
# You should craft individual data records with information about the stockid, price, price timestamp, 52WeekHigh and 52WeekLow values and push them individually on the Kinesis stream

kinesis = boto3.client('kinesis', region_name = "us-east-1") #Modify this line of code according to your requirement.

today = datetime.date.today()
yesterday = datetime.date.today() - datetime.timedelta(1)

# Example of pulling the data between 2 dates from yfinance API
## Add code to pull the data for the stocks specified in the doc

stock_list = ['MSFT', 'MVIS', 'GOOG', 'SPOT', 'INO', 'OCGN', 'ABML', 'RLLCF', 'JNJ', 'PSFE']
for stock in stock_list:
    ticker = yf.Ticker(stock)
    info = ticker.info
    # print(info)
    week_52_low = info['fiftyTwoWeekLow']
    week_52_high = info['fiftyTwoWeekHigh']
    data = yf.download(stock, start=yesterday, end=today, interval='1h' )
    isempty = data.empty
    if not isempty:
        data.reset_index(inplace=True)
        print("Data for stock id:\n", stock)
        for timestamp, current_value in zip(data['index'], data['Close']):
            temp_dict = {'Stock_id': stock, 'timestamp': timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                         'Value': current_value, '52WeekLow': week_52_low, '52WeekHigh': week_52_high}
            print("Loading data to kinesis stream for rec:\n", temp_dict)
            ## Add your code here to push data records to Kinesis stream.
            response = kinesis.put_record(StreamName="stockpoidatastream",
                                          Data=json.dumps(temp_dict),
                                          PartitionKey=stock)
            print(response)


