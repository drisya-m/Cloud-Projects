from pprint import pprint
import boto3
import json
import datetime
import os
import random
import base64
from boto3.dynamodb.conditions import Key

def lambda_handler(event, context):
    dynamodb_res = boto3.resource('dynamodb', region_name='us-east-1')
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        payload = base64.b64decode(record["kinesis"]["data"])
        payload = str(payload, 'utf-8')
        #pprint(payload, sort_dicts=False)

        payload_rec = json.loads(payload)
        #pprint(payload_rec, sort_dicts=False)

        table = dynamodb_res.Table('StockPOI')
        low_poi = payload_rec['52WeekLow'] * 1.2
        high_poi = payload_rec['52WeekHigh'] * 0.8
        if(payload_rec['Value'] <= low_poi) or (payload_rec['Value'] >= high_poi):
            temp_dict = {'Stock_id': payload_rec['Stock_id'], 'timestamp': payload_rec['timestamp'],
                         'Value': str(payload_rec['Value']), '52WeekLow': str(payload_rec['52WeekLow']),
                         '52WeekHigh': str(payload_rec['52WeekHigh'])}
            ts_from = str(payload_rec['timestamp'])[:10] + " 00:00:00"
            ts_to = str(payload_rec['timestamp'])[:10] + " 23:59:00"
            expression = Key('Stock_id').eq(payload_rec['Stock_id']) & Key('timestamp').between(ts_from, ts_to)
            response = table.query(
                KeyConditionExpression=expression
            )
            pprint(response['Items'])
            alert_sent = 'Yes'
            for item in response['Items']:
                if item['Alert Sent'] == 'Yes':
                    alert_sent = 'No'
            if alert_sent == 'Yes':
                temp_dict['Alert Sent'] = 'Yes'
            else:
                temp_dict['Alert Sent'] = 'No'
            pprint(temp_dict)

            response = table.put_item(Item=temp_dict)
            result = 1
            if temp_dict['Alert Sent'] == 'Yes':
                client = boto3.client('sns', region_name='us-east-1')
                try:
                    message = 'Point of interest observed for: ' + temp_dict['Stock_id'] + \
                              ' for value ' + temp_dict['Value'] + ' at timestamp ' + temp_dict['timestamp']
                    topic_arn = "arn:aws:sns:us-east-1:555374222971:StockPOI"
                    subject = "Stock Point of Interest:" + temp_dict['Stock_id']
                    client.publish(TopicArn=topic_arn, Message=message, Subject=subject)
                    result = 1
                except Exception:
                    result = 0

            return result




