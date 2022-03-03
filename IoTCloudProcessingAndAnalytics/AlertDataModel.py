# Class to define the operations on Alert Data Model

import boto3
import json
from datetime import datetime, timedelta
from Database import Database
from AggregateModel import AggregateModel
from boto3.dynamodb.conditions import Key

class AlertDataModel:
    # Class static variables used like the table name, devices and device types
    # Static variables are referred to by using <class_name>.<variable_name>
    TABLE = "bsm_alerts"
    SENSOR = ["HeartRate", "SPO2", "Temperature"]
    DEVICE = ['BSM_G101', 'BSM_G102']

    def __init__(self):
        self._db = Database()

    # Function to find a single data item from bsm_alerts.
    # Hash key is the combination of device id and datatype. Sort key is timestamp(first breach instance)
    def find_single_alert_data(self,key, timestamp):
        expression = {'device-type': key,
               'timestamp': timestamp}
        response = self._db.get_single_item(AlertDataModel.TABLE, expression)
        return response

    # Function to retrieve alert data based on device-datatype combination
    def find_alert_data_by_device(self, device_type):
        expression = Key('device-type').eq(device_type)
        response = self._db.get_data_from_table(AlertDataModel.TABLE, expression)
        return response

    # Function to retrieve data from bsm_alerts for device and timestamp range
    def find_alert_data_by_ts_range(self, device_type, from_ts, to_ts):
        expression = Key('device-type').eq(device_type) & Key('timestamp').between(from_ts, to_ts)
        response = self._db.get_data_from_table(AlertDataModel.TABLE, expression)
        return response

    # To insert a single data item into bsm_alerts
    def insert_single_item(self, rec):
        self._db.insert_item(AlertDataModel.TABLE, rec)
        return

    # To delete the bsm_alerts table
    def del_alert_table(self):
        response = self._db.del_table(AlertDataModel.TABLE)
        return response

    # Function to create the bsm_alerts tables
    def create_alert_table(self):
        attributedefinitions = [
            {
                'AttributeName': 'device-type',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'timestamp',
                'AttributeType': 'S'
            }
        ]
        keyschema = [
            {
                'AttributeName': 'device-type',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'timestamp',
                'KeyType': 'RANGE'
            }
        ]
        provisionedthroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
        response = self._db.create_dynamo_table(AlertDataModel.TABLE, attributedefinitions, keyschema, provisionedthroughput)
        return response

    # To get the rules from the rules configuration file
    def get_rules(self, filename):
        with open(filename, "r") as jsonfile:
            data = json.load(jsonfile)
        return data

    # Function to generate alerts based on the rules set in config file
    def generate_alert_data(self, from_timestamp, to_timestamp):
        result = ""
        existing_tables = self._db.get_tables_list()
        if AlertDataModel.TABLE not in existing_tables:
            self.create_alert_table()
        aggregatemodel = AggregateModel()
        data = self.get_rules("rules.json")

        for key, value in data.items():
            for device in AlertDataModel.DEVICE:
                print(f"Processing rules for {device}")
                for rule_type, item in value.items():
                    dev_id = device + "-" + item['datatype']
                    response = aggregatemodel.find_agg_data_by_ts_range(dev_id, from_timestamp, to_timestamp)
                    result = self.insert_alert_data(response, rule_type, item['avg_min'], item['avg_max'], item['trigger_count'])
        return result

    # Function to check the rule conditions set and insert record in bsm_alerts table
    # in case a rule is breached
    def insert_alert_data(self, response, rule_type, avg_min, avg_max, trigger_count):
        count = 0
        rule_breached = ""
        for rec in response['Items']:
            if (rec['Avg'] < avg_min) or (rec['Avg'] > avg_max):
                if rec['Avg'] < avg_min:
                    rule_breached = "breach type min"
                elif rec['Avg'] > avg_max:
                    rule_breached = "breach type max"
                count += 1
                if count >= trigger_count:
                    temp_ts = datetime.strptime(rec['timestamp'], "%Y-%m-%d %H:%M:%S")
                    first_instance = temp_ts - timedelta(minutes=trigger_count - 1)
                    rec_data = {
                        'deviceid': rec['deviceid'],
                        'datatype': rec['datatype'],
                        'device-type': rec['device-type'],
                        'timestamp': first_instance.strftime("%Y-%m-%d %H:%M:%S"),
                        'rule-breached': rule_breached
                    }
                    print('Alert for {} on rule {} starting '
                          'at {} with {}'.format(rec['deviceid'], rule_type,
                                                 first_instance.strftime("%Y-%m-%d %H:%M:%S"), rule_breached))
                    self.insert_single_item(rec_data)
            else:
                count = 0
        result = "Alert data inserted"
        return result
