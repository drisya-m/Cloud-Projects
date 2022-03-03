#Class to define the operations on Aggregate data model

import boto3
import json
from datetime import datetime, timedelta
from Database import Database
from RawDataModel import RawDataModel
from boto3.dynamodb.conditions import Key

class AggregateModel:
    # Class static variables used like the table name, devices
    # Static variables are referred to by using <class_name>.<variable_name>
    TABLE = "bsm_agg_data"
    SENSOR = ["HeartRate", "SPO2", "Temperature"]

    def __init__(self):
        self._db = Database()
        self._latest_error = ''

    # To retrieve single data item from bsm_agg_data
    # device-datatype combination is the hash key and timestamp is the range
    def find_single_agg_data(self,key, timestamp):
        expression = {'device-type': key,
               'timestamp': timestamp}
        response = self._db.get_single_item(AggregateModel.TABLE, expression)
        return response

    # To retrieve aggregate data based on device-datatype combination
    def find_agg_data_by_device(self, device_type):
        expression = Key('device-type').eq(device_type)
        response = self._db.get_data_from_table(AggregateModel.TABLE, expression)
        return response

    # To retrieve aggregated data for a device and timestamp range
    def find_agg_data_by_ts_range(self, device_type, from_ts, to_ts):
        expression = Key('device-type').eq(device_type) & Key('timestamp').between(from_ts, to_ts)
        response = self._db.get_data_from_table(AggregateModel.TABLE, expression)
        return response


    # Function to process data from raw data table and aggregate per minute for each sensor type
    # for the device. The aggregated data is inserted into bsm_agg_data table
    def insert_agg_data(self, deviceid, from_timestamp, to_timestamp):
        rawdata = RawDataModel()
        existing_tables = self._db.get_tables_list()
        if AggregateModel.TABLE not in existing_tables:
            self.create_agg_table()

        response = rawdata.find_by_device_ts_range(deviceid, from_timestamp, to_timestamp)
        heart_rate_list = []
        spo2_list = []
        temperature_list = []
        for item in response:
            if item['datatype'] == AggregateModel.SENSOR[0]:
                heart_rate_list.append(item)
            elif item['datatype'] == AggregateModel.SENSOR[1]:
                spo2_list.append(item)
            elif item['datatype'] == AggregateModel.SENSOR[2]:
                temperature_list.append(item)
        for sensor in AggregateModel.SENSOR:
            agg_dict = {}
            if sensor == 'HeartRate':
                agg_dict = self.get_agg_values(heart_rate_list)
            elif sensor == "SPO2":
                agg_dict = self.get_agg_values(spo2_list)
            elif sensor == "Temperature":
                agg_dict = self.get_agg_values(temperature_list)
            for rec in agg_dict:
                temp_rec = {
                    'deviceid': deviceid,
                    'datatype': sensor,
                    'device-type': deviceid + "-" + sensor,
                    'timestamp': rec,
                    'Min': agg_dict[rec]['min'],
                    'Max': agg_dict[rec]['max'],
                    'Avg': agg_dict[rec]['avg']
                }
                self.insert_single_item(temp_rec)
        result = f'Data inserted to bsm_agg_data for {deviceid}'
        return result


    # To compute the average , minimum and maximum values for a device and sensor per minute
    def get_agg_values(self,new_list):
        agg_val_dict = {}
        start_ts = datetime.strptime(new_list[0]['timestamp'], "%Y-%m-%d %H:%M:%S")
        start_ts = start_ts - timedelta(seconds=start_ts.second)
        temp_dict = {}
        temp_dict[start_ts.strftime("%Y-%m-%d %H:%M:%S")] = []
        for item in new_list:
            temp_ts = datetime.strptime(item['timestamp'], "%Y-%m-%d %H:%M:%S")
            if (temp_ts >= start_ts) and (temp_ts <= (start_ts + timedelta(minutes=1))):
                temp_dict[start_ts.strftime("%Y-%m-%d %H:%M:%S")].append(item['value'])
            elif temp_ts > (start_ts + timedelta(minutes=1)):
                temp_dict[(start_ts + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")] = []
                temp_dict[(start_ts + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")].append(item['value'])
                start_ts = start_ts + timedelta(minutes=1)
        for key, rec in temp_dict.items():
            agg_val_dict[key] = {'min': min(rec), 'max': max(rec), 'avg': round(sum(rec) / len(rec))}
        return agg_val_dict

    # To insert data into bsm_agg_data
    def insert_single_item(self, rec):
        self._db.insert_item(AggregateModel.TABLE, rec)
        return

    # To delete the bsm_agg_data table
    def del_agg_table(self):
        response = self._db.del_table(AggregateModel.TABLE)
        return response

    # To create the bsm_agg_data table
    def create_agg_table(self):
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
        response = self._db.create_dynamo_table(AggregateModel.TABLE, attributedefinitions, keyschema, provisionedthroughput)
        return response
