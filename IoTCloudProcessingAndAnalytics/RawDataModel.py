# Class created for defining the variables and operation on RawData Model

import boto3
from datetime import datetime
import time
import json
from Database import Database
from boto3.dynamodb.conditions import Key

class RawDataModel:
    # Class static variables used like the table name
    # Static variables are referred to by using <class_name>.<variable_name>
    TABLE = "bsm_raw_data"

    def __init__(self):
        self._db = Database()


    # Function to retrieve data from bsm_raw_data for a device id
    def find_by_deviceid(self, deviceid):
        self._latest_error = ''
        expression = Key('deviceid').eq(deviceid)
        response = self._db.get_data_from_table(RawDataModel.TABLE, expression)
        for item in response['Items']:
            item['timestamp'] = datetime.strptime(item['timestamp'], "%Y-%m-%d %H:%M:%S.%f").replace(microsecond=0)
            item['timestamp'] = item['timestamp'].strftime("%Y-%m-%d %H:%M:%S")
        return response


    #Function to retrieve data from bsm_raw_data for a device id and timestamp range
    def find_by_device_ts_range(self, deviceid, from_timestamp, to_timestamp):
        self._latest_error = ''
        expression = Key('deviceid').eq(deviceid)
        response = self._db.get_data_from_table(RawDataModel.TABLE, expression)
        new_list = []
        for item in response['Items']:
            temp_ts = datetime.strptime(item['timestamp'], "%Y-%m-%d %H:%M:%S.%f").replace(microsecond=0)
            temp_ts = temp_ts.strftime("%Y-%m-%d %H:%M:%S")
            if(temp_ts >= from_timestamp) and (temp_ts <= to_timestamp):
                item['timestamp'] = temp_ts
                new_list.append(item)
        sorted_list = sorted(new_list, key=lambda rec: rec['timestamp'])
        return sorted_list

    # Function to delete the bsm_raw_data
    def del_raw_data_table(self):
        response = self._db.del_table(RawDataModel.TABLE)
        return response



