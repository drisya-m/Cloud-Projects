# Main file for simulation

from RawDataModel import RawDataModel
from AggregateModel import AggregateModel
from AlertDataModel import AlertDataModel
import boto3
from datetime import datetime
import time
import json


rawdatamodel = RawDataModel()
aggregatedatamodel = AggregateModel()
alertdatamodel = AlertDataModel()

#Read data from bsm_raw_data for a device id
device_id = 'BSM_G102'
print(f'Raw Data for Device id:{device_id}')
response = rawdatamodel.find_by_deviceid(device_id)
print(response)

#Read data from bsm_raw_data for a device id and timestamp range
device_id = 'BSM_G101'
from_timestamp = '2021-11-27 12:52:06'
to_timestamp = '2021-11-27 12:52:10'
print("\n\n")
print(f'Raw Data for Device id {device_id} and timestamp range {from_timestamp} - {to_timestamp}')
response = rawdatamodel.find_by_device_ts_range(device_id, from_timestamp, to_timestamp)
print(response)

#Generate the aggregate data from bsm_raw_data per minute and insert to bsm_agg_data table
# for a given timestamp range
from_timestamp = '2021-11-27 13:00:00'
to_timestamp = '2021-11-27 14:00:00'
print("\n\n")
print(f'Aggregate data per minute for time range {from_timestamp} - {to_timestamp}')
response = aggregatedatamodel.insert_agg_data('BSM_G101', from_timestamp, to_timestamp)
print(response)
response = aggregatedatamodel.insert_agg_data('BSM_G102', from_timestamp, to_timestamp)
print(response)
#
# # Get data by device-type from bsm_agg_data
device_datatype = 'BSM_G102-SPO2'
print("\n\n")
print(f'Aggregated data for {device_datatype}:')
response = aggregatedatamodel.find_agg_data_by_device(device_datatype)
print(response)
#
# # Get data by device-type and timestamp from bsm_agg_data
device_datatype = 'BSM_G101-HeartRate'
timestamp = '2021-11-27 13:04:00'
print("\n\n")
print(f'Aggregated data for {device_datatype} and {timestamp}:')
response = aggregatedatamodel.find_single_agg_data(device_datatype, timestamp)
print(response)

# # Get data by device-type and timestamp range from bsm_agg_data
device_datatype = 'BSM_G102-Temperature'
from_timestamp = '2021-11-27 13:04:00'
to_timestamp = '2021-11-27 13:10:00'
print("\n\n")
print(f'Aggregated data for {device_datatype} and {from_timestamp} to {to_timestamp}:')
response = aggregatedatamodel.find_agg_data_by_ts_range(device_datatype, from_timestamp, to_timestamp)
print(response)

# # Get alerts based on rules in rules.json and insert data to bsm_alerts table in
# case of rule breach
from_timestamp = '2021-11-27 13:00:00'
to_timestamp =  '2021-11-27 14:00:00'
print("\n\n")
print(f"Generating alerts for rule breach for time {from_timestamp} - {to_timestamp}")
response = alertdatamodel.generate_alert_data(from_timestamp, to_timestamp)
print(response)

# # Get data by device-type from bsm_alerts
device_datatype = 'BSM_G102-SPO2'
print("\n\n")
print(f'Alert data for {device_datatype}:')
response = alertdatamodel.find_alert_data_by_device(device_datatype)
print(response)

# # Get data by device-type and timestamp from bsm_alerts
device_datatype = 'BSM_G101-HeartRate'
timestamp = '2021-11-27 13:05:00'
print("\n\n")
print(f'Alert data for {device_datatype} and {timestamp}:')
response = alertdatamodel.find_single_alert_data(device_datatype, timestamp)
print(response)

# # Get data by device-type and timestamp range from bsm_agg_data
device_datatype = 'BSM_G102-HeartRate'
from_timestamp = '2021-11-27 13:30:00'
to_timestamp = '2021-11-27 13:36:00'
print("\n\n")
print(f'Alert data for {device_datatype} and {from_timestamp} to {to_timestamp}:')
response = alertdatamodel.find_alert_data_by_ts_range(device_datatype, from_timestamp, to_timestamp)
print(response)

# To delete the aggregate table if needed. The table will be created during call to
# insert_agg_data if it doesnt exist
response = aggregatedatamodel.del_agg_table()
print(response)

# To delete the aggregate table if needed. The table will be created during call to
# insert_agg_data if it doesnt exist
response = alertdatamodel.del_alert_table()
print(response)

# To delete the raw data table if needed.
response = rawdatamodel.del_raw_data_table()
print(response)




