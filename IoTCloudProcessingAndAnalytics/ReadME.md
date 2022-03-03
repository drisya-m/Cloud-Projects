The project uses AWS services such as IoT core and DynamoDB along with python. The project simply focuses on a key area of data aggregation and anomaly detection based on the rule created by the user. The idea is to implement everything on local machine using boto3 and AWSIoTSDK.

Skills and Tools

boto3 & AWSPythonIoTSDK: Python simulation code will be written using SDK and using it to simulate the data. 
IoT core: This is used for thing creation, viewing data on MQTT, setting up rule to push data from MQTT to DB DynamoDB: creation of tables to store raw data, aggregate data and anomaly data. 
Python codes to perform the above mentioned operation such as creating the DB, pulling raw data and performing aggregation as well as anomaly detection and put the data back into DynamoDB using the boto3 client.
