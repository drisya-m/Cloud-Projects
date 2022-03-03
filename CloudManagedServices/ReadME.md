Build a system that streams stock pricing information for various stocks at different times and then notifies the stakeholders when the values cross specific 
points of interest (POIs). We use the Yahoo Finance APIs to query the running price of stocks and general information like 52-week high/low values.

System flow:

Yahoo API ---> EC2 ----> Kinesis Data Stream -----> AWS Lambda-----> SNS and DynamoDB
