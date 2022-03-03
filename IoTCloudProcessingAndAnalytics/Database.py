# creates dynamodb resource for base level access Dynamodb
import boto3

class Database:
    # Class static variables used for database like db name
    # Static variables are referred to by using <class_name>.<variable_name>

    DB_NAME = 'dynamodb'

    def __init__(self):
        self._db = boto3.resource(Database.DB_NAME)
        self._client = boto3.client(Database.DB_NAME)

    # Function to get the list of tables existing in dynamodb
    def get_tables_list(self):
        response = self._client.list_tables()['TableNames']
        return response

    # Function to retrieve a single data item from table
    def get_single_item(self, table_name, expression):
        table = self._db.Table(table_name)
        response = table.get_item(
            Key=expression
        )
        return response

    # Function to insert single item to table
    def insert_item(self, table_name, data):
        table = self._db.Table(table_name)
        response = table.put_item(
            Item=data
        )
        return

    # Function to retrieve data from table based on the query expression
    def get_data_from_table(self, table_name, expression):
        table = self._db.Table(table_name)
        response = table.query(
            KeyConditionExpression=expression
        )
        return response

    # Function to create table in dynamodb
    def create_dynamo_table(self, table_name, attributedefinitions, keyschema, provisionedthroughput):
        table = self._db.create_table(
            TableName=table_name,
            AttributeDefinitions=attributedefinitions,
            KeySchema=keyschema,
            ProvisionedThroughput=provisionedthroughput
        )
        # Wait until the table exists.
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        response = f'Table created with {table.item_count} records'
        return response

    # Function to delete a table in dynamodb
    def del_table(self, table_name):
        table = self._db.Table(table_name)
        table.delete()
        table.meta.client.get_waiter('table_not_exists').wait(TableName=table_name)
        response = "Table deleted"
        return response
