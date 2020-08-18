import json
import boto3
import os
import csv
import codecs

s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb')

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    try:
        obj = s3.Object(bucket, key).get()['Body']
    except:
        print("S3 Object could not be opened. Check environment variables.")

    csv_dict_reader = csv.DictReader(codecs.getreader('utf-8')(obj))
    headers = csv_dict_reader.fieldnames
    
    table = create_dynamo_table(key, headers)
    batch_handler(csv_dict_reader, table)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Created DynamoDB table!')
    }

def create_dynamo_table(key, headers):
    existing_tables = dynamodb_client.list_tables()['TableNames']
    table_name = os.path.splitext(key)[0]
    
    if table_name not in existing_tables:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'N'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
    else:
        try:
            table = dynamodb.Table(table_name)
        except:
            print("Table could not be created and does not exist")
        
    waiter = dynamodb_client.get_waiter('table_exists')
    waiter.wait(TableName=table_name)
    
    return table
    
def batch_handler(csv_dict_reader, table):
    batch_size = 100
    batch = []
    batch_count = 0
    
    for row in csv_dict_reader:
        if len(batch) >= batch_size:
            write_to_dynamo(batch, table, batch_count)
            batch_count+=1
            batch.clear()
        batch.append(row)
        
    if batch:
        write_to_dynamo(batch, table, batch_count)

def write_to_dynamo(rows, table, batch_count):
    with table.batch_writer() as batch:
        for i in range(len(rows)):
            rows[i]['id']=100*batch_count+i+1
            batch.put_item(
                Item=rows[i]
            )