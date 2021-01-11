import gzip
import json
import logging
import os
import zipfile
import boto3
from botocore.exceptions import ClientError


def lambda_handler(event, context):
    # first step:
    # connecting to cost explorer to get monthly(December) aws usage
    # try/exception: if exception, print and log error message

    try:
        bill_client = boto3.client('ce')
        bill_data = bill_client.get_cost_and_usage(
            TimePeriod={'Start': '2020-12-01', 'End': '2021-01-01'},
            Granularity='MONTHLY',
            Metrics=['UnblendedCost', 'UsageQuantity'],
            GroupBy=[{'Type': 'DIMENSION', 'Key': 'SERVICE'},
                     {'Type': 'DIMENSION', 'Key': 'USAGE_TYPE'}]
        )
    except ClientError as e:
        print('something wrong when fetching the cost and uasge')
        logging.error(e)

    # second step:
    # creating table(bills) and storing bill data into it
    # try/exception: creating table if table(bills) does not exist,
    # otherwise using the existing table(bills),
    # otherwise print and log error message

    dynamodb_client = boto3.client('dynamodb')
    try:
        table = dynamodb_client.create_table(
            TableName='bills',
            KeySchema=[
                {
                    'AttributeName': 'bill_item_id',
                    'KeyType': 'HASH'
                }],
            AttributeDefinitions=[
                {
                    'AttributeName': 'bill_item_id',
                    'AttributeType': 'N'
                }],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        table.meta.client.get_waiter('table_exists').wait(TableName='bills')
    except dynamodb_client.exceptions.ResourceInUseException:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('bills')
    except ClientError as e:
        print('something wrong when using dynamodb')
        logging.error(e)

    # putting bill_data items into the table(bills)

    start_date = bill_data['ResultsByTime'][0]['TimePeriod']['Start']
    end_date = bill_data['ResultsByTime'][0]['TimePeriod']['End']
    services = bill_data['ResultsByTime'][0]['Groups']
    bill_item_id = 1
    bill_items = []
    for service in services:
        service_name = service['Keys'][0]
        service_type = service['Keys'][1]
        unblended_cost = service['Metrics']['UnblendedCost']
        usage_quantity = service['Metrics']['UsageQuantity']
        bill_item = {
            'bill_item_id': bill_item_id,
            'start_date': start_date,
            'end_date': end_date,
            'service_name': service_name,
            'service_type': service_type,
            'unblended_cost': unblended_cost,
            'usage_quantity': usage_quantity
        }
        table.put_item(Item=bill_item)
        bill_items.append(bill_item)
        bill_item_id = bill_item_id + 1

    # third step:
    # creting S3 bucket: storageforbills
    # try/exception: creating bucket if bucket does not exist,
    # otherwise using existing bucket,
    # otherwise print anf log error message

    s3_client = boto3.client('s3')
    try:
        s3_client.create_bucket(Bucket='storageforbills')
    except s3_client.exceptions.BucketAlreadyExists:
        pass
    except ClientError as e:
        print('someting wrong when using bucket')
        logging.error(e)

    # fourth step:
    # creating unzipped bills.json and storing it in the bucket:storageforbills
    # try/exception: uploadung bills.json, if error happens, print and log error

    bill_items_json = json.dumps(bill_items)
    gzip.compress(bill_items_json.encode('utf-8'))
    try:
        s3_client.put_object(
            ACL='private',
            Body=bill_items_json,
            Key='bills.json',
            Bucket='storageforbills')
    except ClientError as e:
        print('something wrong when uploading bills.json')
        logging.error(e)

    # fifth step:
    # zipping bills.json and storing it in the storageforbills bucket
    # try/exception: uploadung bills.zip, if error happens, print and log error

    with open(os.path.join('/tmp', 'bills.json'), 'wb') as f:
        s3_client.download_fileobj('storageforbills', 'bills.json', f)
    with zipfile.ZipFile(os.path.join('/tmp', 'bills.zip'), 'w', compression=zipfile.ZIP_DEFLATED) as my_zip:
        my_zip.write(os.path.join('/tmp', 'bills.json'))
    try:
        s3_client.upload_file(
            os.path.join('/tmp', 'bills.zip'),
            'storageforbills',
            'bills.zip')
    except ClientError as e:
        print('something wrong when uploading bills.zip')
        logging.error(e)

    # sixth step:
    # creating presigned_url whose duration is 2 days(172800 seconds)
    # try/exception: uploadung bills.zip, if error happens, print and log error

    try:
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': 'storageforbills',
                    'Key': 'bills.zip'},
            ExpiresIn=172800)
        print(presigned_url)
    except ClientError as e:
        print("something wrong when generating presigned_url")
        logging.error(e)
