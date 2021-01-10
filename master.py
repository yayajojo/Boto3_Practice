import boto3
import json
import gzip


def lambda_handler(event, context):
    # TODO implement
    billClient = boto3.client('ce')
    # connecting to cost explorer to get monthly(December) aws usage
    billData = billClient.get_cost_and_usage(
        TimePeriod={
            'Start': '2020-12-01',
            'End': '2021-01-01'
        },
        Granularity='MONTHLY',
        Metrics=['UnblendedCost', 'UsageQuantity'],
        GroupBy=[{'Type': 'DIMENSION', 'Key': 'SERVICE'},
                 {'Type': 'DIMENSION', 'Key': 'USAGE_TYPE'}]
    )

    # creating table bills for storing billData.
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.create_table(
        TableName='bills',
        KeySchema=[
            {
                'AttributeName': 'billItemID',
                'KeyType': 'HASH'
            }],

        AttributeDefinitions=[
            {
                'AttributeName': 'billItemID',
                'AttributeType': 'N'
            }],

        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    table.meta.client.get_waiter('table_exists').wait(TableName='bills')
    
    # putting items in the bills table
    startDate = billData['ResultsByTime'][0]['TimePeriod']['Start']
    endDate = billData['ResultsByTime'][0]['TimePeriod']['End']
    services = billData['ResultsByTime'][0]['Groups']
    billItem = 1
    items = []
    for service in services:
        serviceName = service['Keys'][0]
        serviceType = service['Keys'][1]
        unblendedCost = service['Metrics']['UnblendedCost']
        usageQuantity = service['Metrics']['UsageQuantity']
        item = {
            'billItemID': billItem,
            'startDate': startDate,
            'endDate': endDate,
            'serviceName': serviceName,
            'serviceType': serviceType,
            'unblendedCost': unblendedCost,
            'usageQuantity': usageQuantity
        }
        table.put_item(
            Item=item
        )
        items.append(item)
        billItem = billItem + 1
    
    # creting S3 bucket: storageforbills
    jsonForStorage = json.dumps(items)
    s3Client = boto3.client('s3')
    s3Client.create_bucket(Bucket='storageforbills')
    
    # creating uncompressed bill.json and storing it in the bucket:storageforbills
    s3Client.put_object(
            ACL='private',
            Body=jsonForStorage,
            Key='bill.json',
            Bucket='storageforbills')
    # compressing jsonForStorage and storing it as
    gzip.compress(jsonForStorage.encode('utf-8'))
    s3Client.put_object(
            ACL='private',
            Body=jsonForStorage,
            Key='compressedbill.json',
            Bucket='storageforbills')
    presignedUrl = s3Client.generate_presigned_url(
            'get_object',
            Params={'Bucket': 'storageforbills',
                    'Key': 'compressedbill.json'},
            ExpiresIn=172800)
    print(presignedUrl)
