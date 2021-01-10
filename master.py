import boto3
import json
import gzip
import botocore
import boto3


def lambda_handler(event, context):
    # TODO implement
    billClient = boto3.client('ce')
    # first step: 
    # connecting to cost explorer to get monthly(December) aws usage
    # try/exception: if exception:raise error
    try:
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
    except botocore.exceptions.ClientError as error:
        print("someting wrong when fetching cost and uasge")
        raise error
    # creating table(bills) for storing billData.
    dynamodbClient = boto3.client('dynamodb')
    # second step:
    # putting bill data into table(bills)
    # try/exception: creating table if table(bills) does not exist,
    # otherwise using existing table(bills),
    # otherwise raise error
    try:
        table = dynamodbClient.create_table(
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
    except dynamodbClient.exceptions.ResourceInUseException:
        dynamodbDB = boto3.resource('dynamodb')
        table = dynamodbDB.Table('bills')
    except botocore.exceptions.ClientError as error:
        print("something wrong")
        raise error
    # putting billData items into the table(bills)
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
    # third step:
    # creting S3 bucket: storageforbills
    # try/exception: creating bucket if bucket does not exist, 
    # otherwise using existing bucket,
    # otherwise raise error
    s3Client = boto3.client('s3')
    try:
        jsonForStorage = json.dumps(items)
        s3Client.create_bucket(Bucket='storageforbills')
    except s3Client.exceptions.BucketAlreadyExists:
        pass
    except botocore.exceptions.ClientError as error:
        print("someting wrong when using bucket")
        raise error
    # creating uncompressed bill.json and storing it in the bucket:storageforbills
    s3Client.put_object(
        ACL='private',
        Body=jsonForStorage,
        Key='bill.json',
        Bucket='storageforbills')

    # compressing jsonForStorage by zip and storing it in the storageforbills bucket
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
