# IMPORTING LIBRARIES
import boto3
import csv
import json
from io import StringIO


# PARAMETERS
S3_BUCKET = 'aqi.data.113'
S3_key = 'openaq_location_6946_measurment.csv'
AWS_REGION = 'us-east-1'

# Create a Local Session to login into our AWS
session = boto3.session.Session(
    region_name=AWS_REGION,
    aws_access_key_id='your_access_key', 
    aws_secret_access_key='your_secret_key'
)

S3 = session.client("s3")
Kinesis = session.client("kinesis")


def read_csv_from_s3(bucket, key):
    response = S3.get_object(Bucket=bucket, Key=key)
    csv_content = response['Body'].read().decode("utf-8")
    reader = csv.DictReader(StringIO(csv_content))
    return list(reader)



def put_kinesis(records):
    for i, record in enumerate(records, 1):
        try:
            Kinesis.put_record(
                StreamName='aqi_data',
                Data=json.dumps(record),
                PartitionKey=record.get("location_id", "default")
            )
        except Exception as e:
            print("The Error which we have encountered = ", (e))


def main():
    records = read_csv_from_s3(S3_BUCKET, S3_key)
    print("We have got the records from S3 Bucket")
    put_kinesis(records=records)
    print("data has been uploaded to Kinesis Data Stream.")


main()