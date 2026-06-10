import boto3
# Initialize S3 cloud client
s3_client = boto3.client('s3')
response = s3_client.create_bucket(Bucket='my-cse-cloud-bucket')
print("Bucket created:", response)
