import boto3
from os import getenv
import logging
import requests
import os
import sys
from dotenv import load_dotenv
from botocore.exceptions import ClientError  # Added import

load_dotenv()


def init_client():
  try:
    client = boto3.client(
        "s3",
        aws_access_key_id=getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=getenv("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=getenv("AWS_SESSION_TOKEN"),
        region_name=getenv("AWS_DEFAULT_REGION"),
    )
    client.list_buckets()
    return client
  except ClientError as e:
    logging.error(e)
    sys.exit(1)


def list_buckets(aws_s3_client):
  try:
    return aws_s3_client.list_buckets()
  except ClientError as e:
    logging.error(e)
    return False


def create_bucket(aws_s3_client, bucket_name, region='us-west-2'):
  try:
    location = {'LocationConstraint': region}
    aws_s3_client.create_bucket(Bucket=bucket_name,
                                CreateBucketConfiguration=location)
    return True
  except ClientError as e:
    logging.error(f'Error creating bucket {bucket_name}: {e}')
    return False


def delete_bucket(aws_s3_client, bucket_name):
  try:
    aws_s3_client.delete_bucket(Bucket=bucket_name)
    return True
  except ClientError as e:
    logging.error(f'Error deleting bucket {bucket_name}: {e}')
    return False


def bucket_exists(aws_s3_client, bucket_name):
  try:
    response = aws_s3_client.head_bucket(Bucket=bucket_name)
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
      return True
  except ClientError as e:
    logging.error(e)
  return False


def set_object_access_policy(aws_s3_client,
                             bucket_name,
                             file_name,
                             region='us-west-2'):
  try:
    response = aws_s3_client.put_object_acl(ACL="public-read",
                                            Bucket=bucket_name,
                                            Key=file_name)
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
      return True
  except ClientError as e:
    logging.error(e)
  return False


def download_file(url, local_filename):
  with requests.get(url, stream=True) as r:
    r.raise_for_status()
    with open(local_filename, 'wb') as f:
      for chunk in r.iter_content(chunk_size=8192):
        f.write(chunk)
  return local_filename


def download_file_and_upload_to_s3(s3_client,
                                   bucket_name,
                                   url,
                                   file_name,
                                   keep_local=False):
  try:
    local_file = download_file(url, file_name)
    s3_client.upload_file(local_file, bucket_name, file_name)
    if not keep_local:
      os.remove(local_file)
    return True
  except Exception as e:
    logging.error(f"Error downloading/uploading file: {e}")
    return False


if __name__ == "__main__":
  s3_client = init_client()
  buckets = list_buckets(s3_client)

  # Public access to image
  print(
      f"Set read status: {set_object_access_policy(s3_client, 'myaws42141412412414141', 'test.jpg')}"
  )

  if buckets:
    for bucket in buckets["Buckets"]:
      print(
          f"\nBucket Name: {bucket['Name']}, Creation Time: {bucket['CreationDate']}\n"
      )

  # Download and upload image
  download_file_and_upload_to_s3(
      s3_client,
      'myaws42141412412414141',
      'https://media.istockphoto.com/id/1318638557/vector/100-points.jpg?s=612x612&w=0&k=20&c=0Ma5cgxh9qS8MJs5I_vBfziOOr2LSNLUlA0_XWAVnW8=',
      'test.jpg',
      keep_local=True)
