import boto3
import os
from airflow.models import Variable

access_key = os.environ.get("AWS_ACCESS_KEY_ID")
secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")


def create_buckets(bucket_names):
    s3 = boto3.client("s3")

    for bucket_name in bucket_names:
        try:
            s3.create_bucket(Bucket=bucket_name)
            print(f"Bucket {bucket_name} created successfully.")
        except s3.exceptions.BucketAlreadyOwnedByYou as e:
            print(f"Bucket {bucket_name} already exists.")
        except s3.exceptions.BucketAlreadyExists as e:
            print(f"Bucket {bucket_name} already exists and is owned by someone else.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")


def main():
    bucket1 = Variable.get("s3_bucket_1")
    bucket2 = Variable.get("s3_bucket_2")
    bucket3 = Variable.get("s3_bucket_3")
    bucket_names = [bucket1, bucket2, bucket3]
    create_buckets(bucket_names)


if __name__ == "__main__":
    main()
