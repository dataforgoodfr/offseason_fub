import boto3
import os
from dotenv import load_dotenv
import pandas as pd
import io
from io import StringIO
from botocore.exceptions import ClientError

# Load environment variables from .env file
# You need to create a .env file and specify on it the variables AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_REGION
# so that they can be loaded by load_doenv
load_dotenv()

access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region = "fr-par" #os.getenv("AWS_REGION")
endpoint_url = "https://s3.fr-par.scw.cloud"

# Create the S3 client
s3 = boto3.client(
    "s3",
    region_name=region,
    endpoint_url=endpoint_url,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
)

def preview_file(key, bucket_name='fub-s3', nrows=None, csv_sep=";", csv_engine="python", quotechar='"', encoding="utf-8"):
    """Download and preview the first few rows of a CSV or Excel file from S3."""
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    file_stream = io.BytesIO(obj['Body'].read())
    if key.endswith(".csv"):
        df = pd.read_csv(
            file_stream,
            nrows=nrows,
            sep=csv_sep,
            engine=csv_engine,
            quotechar=quotechar,
            encoding=encoding
        )
    elif key.endswith((".xlsx", ".xls", ".xlsm")):
        df = pd.read_excel(file_stream, nrows=nrows)
    else:
        raise ValueError(f"Unsupported file type: {key}")

    return df

def write_csv_on_s3(df, save_path, bucket_name='fub-s3', csv_sep=";", quotechar='"'):
    # Convert DataFrame to CSV in memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, sep=csv_sep, quotechar=quotechar)
    # Define S3 bucket and file path
    s3.put_object(Bucket=bucket_name, Key=save_path, Body=csv_buffer.getvalue())
    print(f"File saved on s3 at location {save_path}")

#function to list objects in bucket
def list_objects(bucket_name='fub-s3'):
    """List up to 1000 objects in a bucket."""
    print(f"Listing objects in bucket '{bucket_name}':")

    response = s3.list_objects_v2(Bucket=bucket_name)

    contents = response.get("Contents", [])
    if not contents:
        print("Bucket is empty.")
        return []

    for obj in contents:
        print(f" - {obj['Key']}")

    return [obj["Key"] for obj in contents]





a = list_objects('fub-s3')
if __name__ == '__main__':
    #df = pd.read_csv("220128_BV_Communes_catégories.csv")
    #df = preview_file(key="data/converted/2021/brut/reponses-2021-12-01-08-00-00.csv", nrows=None)
    #print('df', df.shape)
    dest_path = "data/converted/2025/brut/220128_BV_Communes_catégories.csv"
    file_path = "220128_BV_Communes_catégories.csv"
    #response = s3.upload_file(file_path, 'fub-s3', dest_path)

