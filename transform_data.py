#!/usr/bin/env python3
"""
The script fetches candlestick data from s3, transforms it,
and saves it locally for loading into PostgreSQL
"""
import os
import pandas as pd
import boto3
from dotenv import load_dotenv
from io import BytesIO


load_dotenv()


bucket_name = os.getenv("S3_BUCKET_NAME")


def download_data_from_s3(filename, bucket_name):
    """
    Downloads the data from s3 bucket
    """
    s3_client = boto3.client("s3")
    obj = s3_client.get_object(Bucket=bucket_name, Key=filename)
    return pd.read_csv(BytesIO(obj['Body'].read()))


def transform_data(data):
    """
    Transforms data into suitable format to load into PostgreSQL
    """

