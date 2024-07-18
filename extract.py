#!/usr/bin/env python3
"""
This script extracts daily candlestick data from the Binance API,
stores it locally,
and uploads it to S3.
"""
import os
from binance.client import Client
import pandas as pd
import boto3
from dotenv import load_dotenv
from tqdm import tqdm
from datetime import datetime, timedelta

# Load environmental variables from the .env file
load_dotenv()

api_key = os.getenv("BINANCE_API_KEY")
api_secret = os.getenv("BINANCE_SECRET_KEY")
bucket_name = os.getenv("S3_BUCKET_NAME")

client = Client(api_key, api_secret)


def fetch_historical_candlestick(currency_pairs, start_date, end_date):
    """
    This fetches historical data for the currency
    pairs within the given date range.
    """
    all_data = []

    for pair in tqdm(currency_pairs, desc="Fetching data", unit="pair"):
        klines = client.get_historical_klines(pair, Client.KLINE_INTERVAL_1DAY,
                                              start_date, end_date)
        data = pd.DataFrame(klines, columns=[
            'open_time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
            'ignore'
        ])
        data['open_time'] = pd.to_datetime(data['open_time'], unit='ms')
        data['close_time'] = pd.to_datetime(data['close_time'], unit='ms')
        data['currency_pair'] = pair
        all_data.append(data[[
            'open_time', 'close_time', 'open', 'high', 'low', 'close',
            'volume', 'currency_pair'
        ]])

    return pd.concat(all_data, ignore_index=True)


def save_data_locally(data, filename, append=True):
    """
    Saves the data locally. Appends to the file if it exists.
    """
    if append and os.path.exists(f'local_data/{filename}.csv'):
        existing_data = pd.read_csv(f'local_data/{filename}.csv')
        data = pd.concat([existing_data, data]).drop_duplicates(subset=[
            'open_time', 'currency_pair']).reset_index(drop=True)

    data.to_csv(f'data/{filename}.csv', index=False)


def upload_data_to_s3(filename, bucket_name, object_key):
    """
    Persist the data in S3 bucket.
    """
    s3_client = boto3.client('s3')
    with open(f'data/{filename}.csv', 'rb') as data:
        s3_client.put_object(Body=data, Bucket=bucket_name, Key=object_key)


if __name__ == '__main__':
    currency_pairs = [
        'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'DOGEUSDT', 'XRPUSDT'
    ]

    # Fetch historical data from the past 30 days as an example
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

    historical_data = fetch_historical_candlestick(
            currency_pairs, start_date, end_date)

    # Save data locally as CSV files
    filename = f'daily_candlestick_data_{datetime.now().strftime("%Y%m%d")}'
    save_data_locally(historical_data, filename)

    # Persist in S3 bucket
    upload_data_to_s3(filename, bucket_name, f'{filename}.csv')
