#!/usr/bin/python3
"""
Building the ingestion engine for the binance data
"""
import os
import json
from datetime import datetime
from tqdm import tqdm
from binance.client import Client
from dotenv import load_dotenv


# Load environmental variables from the .env file
load_dotenv()


def fetch_binance_data(symbols, interval, start_time, end_time):
    """
    Fetches the data according to the start and end dates given
    """
    api_key = os.getenv("API_KEY")
    api_secret = os.getenv("API_SECRET")

    client = Client(api_key, api_secret)

    # Convert start and end times to milliseconds
    start_time_ms = int(datetime.strptime(
        start_time, "%Y-%m-%d").timestamp() * 1000)
    end_time_ms = int(datetime.strptime(
        end_time, "%Y-%m-%d").timestamp() * 1000)

    all_klines = {}

    for symbol in symbols:
        klines = {}

        for kline in tqdm(client.get_historical_klines(
            symbol, interval, start_time_ms, end_time_ms, limit=25)):
            timestamp = kline[0]

            klines[timestamp] = {
                    "symbol": symbol,
                    "open_price": kline[1],
                    "high_price": kline[2],
                    "low_price": kline[3],
                    "close_price": kline[4],
                    "volume": kline[5]
            }
            
        all_klines[symbol] = klines

    return all_klines


def model_data(data):
    """
    Models the data
    """
    modeled_data = {}

    for symbol, klines in data.items():
        modeled_klines = {}

        for timestamp, kline_data in klines.items():
            modeled_klines[timestamp] = {
                    "Symbol": kline_data["symbol"],
                    "Timestamp": timestamp,
                    "Open": kline_data["open_price"],
                    "Close": kline_data["close_price"],
                    "High": kline_data["high_price"],
                    "Low": kline_data["low_price"],
                    "Volume": kline_data["volume"]
                }
        modeled_data[symbol] = modeled_klines
    return modeled_data


def persist_data_to_file(data, filename):
    """
    Persists the data to a file locally
    """
    with open(filename, 'w') as json_file:
        json.dump(data, json_file)
    print(f"Data persisted to {filename}")


def main():
    """
    This would be the main function to run the program of extraction
    and ingestion
    """
    symbols = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "SHIBUSDT"]
    interval = Client.KLINE_INTERVAL_1DAY

    start_time = "2023-01-01"
    end_time = "2023-12-31"

    # Fetch historical data
    binance_data = fetch_binance_data(symbols, interval, start_time, end_time)

    if binance_data:
        modeled_data = model_data(binance_data)

        # Persist in JSON file
        filename = f"cyptodata_{start_time}_{end_time}.json"
        persist_data_to_file(modeled_data, filename)
    else:
        print("No data to persist")


if __name__ == "__main__":
    main()
