#!/usr/bin/python3
"""
This script loads the transformed data from a CSV file into a PostgresSQL
database or warehouse
"""
import os
import pandas as pd
from dotenv import load_dotenv
import psycopg2

load_dotenv()

host = os.getenv("PG_HOST")
database = os.getenv("PG_DATABASE")
user = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
port = os.getenv("PG_PORT")


def write_data_to_postgresql(data, table_name):
    """
    Writes data to a Postgres Database
    """
    conn = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )

    cur = conn.cursor()

    for i, row in data.iterrows():
        cur.execute(f"""
            INSERT INTO {table_name} (
                open_time, close_time, open_price, high_price, low_price,
                close_price, volume, currency_pair
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (open_time, currency_pair)
            DO NOTHING;
        """, tuple(row))

    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    new_file = 'local_data/transformed_daily_candlestick_data_20240527.csv'

    data = pd.read_csv(new_file)

    write_data_to_postgresql(data, 'candlestic_data')
