import time
import pandas as pd
import requests
import pytz
from datetime import datetime, timedelta
from fyers_apiv3 import fyersModel
from services.config import redis_connection
from services.loger import logger
from psycopg2.extras import execute_batch
import psycopg2
import json

# ----------------- CONFIGURATION ----------------- #

CLIENT_ID = "client_id_here"
TOKEN = "token_here"

IST = pytz.timezone("Asia/Kolkata")

# Create a single Redis connection that is reused
redis_client = redis_connection()
redis_key = f"account_matrix:account"

# Fetch hash from Redis and decode keys/values
account_matrix = redis_client.hgetall(redis_key)
account_matrix = {k.decode('utf-8'): v.decode('utf-8') for k, v in account_matrix.items()}

# Correct: don't add commas here
start_date = account_matrix["from_date"]
end_date = account_matrix["to_date"]

# Convert to datetime
from datetime import datetime

start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")

# Calculate lookback
DEFAULT_LOOKBACK_DAYS = (end_date_dt - start_date_dt).days


# -------------------------------------------------- #

TRADING_SYSTEM_CONN_PARAMS = {
    "dbname": "trading_system",
    "user": "postgres",
    "password": "onealpha12345",
    "host": "localhost",
    "port": 5432
}
conn = psycopg2.connect(**TRADING_SYSTEM_CONN_PARAMS)

def get_last_cached_timestamp(prefix, symbol):
    """
    Get the latest timestamp for a symbol from the nse_stocks table in PostgreSQL.
    """
    query = """
        SELECT MAX(timestamp)
        FROM public.nse_stocks
        WHERE symbol = %s;
    """
    with conn.cursor() as cur:
        cur.execute(query, (symbol,))
        result = cur.fetchone()
        if result and result[0]:
            return result[0].astimezone(IST)
        return None

# def cache_data_fyers(df, symbol):
#     """
#     Store Fyers data in PostgreSQL under table 'nse_stocks'.
#     Data is upserted on conflict of (symbol, timestamp).
#     """
#     if df is None or df.empty:
#         print(f"⚠️ No new Fyers data to insert for {symbol}.")
#         return
#
#     try:
#         # Prepare data for insertion
#         rows = []
#         for record in df.to_dict(orient="records"):
#             ts = record["timestamp"]
#             if isinstance(ts, pd.Timestamp):
#                 ts = ts.to_pydatetime()
#             ts = ts.astimezone(IST)
#             rows.append((
#                 symbol,
#                 ts,
#                 record["open"],
#                 record["high"],
#                 record["low"],
#                 record["close"],
#                 record["volume"]
#             ))
#         # print('rows',rows)
#         with psycopg2.connect(**TRADING_SYSTEM_CONN_PARAMS) as conn:
#             with conn.cursor() as cur:
#                 upsert_query = """
#                     INSERT INTO public.nse_stocks (symbol, timestamp, open, high, low, close, volume)
#                     VALUES (%s, %s, %s, %s, %s, %s, %s)
#                     ON CONFLICT (symbol, timestamp) DO UPDATE
#                     SET open = EXCLUDED.open,
#                         high = EXCLUDED.high,
#                         low = EXCLUDED.low,
#                         close = EXCLUDED.close,
#                         volume = EXCLUDED.volume;
#                 """
#                 execute_batch(cur, upsert_query, rows)
#             conn.commit()
#             print(f"✅ Upserted {len(rows)} rows into 'nse_stocks' for {symbol}.")
#
#     except Exception as e:
#         print(f"⚠️ Error upserting data for {symbol}: {str(e)}")

def cache_data_fyers(df, symbol):
    """
    Store Fyers data in PostgreSQL under table 'nse_stocks'.
    Data is upserted on conflict of (symbol, timestamp).
    Drops current (incomplete) candle and processes only complete candles up to the last minute.
    """
    if df is None or df.empty:
        print(f"⚠️ No new Fyers data to insert for {symbol}.")
        return

    try:
        # Get current time in IST
        current_time = datetime.now(IST)
        # Calculate the end of the last complete minute
        last_complete_minute = current_time.replace(second=0, microsecond=0) - timedelta(minutes=1)

        # Filter out the current candle (incomplete candle)
        df = df[df['timestamp'] <= last_complete_minute]

        if df.empty:
            print(f"⚠️ No complete candle data to insert for {symbol} after filtering.")
            return

        # Prepare data for insertion
        rows = []
        for record in df.to_dict(orient="records"):
            ts = record["timestamp"]
            if isinstance(ts, pd.Timestamp):
                ts = ts.to_pydatetime()
            ts = ts.astimezone(IST)
            rows.append((
                symbol,
                ts,
                record["open"],
                record["high"],
                record["low"],
                record["close"],
                record["volume"]
            ))

        # Insert data into the database
        with psycopg2.connect(**TRADING_SYSTEM_CONN_PARAMS) as conn:
            with conn.cursor() as cur:
                upsert_query = """
                    INSERT INTO public.nse_stocks (symbol, timestamp, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, timestamp) DO UPDATE
                    SET open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume;
                """
                execute_batch(cur, upsert_query, rows)
            conn.commit()
            print(f"✅ Upserted {len(rows)} rows into 'nse_stocks' for {symbol}.")

    except Exception as e:
        print(f"⚠️ Error upserting data for {symbol}: {str(e)}")

def fetch_fyers_data(symbol: str, start_dt: datetime, end_dt: datetime):
    print("called fetch data from fyers api")
    all_data = []
    chunk_start = start_dt
    print(f"⏳ Fetching Fyers data for {symbol} from {start_dt} to {end_dt}...")
    while chunk_start < end_dt:
        chunk_end = min(chunk_start + timedelta(days=100), end_dt)
        from_sec = int(chunk_start.timestamp())
        to_sec = int(chunk_end.timestamp())
        # print('symbol', symbol, 'chunk_start', chunk_start, 'chunk_end',chunk_end)
        data = {
            "symbol": f"NSE:{symbol}-EQ",
            "resolution": "1",  # 1-minute data
            "date_format": "0",
            "range_from": str(from_sec),
            "range_to": str(to_sec),
            "cont_flag": "1",
        }

        fyers = fyersModel.FyersModel(client_id=CLIENT_ID, is_async=False, token=TOKEN, log_path="")

        retries = 5
        wait_time = 10  # initial wait time in seconds
        response = None

        while retries:
            try:
                response = fyers.history(data=data)

                if response.get("code") == 429:  # Rate limit reached
                    wait_time = int(response.get("retry_after", wait_time))  # Get retry-after if provided
                    logger.warning(f"Rate limit reached. Waiting for {wait_time} seconds before retrying...")
                    time.sleep(wait_time)
                    retries -= 1
                    wait_time *= 2  # Exponential backoff
                    continue
                break  # Successful response

            except requests.exceptions.RequestException as e:
                logger.error(f"Request error for {symbol}: {e}")
                break

        if response is None or response.get("code") == 429:
            logger.error(
                f"Rate limit still reached for {symbol} from {chunk_start} to {chunk_end}. Skipping this chunk.")
            chunk_start = chunk_end + timedelta(minutes=1)
            continue

        candles = response.get("candles", [])
        # print('candles', candles)
        if candles:
            for c in candles:
                ts_ist = datetime.fromtimestamp(c[0], tz=IST)
                all_data.append({
                    "symbol": symbol,
                    "timestamp": ts_ist,
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume": float(c[5]),
                })
        else:
            logger.warning(f"No data returned for {symbol} in the requested period.")

        # Move to the next chunk (adding 1 minute gap between chunks)
        chunk_start = chunk_end + timedelta(minutes=1)

    if not all_data:
        return pd.DataFrame()
    return pd.DataFrame(all_data)


def fill_and_cache_fyers(symbol: str):
    """
    For Fyers:
    If no cached data exists, fetch the last DEFAULT_LOOKBACK_DAYS of data.
    Otherwise, determine the gap from the last cached timestamp to current time and fill it.
    """
    prefix = "nse"
    last_ts = get_last_cached_timestamp(prefix, symbol)
    now_ist = datetime.now(IST)
    if last_ts is None:
        start_dt = now_ist - timedelta(days=DEFAULT_LOOKBACK_DAYS)
        print(f"⏳ No existing data for {symbol}. Fetching last {DEFAULT_LOOKBACK_DAYS} days from {start_dt} to {now_ist}...")
    else:
        start_dt = last_ts + timedelta(minutes=1)
        if start_dt >= now_ist:
            print(f"✅ {symbol} is already up-to-date. No gaps to fill.")
            return
        # print(f"⏳ Filling Fyers gap for {symbol} from {start_dt} to {now_ist}...")

    df = fetch_fyers_data(symbol, start_dt, now_ist)
    if not df.empty:
        cache_data_fyers(df, symbol)
    else:
        print(f"⚠️ No data returned for {symbol} in the requested period.")


def fyers_Symbol_gap_filler(symbol: str):
    """
    Fills missing data for a single symbol.
    """
    fill_and_cache_fyers(symbol)
