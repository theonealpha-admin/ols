import json
import csv
import os
import pandas as pd
from datetime import datetime, timedelta
import pytz
from services.ols.ols_recipe import SpreadCalculator
from services.loger import logger
from services.config import redis_connection
from services.db_config import get_db_connection
import decimal
IST = pytz.timezone("Asia/Kolkata")
import warnings

# Suppress all runtime warning
warnings.filterwarnings("ignore", category=RuntimeWarning)

# Initialize Redis client (adjust as needed)
redis_client = redis_connection()


def now_ist():
    return datetime.now(IST).replace(second=0, microsecond=0)


def get_cached_ohlc_data(symbol, start=None, end=None, ohlc_prefix="ohlc"):
    try:
        symbol = symbol.upper()

        start_time = start if start else datetime(2025, 5, 19, tzinfo=IST)
        end_time = end if end else datetime.now(IST)

        query = """
            SELECT symbol, timestamp, open, high, low, close, volume
            FROM public.nse_stocks
            WHERE symbol = %s
              AND timestamp >= %s
              AND timestamp < %s
            ORDER BY timestamp ASC;
        """

        conn = get_db_connection()
        cursor = conn.cursor()

        current_time = start_time
        chunk_delta = timedelta(days=1000)
        all_rows = []

        while current_time < end_time:
            next_time = min(current_time + chunk_delta, end_time)
            params = (symbol, current_time, next_time)
            cursor.execute(query, params)
            all_rows.extend(cursor.fetchall())
            current_time = next_time

        columns = [desc[0] for desc in cursor.description]
        cursor.close()
        conn.close()

        if not all_rows:
            logger.warning(f"No OHLC data found in DB for {symbol} between {start_time} and {end_time}")
            return pd.DataFrame()

        df = pd.DataFrame(all_rows, columns=columns)

        # Convert Decimal to float for price and volume columns (if needed)
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in df.columns and df[col].dtype == object:
                if df[col].apply(lambda x: isinstance(x, decimal.Decimal)).any():
                    df[col] = df[col].astype(float)

        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(IST)
            df.sort_values("timestamp", inplace=True)
        else:
            logger.error(f"No 'timestamp' column in DB data for {symbol}")
            return pd.DataFrame()

        return df

    except Exception as e:
        logger.error(f"Error fetching OHLC data for {symbol} from DB: {e}", exc_info=True)
        print(f"DEBUG: Error retrieving OHLC data for {symbol}: {e}")
        return pd.DataFrame()

def calculate_spread(sym1, sym2, start_time=None, ohlc_prefix="ohlc"):
    exchange = "nse" if ohlc_prefix == "nse" else "binance"
    calculator = SpreadCalculator(exchange)

    # Fetch the OHLC data
    df1 = get_cached_ohlc_data(sym1, start=start_time, ohlc_prefix=ohlc_prefix)
    df2 = get_cached_ohlc_data(sym2, start=start_time, ohlc_prefix=ohlc_prefix)

    # Fetch the window size from Redis
    redis_key = "account_matrix:account"
    account_matrix = redis_client.hgetall(redis_key)
    account_matrix = {k.decode('utf-8'): v.decode('utf-8') for k, v in account_matrix.items()}
    window = int(account_matrix["window"])

    # Call the instance method
    return calculator.calculate_historical_spread(df1, df2, window)



def get_cached_spread_data(spread_prefix, pair_name):
    try:
        redis_key = f"{spread_prefix}_{pair_name}".lower()
        records = redis_client.zrange(redis_key, 0, -1)
        data = []
        for rec in records:
            d = json.loads(rec)
            d["timestamp"] = pd.to_datetime(d["timestamp"], utc=True).tz_convert(IST)
            data.append(d)
        if data:
            df = pd.DataFrame(data)
            df.sort_values("timestamp", inplace=True)
            return df
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error in get_cached_spread_data: {e}", exc_info=True)
        # print(f"DEBUG: Error getting cached spread data: {e}")
        return pd.DataFrame()


def fetch_account_matrix(redis_client):
    redis_key = "account_matrix:account"
    try:
        account_matrix = redis_client.hgetall(redis_key)
        window_value = account_matrix.get(b'window')
        return {"window": window_value.decode('utf-8')} if window_value else {}
    except Exception as e:
        logger.error(f"Error in fetch_account_matrix: {e}", exc_info=True)
        return {}


# def get_last_spread_timestamp(redis_client, spread_prefix, pair_name):
#     try:
#         redis_key = f"{spread_prefix}_{pair_name}".lower()
#         # print(f"Redis key in get_last_spread_timestamp: {redis_key}")
#         window_data = fetch_account_matrix(redis_client)
#         window_value = int(window_data.get("window", -1)) + 20
#         entry = redis_client.zrange(redis_key, -window_value, -window_value, withscores=True)
#         if entry:
#             return datetime.fromtimestamp(entry[0][1], tz=IST)
#         return None
#     except Exception as e:
#         logger.error(f"Error in get_last_spread_timestamp: {e}", exc_info=True)
#         print(f"DEBUG: Error getting last spread timestamp: {e}")
#         return None

def get_last_spread_timestamp(pair_name):
    try:
        table_name = "public.nse_spreads"
        query = f"""
            SELECT *
            FROM {table_name}
            WHERE symbol = %s
            ORDER BY "timestamp" DESC
            LIMIT 1;
        """
        # print("query", query)
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, (pair_name,))
        result = cursor.fetchall()
        cursor.close()
        conn.close()

        if result:
            # Get the last row's timestamp (since ordered ASC, last of 100 is latest)
            timestamp = result[-1][1]  # Assuming timestamp is second column (index 1) after symbol
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=IST)
            return timestamp
        return None
    except Exception as e:
        logger.error(f"Error in get_last_spread_timestamp: {e}", exc_info=True)
        print(f"DEBUG: Error getting last spread timestamp: {e}")
        return None

def insert_spread_data_to_db(spread_df, pair_name, exchange_prefix):
    try:
        if spread_df.empty:
            return

        spread_df['timestamp'] = pd.to_datetime(spread_df['timestamp']).dt.tz_convert(IST)
        spread_df['symbol'] = pair_name.lower()

        insert_query = """
            INSERT INTO public.nse_spreads (symbol, timestamp, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, timestamp) DO NOTHING;
        """

        data_tuples = [
            (
                row["symbol"],
                row["timestamp"],
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                float(row["volume"]),
                # float(row["slope"]),
            )
            for _, row in spread_df.iterrows()
        ]

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.executemany(insert_query, data_tuples)
        conn.commit()

        logger.info(f"Inserted {len(data_tuples)} records into DB for {pair_name}")
        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(f"DB Insert Error: {e}")

def delete_spreads_name(exchange_prefix):
    """
    Saves spread symbols into Redis. Optionally clears the existing set if reset=True.
    """
    try:
        if exchange_prefix.lower() in ("nse", "fyers"):
            redis_spread_key = 'spreads:nse_spreads_name'
        else:
            redis_spread_key = 'spreads:binance_spreads_name'
        redis_client.delete(redis_spread_key)
    except Exception as e:
        logger.error(f" Error delete_spreads_name for {exchange_prefix}: {e}", exc_info=True)

from datetime import datetime, timedelta, time
# Define NSE trading hours
MARKET_OPEN = time(9, 15)
MARKET_CLOSE = time(15, 30)

def is_trading_time(dt: datetime) -> bool:
    """Check if datetime is within trading hours on a weekday."""
    return dt.weekday() < 5 and MARKET_OPEN <= dt.time() <= MARKET_CLOSE

def get_previous_trading_minute(dt: datetime) -> datetime:
    """Get the previous valid trading minute."""
    one_min = timedelta(minutes=1)
    while True:
        dt -= one_min
        if is_trading_time(dt):
            return dt

def subtract_trading_minutes(start: datetime, minutes: int) -> datetime:
    """Go back in trading minutes, skipping weekends and non-market hours."""
    current = start
    for _ in range(minutes):
        current = get_previous_trading_minute(current)
    return current

def save_spread_symbol_to_db(spread_symbols, exchange_prefix):
    """
    Saves spread symbols into Redis. Optionally clears the existing set if reset=True.
    """
    try:
        if exchange_prefix.lower() in ("nse", "fyers"):
            redis_spread_key = 'spreads:nse_spreads_name'
        else:
            redis_spread_key = 'spreads:binance_spreads_name'

        # Only clear Redis set if explicitly requested
        # if reset:
        #     redis_client.delete(redis_spread_key)

        if spread_symbols:
            if isinstance(spread_symbols, str):
                spread_symbols = [spread_symbols]

            cleaned = [
                symbol.replace('nse:', '').replace('-eq', '').lower()
                for symbol in spread_symbols
            ]

            redis_client.sadd(redis_spread_key, *cleaned)

            print(f"Updated Redis set {redis_spread_key} with {len(cleaned)} symbols: {cleaned}")
        else:
            print(f"No spread symbols provided to save for {exchange_prefix}.")

    except Exception as e:
        logger.error(f"Error saving spread symbols for {exchange_prefix}: {e}", exc_info=True)


def fill_historical_gaps(pair_name, exchange_prefix, calculator):
    calculator = SpreadCalculator(exchange_prefix)
    """
    Uses the last spread timestamp to compute only the gap spread data up to the current time.
    If no previous data exists, computes the full spread data from the earliest available OHLC data.
    """
    ACCOUNT_KEY = "account_matrix:account"
    window_raw = redis_client.hget(ACCOUNT_KEY, "window")
    window = int(window_raw) if window_raw is not None else None
    # print("window =", window)

    try:
        spread_prefix = "binance_spreads" if exchange_prefix.lower() == "binance" else "nse_spreads"
        last_spread = get_last_spread_timestamp(pair_name)
        lookback_start = subtract_trading_minutes(last_spread, window)
        if exchange_prefix.lower() == "nse":
            sym1, sym2 = pair_name.replace("nse_spreads_", "").split("_")
        else:
            sym1, sym2 = pair_name.split("_")

        ohlc_prefix = "binance" if exchange_prefix.lower() == "binance" else "nse"

        if lookback_start:
            new_start = lookback_start + timedelta(minutes=1)
        else:
            # If no previous data exists, start from the earliest available OHLC data
            new_start = None
            logger.info(f"No previous spread data for {pair_name}. Computing full spread data from the beginning.")

        spread_data = calculate_spread(sym1, sym2, start_time=new_start, ohlc_prefix=ohlc_prefix)
        # print("spread_data", spread_data.tail())

        if spread_data.empty:
            logger.info(f"No new spread data computed for {pair_name}.")
            return

        insert_spread_data_to_db(spread_data, pair_name, exchange_prefix)
        save_spread_symbol_to_db(pair_name, exchange_prefix)
    except Exception as e:
        logger.error(f"Error in fill_historical_gaps for {pair_name}: {e}", exc_info=True)
        print(f"DEBUG: Error in gap fill for {pair_name}: {e}")

def process_exchange_symbols(exchange_name, sym1, sym2):
    print("started spreads process for nse")
    exchange_prefix = exchange_name
    try:
        calculator = SpreadCalculator(exchange_prefix)
        pair_name = calculator.generate_pair_name(sym1, sym2)
        fill_historical_gaps(pair_name, exchange_prefix, calculator)
        return True
    except Exception as e:
        logger.error(f"Spreads_gap_filler Pair processing error: {e}")
        return False

process_exchange_symbols("nse", "RELIANCE", "TCS")