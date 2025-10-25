import json
import re
import time
from collections import defaultdict
import asyncio
import numpy as np
import pandas as pd
import pytz
from datetime import datetime, timedelta
from threading import Lock, Thread
from typing import Optional, Dict, Tuple, List
from services.loger import logger
from services.config import redis_client
import psycopg2
from psycopg2 import sql

IST = pytz.timezone("Asia/Kolkata")

class SpreadCalculator:
    def __init__(self, exchange: str):
        self.exchange = exchange.lower()
        self.redis_prefix = self._get_redis_prefix()
        self._validate_exchange()
        self.temp_tray = []
        self.initial_check = 0
        self.slope_dict = {}
        self.last_run_time = None
        self.r = redis_client
        # Database connection
        self.conn = psycopg2.connect(
            dbname="trading_system",
            user="postgres",
            password="onealpha12345",
            host="localhost",
            port=5432
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def _validate_exchange(self):
        if self.exchange not in ["nse", "binance"]:
            raise ValueError(f"Unsupported exchange: {self.exchange}")

    def _get_redis_prefix(self) -> str:
        return {
            "nse": "nse_spreads",
            "binance": "binance_spreads"
        }[self.exchange]

    def clean_symbol(self, symbol: str) -> str:
        """Standardize symbol formatting across exchanges"""
        if self.exchange == "nse":
            return symbol.replace('NSE:', '').replace('-EQ', '').lower()
        return symbol.replace('/', '').replace(':', '').lower()

    def generate_pair_name(self, sym1: str, sym2: str) -> str:
        """Generate consistent pair name for Redis keys"""
        return f"{self.clean_symbol(sym1)}_{self.clean_symbol(sym2)}"

    def fetch_latest_spreads(self, key: str, limit: int = 3) -> list:
        table_name = f"{self.exchange}_spreads"
        print(f"Fetching latest spreads for key: {key} from table: {table_name} with limit: {limit}")
        query = """
            SELECT symbol, slope
            FROM %s
            WHERE symbol LIKE %%s
            ORDER BY timestamp DESC
            LIMIT %%s
        """ % table_name

        try:
            self.cursor.execute(query, (f"%{key}%", limit))
            return self.cursor.fetchall()
        except Exception as e:
            print(f"ERROR: Database fetch failed - {str(e)}")
            return []

    def calculate_spread(self, sym1: str, sym2: str, price1: float, price2: float):
        try:
            if self.exchange == 'nse':
                sym1_extracted = sym1.split(":")[1].split("-")[0].lower()
                sym2_extracted = sym2.split(":")[1].split("-")[0].lower()
                key = f"{sym1_extracted}_{sym2_extracted}"
            else:
                key = f"{sym1.lower()}_{sym2.lower()}"

            dict_key = sym1 + "_" + sym2

            # First time fetch or periodic update
            if self.initial_check == 0 or self.last_run_time is None or datetime.now() >= self.last_run_time + timedelta(
                    seconds=60):
                db_data = self.fetch_latest_spreads(key)
                self.temp_tray = db_data
                self.last_run_time = datetime.now()
                self.initial_check = 1

                for row in db_data:
                    symbol = row[0]
                    slope = row[1]
                    if slope is not None:
                        self.slope_dict[symbol] = slope

            if len(self.temp_tray) == 0:
                return None

            slope = self.slope_dict.get(key)
            if slope is not None:
                return price2 - (slope * price1)

        except Exception as e:
            print(f"ERROR: Spread calculation failed - {e}")
            return None

    def _fetch_ohlc_from_db(self, symbol: str, start_time: str, end_time: str = None, retries: int = 2,
                            delay: float = 1) -> list:

        table_name = f"{self.exchange}_stocks"
        # print("table_name",table_name)
        window = getattr(self, 'window', 2) + 2
        try:
            datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            if end_time:
                datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            logger.error(f"Invalid time format for {symbol}: start_time={start_time}, end_time={end_time}")
            return []

        conn_params = {
            "dbname": "trading_system",
            "user": "postgres",
            "password": "onealpha12345",
            "host": "localhost",
            "port": 5432
        }

        for attempt in range(retries):
            try:
                with psycopg2.connect(**conn_params) as conn:
                    with conn.cursor() as cur:
                        query = sql.SQL("""
                            SELECT symbol, timestamp, open, high, low, close, volume
                            FROM public.{table}
                            WHERE symbol = %s AND timestamp >= %s
                            {end_condition}
                            ORDER BY timestamp DESC
                            LIMIT %s
                        """).format(
                            table=sql.Identifier(table_name),
                            end_condition=sql.SQL("AND timestamp <= %s") if end_time else sql.SQL("")
                        )
                        params = [symbol, start_time]
                        if end_time:
                            params.append(end_time)
                        params.append(window)
                        cur.execute(query, params)
                        rows = cur.fetchall()
                        parsed_data = []
                        for row in rows:
                            try:
                                timestamp_str = row[1].strftime("%Y-%m-%d %H:%M:%S%z")  # Outputs +0530
                                parsed_data.append({
                                    "symbol": row[0],
                                    "timestamp": timestamp_str,
                                    "open": float(row[2]),
                                    "high": float(row[3]),
                                    "low": float(row[4]),
                                    "close": float(row[5]),
                                    "volume": float(row[6])
                                })
                            except (ValueError, TypeError) as e:
                                logger.warning(f"Invalid row for {symbol}: timestamp={row[1]}, error={e}")
                                continue
                        if not parsed_data:
                            logger.warning(f"No valid data fetched for {symbol} from {start_time} to {end_time}")
                        return parsed_data
            except Exception as e:
                logger.error(f"Error fetching data for {symbol}: {e}")
                if attempt < retries - 1:
                    time.sleep(delay)
        return []

    def calculate_historical_spread(self, df1: pd.DataFrame, df2: pd.DataFrame, window: int) -> pd.DataFrame:
        if df1.empty or df2.empty:
            logger.error("One or both input DataFrames are empty")
            return pd.DataFrame()

        # Rename columns
        df1 = df1.rename(columns={"open": "open_1", "high": "high_1", "low": "low_1", "close": "close_1", "volume": "volume_1"})
        df2 = df2.rename(columns={"open": "open_2", "high": "high_2", "low": "low_2", "close": "close_2", "volume": "volume_2"})

        # Ensure timestamp is string with timezone
        for df, name in [(df1, "df1"), (df2, "df2")]:
            if df["timestamp"].dtype != "object":
                try:
                    df["timestamp"] = pd.to_datetime(df["timestamp"])
                    if df["timestamp"].dt.tz is None:
                        df["timestamp"] = df["timestamp"].dt.tz_localize("Asia/Kolkata")
                    df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S%z")
                except Exception as e:
                    logger.error(f"Failed to convert {name} timestamps to strings: {e}")
                    return pd.DataFrame()
            valid_format = df["timestamp"].apply(lambda x: isinstance(x, str) and bool(
                re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\+\d{4}$', x)) if pd.notnull(x) else False)
            if not valid_format.all():
                logger.error(f"Invalid timestamps in {name}: {df['timestamp'][~valid_format].tolist()}")
                return pd.DataFrame()

        # Sort by timestamp
        df1 = df1.sort_values("timestamp")
        df2 = df2.sort_values("timestamp")
        self.window = window

        # Fetch additional data if needed
        for df, name, suffix in [(df1, "df1", "_1"), (df2, "df2", "_2")]:
            if len(df) < window:
                symbol = df["symbol"].iloc[0] if "symbol" in df.columns else "unknown"
                end_time = df["timestamp"].iloc[0][:19]  # Remove timezone for DB query
                start_time = (pd.to_datetime(df["timestamp"].iloc[0]) - timedelta(minutes=window * 2)).strftime("%Y-%m-%d %H:%M:%S")
                logger.info(f"Fetching data for {symbol} from {start_time} to {end_time}")
                extra_data = self._fetch_ohlc_from_db(symbol, start_time, end_time)
                if extra_data:
                    extra_df = pd.DataFrame(extra_data).rename(columns={
                        "open": f"open{suffix}", "high": f"high{suffix}", "low": f"low{suffix}",
                        "close": f"close{suffix}", "volume": f"volume{suffix}"
                    })
                    try:
                        extra_df["timestamp"] = pd.to_datetime(extra_df["timestamp"])
                        if extra_df["timestamp"].dt.tz is None:
                            extra_df["timestamp"] = extra_df["timestamp"].dt.tz_localize("Asia/Kolkata")
                        extra_df["timestamp"] = extra_df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S%z")
                    except Exception as e:
                        logger.error(f"Timestamp localization/formatting error: {e}")
                        continue

                    valid_format = extra_df["timestamp"].apply(lambda x: isinstance(x, str) and bool(
                        re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\+\d{4}$', x)) if pd.notnull(x) else False)
                    if not valid_format.all():
                        logger.error(f"Invalid timestamps in fetched data for {symbol}: {extra_df['timestamp'][~valid_format].tolist()}")
                        continue

                    df = pd.concat([df, extra_df]).drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
                    if name == "df1":
                        df1 = df
                    else:
                        df2 = df

        # Final timestamp validation
        for df, name in [(df1, "df1"), (df2, "df2")]:
            valid_format = df["timestamp"].apply(lambda x: isinstance(x, str) and bool(
                re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\+\d{4}$', x)) if pd.notnull(x) else False)
            if not valid_format.all():
                logger.error(f"Invalid timestamps in final {name}: {df['timestamp'][~valid_format].tolist()}")
                return pd.DataFrame()

        try:
            df1["timestamp_dt"] = pd.to_datetime(df1["timestamp"])
            df2["timestamp_dt"] = pd.to_datetime(df2["timestamp"])

            merged = pd.merge_asof(
                df1,
                df2,
                left_on="timestamp_dt",
                right_on="timestamp_dt",
                direction="backward",
                tolerance=pd.Timedelta(seconds=60)
            ).ffill()

            if merged.empty:
                logger.error("Merged DataFrame is empty")
                return pd.DataFrame()

            x = merged["close_1"]
            y = merged["close_2"]
            xy_rolling = (x * y).rolling(window).sum()
            xx_rolling = (x * x).rolling(window).sum()

            if xy_rolling.isna().all() or xx_rolling.isna().all():
                logger.error("Rolling calculations resulted in all NaN values")
                return pd.DataFrame()

            merged["slope_close"] = xy_rolling / xx_rolling
            spread = pd.DataFrame({
                "timestamp": merged["timestamp_x"],
                "open": merged["open_2"] - merged["open_1"] * merged["slope_close"],
                "high": merged["high_2"] - merged["high_1"] * merged["slope_close"],
                "low": merged["low_2"] - merged["low_1"] * merged["slope_close"],
                "close": merged["close_2"] - merged["close_1"] * merged["slope_close"],
                "volume": 0,
                "slope": merged["slope_close"]
            }).dropna()

            return spread

        except Exception as e:
            logger.error(f"Merge error: {e}")
            return pd.DataFrame()

        finally:
            if hasattr(self, 'window'):
                del self.window

    def get_redis_key(self, pair_name: str) -> str:
        """Get formatted Redis key for spread storage"""
        return f"{self.redis_prefix}_{pair_name}".lower()


class SpreadPairProcessor:
    def __init__(self, exchange: str, symbols: list):
        self.calculator = SpreadCalculator(exchange)
        self.symbols = symbols
        self.real_time_data = {}
        self.data_lock = Lock()
        self.exchange = exchange
        self.candle_manager = self.CandleManager(exchange)
        self.pairs = None

    class CandleManager:
        def __init__(self, exchange: str):
            self.exchange = exchange
            self.current_candles = defaultdict(dict)
            self.lock = Lock()
            self.last_flush = time.time()
            self.r = redis_client

        def _fetch_ohlc_from_db(self, symbol: str, start_time: str, end_time: str = None, retries: int = 2,
                                delay: float = 1) -> list:
            table_name = f"{self.exchange}_stocks"
            window = getattr(self, 'window', 2) + 2
            try:
                start_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
                end_dt = (datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
                          if end_time else None)
            except ValueError:
                logger.error(f"Invalid time format for {symbol}")
                return []
            TRADING_SYSTEM_CONN_PARAMS = {
                "dbname": "trading_system",
                "user": "postgres",
                "password": "onealpha12345",
                "host": "localhost",
                "port": 5432
            }
            for attempt in range(retries):
                try:
                    with psycopg2.connect(**TRADING_SYSTEM_CONN_PARAMS) as conn:
                        with conn.cursor() as cur:
                            query = sql.SQL("""
                                SELECT symbol, timestamp, open, high, low, close, volume
                                FROM public.{table}
                                WHERE symbol = %s AND timestamp >= %s
                                {end_condition}
                                ORDER BY timestamp DESC
                                LIMIT %s
                            """).format(
                                table=sql.Identifier(table_name),
                                end_condition=sql.SQL("AND timestamp <= %s") if end_dt else sql.SQL("")
                            )

                            params = [symbol.lower(), start_dt]
                            if end_dt:
                                params.append(end_dt)
                            params.append(window)

                            cur.execute(query, params)
                            rows = cur.fetchall()

                            parsed_data = []
                            for row in rows:
                                parsed_data.append({
                                    "symbol": row[0],
                                    "timestamp": int(row[1].astimezone(IST).timestamp()),
                                    "open": float(row[2]),
                                    "high": float(row[3]),
                                    "low": float(row[4]),
                                    "close": float(row[5]),
                                    "volume": float(row[6]),
                                })

                            return parsed_data

                except Exception as e:
                    logger.error(f"Error fetching data for {symbol} from {table_name}: {e}")

                if attempt < retries - 1:
                    time.sleep(delay)

            return []

        def _get_current_minute_key(self, event_time: float):
            """Get precise minute key from event timestamp"""
            return datetime.fromtimestamp(event_time / 1000, IST).strftime("%Y-%m-%d %H:%M")

        # def _push_to_websocket(self, candle_data: dict):
        #     try:
        #         app.push_candle_data(candle_data)
        #         print(f"Pushed data to WebSocket {candle_data}")
        #     except Exception as e:
        #         logger.error(f"Error pushing to WebSocket: {e}", exc_info=True)

        def update_candle(self, pair: str, price: float, event_time: float,
                          update_callback, monitor_callback):
            """Real-time candle updates with proper OHLC tracking and WebSocket push."""
            try:
                if not all([price, event_time]):
                    logger.warning(f"Invalid input: price={price}, event_time={event_time}")
                    return
                current_minute = self._get_current_minute_key(event_time)

                with self.lock:
                    candle_closed = False
                    prev_candle_data = None

                    # Check if a new candle should start (minute has changed)
                    if pair in self.current_candles and self.current_candles[pair].get('minute') != current_minute:
                        prev_candle_data = self.current_candles[pair].copy()
                        candle_closed = True

                    # Update or initialize the current candle
                    if pair not in self.current_candles or self.current_candles[pair].get('minute') != current_minute:
                        self.current_candles[pair] = {
                            'minute': current_minute,
                            'open': price,
                            'high': price,
                            'low': price,
                            'close': price,
                            'volume': 1,
                            'last_updated': time.time()
                        }
                    else:
                        candle = self.current_candles[pair]
                        candle['high'] = max(candle['high'], price)
                        candle['low'] = min(candle['low'], price)
                        candle['close'] = price
                        candle['volume'] += 1
                        candle['last_updated'] = time.time()

                    # Prepare candle data for WebSocket push
                    candle_data = self.current_candles[pair]
                    websocket_data = {
                        'timestamp': candle_data['minute'],
                        'open': candle_data['open'],
                        'high': candle_data['high'],
                        'low': candle_data['low'],
                        'close': candle_data['close'],
                        'volume': candle_data['volume']
                    }


                    # Callback for live candle updates
                    update_callback(
                        pair,
                        candle_data['minute'],
                        candle_data['open'],
                        candle_data['high'],
                        candle_data['low'],
                        candle_data['close'],
                        candle_data['volume'],
                        None
                    )
                    monitor_callback(pair.lower(), candle_data['close'])

                    if candle_closed and prev_candle_data:
                        try:
                            update_callback(
                                pair,
                                prev_candle_data['minute'],
                                prev_candle_data['open'],
                                prev_candle_data['high'],
                                prev_candle_data['low'],
                                prev_candle_data['close'],
                                prev_candle_data['volume'],
                                None
                            )

                            closed_candle_data = {
                                'timestamp': prev_candle_data['minute'],
                                'open': prev_candle_data['open'],
                                'high': prev_candle_data['high'],
                                'low': prev_candle_data['low'],
                                'close': prev_candle_data['close'],
                                'volume': prev_candle_data['volume']
                            }

                        except Exception as e:
                            logger.error(f"Closed candle callback failed for {pair}: {e}", exc_info=True)

                    # Periodic cleanup
                    if time.time() - self.last_flush > 5:
                        self._flush_old_candles(current_minute)
                        self.last_flush = time.time()

            except Exception as e:
                logger.error(f"Candle update error for {pair}: {e}", exc_info=True)

        def _flush_old_candles(self, current_minute: str):
            to_remove = []
            for pair, candle in self.current_candles.items():
                if candle.get('minute') != current_minute:
                    to_remove.append(pair)
            for pair in to_remove:
                del self.current_candles[pair]

    def _parse_data(self, data: dict) -> Optional[dict]:
        if self.calculator.exchange == "binance":
            return self._parse_binance_data(data)
        else:
            return self._parse_fyers_data(data)

    def _parse_fyers_data(self, data: dict) -> Optional[dict]:
        """Improved FYERS data parser"""
        try:
            if isinstance(data, str):
                data = json.loads(data)
            parsed = {
                "symbol": data.get('symbol'),
                "price": float(data.get('ltp', 0)),
                "event_time": data.get('timestamp', int(time.time() * 1000))
            }
            # Ensure that symbol exists before storing in Redis
            raw_symbol = parsed.get("symbol")  # e.g., "NSE:SBIN-EQ"
            if raw_symbol:
                symbol = raw_symbol.split(":")[1].split("-")[0]
                if symbol and redis_client:
                    # Key format: symbol_ltp:{symbol}
                    redis_key = f"nse_symbol_ltp:{symbol}"
                    # Store the data in Redis as a JSON string (optional: you may also store price directly)
                    redis_client.set(redis_key, json.dumps(parsed))

                return parsed
        except (KeyError, TypeError, ValueError) as e:
            logger.error(f"Invalid FYERS data format: {e}")
            return None

    def _parse_binance_data(self, data: dict) -> Optional[dict]:
        """New Binance data parser"""
        try:
            if isinstance(data, str):
                data = json.loads(data)
            inner = data.get('data', {})
            parsed = {
                "symbol": inner.get('s'),
                "price": float(inner.get('c', 0)),
                "event_time": inner.get('E', int(time.time() * 1000))
            }
            # Ensure that symbol exists before storing in Redis
            symbol = parsed.get("symbol")
            if symbol and redis_client:
                # Key format: symbol_ltp:{symbol}
                redis_key = f"binance_symbol_ltp:{symbol}"

                # Store the data in Redis as a JSON string (optional: you may also store price directly)
                redis_client.set(redis_key, json.dumps(parsed))

            return parsed

        except (KeyError, TypeError, ValueError) as e:
            logger.error(f"Invalid Binance data format: {e}")
            return None

    def process_message(self, data: dict, update_callback, monitor_callback):
        """Enhanced message processing with validation"""
        try:
            clean_data = self._parse_data(data)
            if not clean_data or 'symbol' not in clean_data or 'price' not in clean_data:
                return
            symbol = clean_data['symbol']
            price = clean_data['price']
            event_time = clean_data.get('event_time', int(time.time() * 1000))
            with self.data_lock:
                self.real_time_data[symbol] = (price, event_time)
                # print(symbol, self.real_time_data)
                if self.pairs is None:
                    self.pairs = self._get_relevant_pairs()
                # print("data", data)
                # print(f"pairs {self.pairs} are passed to calculate_spread function.")
                for sym1, sym2 in self.pairs:
                    # print("sym1", sym1, "sym2", sym2)
                    self._process_pair(sym1, sym2, update_callback, monitor_callback)
        except Exception as e:
            logger.error(f"Message processing error: {e}", exc_info=True)

    def _process_pair(self, sym1: str, sym2: str, update_callback, monitor_callback):
        # print("sym1", sym1, "sym2", sym2)
        try:
            price1, time1 = self.real_time_data.get(sym1.upper(), (None, None))
            price2, time2 = self.real_time_data.get(sym2.upper(), (None, None))

            if None in (price1, price2):
                logger.debug(f"Skipping pair {sym1}_{sym2} - missing prices")
                return
            # print(f"sym1 {sym1}, price1 {price1}, sym2 {sym2}, price2 {price2} are passed to calculate_spread function. ")
            spread = self.calculator.calculate_spread(sym1, sym2, price1, price2)
            print("spread", spread)

            if spread is None:
                logger.debug(f"Invalid spread for {sym1}_{sym2} ({price1}, {price2})")
                return

            pair_name = self.calculator.generate_pair_name(sym1, sym2)
            latest_time = max(time1, time2)

            self.candle_manager.update_candle(
                pair_name,
                spread,
                latest_time,
                update_callback,
                monitor_callback
            )
        except Exception as e:
            logger.error(f"Pair processing error for {sym1}_{sym2}: {e}", exc_info=True)

    def _get_relevant_pairs(self) -> list:
        # pairs = []
        if self.exchange == "binance":
            redis_key = "manual_symbols:user_pairs_binance"
        else:
            redis_key = "manual_symbols:user_pairs_nse"

        raw = redis_client.get(redis_key)
        manual_pairs = json.loads(raw)

        return manual_pairs
    
