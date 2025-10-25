from datetime import datetime
import redis
import time
import json
from services.loger import logger
from services.ols.ols_maker import process_exchange_symbols
from concurrent.futures import ThreadPoolExecutor
from services.ols.fyersdata import fyers_Symbol_gap_filler


redis_client = redis.Redis(host='localhost', port=6379, db=0)
FYERS_SYMBOLS = ["NSE:ABB-EQ","NSE:ACC-EQ","NSE:APLAPOLLO-EQ","NSE:AUBANK-EQ","NSE:AARTIIND-EQ"]

def Symbol_gap_filler(EXCHANGE):
    if EXCHANGE == 'nse':
        for full_symbol in FYERS_SYMBOLS:
            symbol = full_symbol.split(":")[1].split("-")[0]
            fyers_Symbol_gap_filler(symbol)

def save_spreads_list(EXCHANGE):
    if EXCHANGE != "nse": return
    k1, k2 = "spreads:nse_spreads_name", "manual_symbols:user_pairs_nse"
    p = {f"{a.lower()}_{b.lower()}" for a, b in json.loads(redis_client.get(k2) or "[]")}
    e = {x.decode() for x in redis_client.smembers(k1) or set()}
    if (n := p - e): redis_client.sadd(k1, *n); redis_client.persist(k1)
    return [x.decode() for x in redis_client.smembers(k1)]


def Spreads_gap_filler(EXCHANGE):
    print("Spreads gap filler start", datetime.now())
    save_spreads_list(EXCHANGE)
    try:
        if EXCHANGE == 'nse':
            redis_key  = 'spreads:nse_spreads_name'
            process_fn = process_exchange_symbols

        raw_pairs = redis_client.smembers(redis_key)
        symbol_pairs = [pair.decode().split('_') for pair in raw_pairs]

        def worker(pair):
            sym1, sym2 = pair
            process_fn(EXCHANGE, sym1.upper(), sym2.upper())

        with ThreadPoolExecutor(max_workers=10) as pool:
            pool.map(worker, symbol_pairs)

    except Exception as e:
        logger.error(f"Spreads gap filler error: {e}")
        raise

if __name__ == "__main__":
    exchange_mode = 'nse'

    # Symbol_gap_filler(exchange_mode)
    print('Symbol_gap_filler completed')

    Spreads_gap_filler(exchange_mode)
    try:
        while True:
            if not redis_client.ping():
                raise ConnectionError("Redis connection lost")
            time.sleep(5)
    except KeyboardInterrupt:
        print("Shutdown signal received. Cleaning up...")