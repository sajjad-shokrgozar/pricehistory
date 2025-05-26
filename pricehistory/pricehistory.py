import requests
import csv
import os
import re
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

from market import Market
from helpers import Helpers

class PriceHistory:
    # Precompile the regex for filtering
    bad_symbol_pattern = re.compile(r'[0-9]|ح$')
    
    @classmethod
    def get(cls, is_option, symbols=None, max_workers=8):
        """
        Main entry point for fetching and adjusting price data.
        Usage:
            result_list = PriceHistory.get(symbols=['فملی'], max_workers=8)

        If `symbols` is None or an empty list, all symbols from firms_info.csv are fetched.
        Returns:
            A list of dictionaries (JSON-serializable) containing the adjusted price data.
        """
        fetch_all = (not symbols)

        # Create a session for all requests
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0'
        })

        # 1) Read (symbol, id) info from CSV
        firms_info = Market.get_firms_info(symbols, fetch_all)

        # 2) Fetch data for each firm in parallel
        all_data = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(cls._fetch_symbol_data, fi, session, is_option) for fi in firms_info]
            for future in futures:
                all_data.extend(future.result())

        # 3) Adjust all prices
        adjusted_data = cls._adjust_price(all_data)

        return adjusted_data

    @classmethod
    def _fetch_symbol_data(cls, symbol_id_tuple, session, is_option):
        """
        Fetches JSON data for a single (symbol, id) pair.
        Returns a list of dicts for each record in 'closingPriceDaily'.
        """
        symbol, firm_id = symbol_id_tuple
        
        # Skip symbols with digits or ending with 'ح' (not for options)
        if not is_option:
            if cls.bad_symbol_pattern.search(symbol):
                return []
        
        url = f'https://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceDailyList/{firm_id}/0'
        
        try:
            resp = session.get(url, timeout=10)
            resp.raise_for_status()
            records = resp.json().get('closingPriceDaily', [])
        except Exception:
            # On any network/JSON error, return empty
            return []
        
        # Safely convert numeric fields (if they're not None)
        def to_numeric(value):
            try:
                return float(value)
            except (TypeError, ValueError):
                return None
        
        # Convert raw response to a list of dicts
        results = []
        for rec in records:
            results.append({
                'symbol':       symbol,
                'id':           rec.get('insCode'),
                'date':         rec.get('dEven'),
                'jdate':        Helpers.to_jalali(rec.get('dEven')),
                'min':          to_numeric(rec.get('priceMin')),
                'max':          to_numeric(rec.get('priceMax')),
                'yesterday':    to_numeric(rec.get('priceYesterday')),
                'first':        to_numeric(rec.get('priceFirst')),
                'close':        to_numeric(rec.get('pClosing')),
                'last':         to_numeric(rec.get('pDrCotVal')),
                'trades_count': to_numeric(rec.get('zTotTran')),
                'volume':       to_numeric(rec.get('qTotTran5J')),
                'value':        to_numeric(rec.get('qTotCap')),
            })
        return results

    @classmethod
    def _adjust_price(cls, data_list):
        """
        Takes the raw list of dicts and applies price adjustment logic in pure Python.
        Returns a new list of dicts with 'adj_price', 'ret', and 'cumprod' added.
        """
        if not data_list:
            return []

        # Group all records by 'id'
        grouped = {}
        for record in data_list:
            stock_id = record['id']
            if stock_id not in grouped:
                grouped[stock_id] = []
            grouped[stock_id].append(record)

        # Adjust each group and collect the final results
        final_results = []
        for stock_id, records in grouped.items():
            adjusted_records = cls._adj_price_calculator(records)
            final_results.extend(adjusted_records)

        # Sort the entire final list by (id, date) ascending if desired
        # (Assumes date is comparable; if it's a string like '14020101', it should compare as intended)
        final_results.sort(key=lambda x: (x['id'], x['date']))

        return final_results

    @staticmethod
    def _adj_price_calculator(records):
        """
        Sort descending by 'date', compute:
          ret      = close / yesterday
          cumprod  = cumulative product of ret (descending order)
          adj_price

        Then shift adj_price so top row uses the 'latest_close_price'.
        Finally, re-sort ascending by 'date' before returning.
        """
        # Sort descending by date
        # If date is a string of digits like '14020101', it still sorts correctly in descending order as string
        # but if you'd rather be sure, convert to int in the sort key.
        records_desc = sorted(records, key=lambda x: x['date'], reverse=True)

        # If the top record is invalid or close is None, skip
        # but let's assume it's valid for now
        latest_close_price = records_desc[0]['close'] if records_desc[0]['close'] else 0.0

        # Compute ret and cumprod (descending)
        cumprod_value = 1.0
        for idx, rec in enumerate(records_desc):
            close_price = rec['close'] if rec['close'] else 0.0
            yesterday_price = rec['yesterday'] if rec['yesterday'] else 1e-9  # avoid div by zero
            rec['ret'] = close_price / yesterday_price
            cumprod_value *= rec['ret']
            rec['cumprod'] = cumprod_value

        # Compute adj_price
        #   adj_price = latest_close_price / cumprod
        # Then shift them by 1 so that records_desc[0]['adj_price'] = latest_close_price
        # and others are "pushed down" by one.
        adj_prices = []
        for rec in records_desc:
            if rec['cumprod'] == 0:
                adj_prices.append(0.0)
            else:
                adj_prices.append(latest_close_price / rec['cumprod'])

        # Shift
        # first row = latest_close_price, subsequent rows = previous
        for i in range(len(adj_prices) - 1, 0, -1):
            adj_prices[i] = adj_prices[i-1]
        adj_prices[0] = latest_close_price

        # Assign back
        for i, rec in enumerate(records_desc):
            rec['adj_price'] = adj_prices[i]

        # Finally, sort ascending by date (if desired for final output)
        records_asc = sorted(records_desc, key=lambda x: x['date'])

        return records_asc
    
    @classmethod
    def get_history_by_symbol_id_list(cls, symbol_id_list: list=None, max_workers=8):
        """
        'symbol_id_list' format: [[symbol1, firm_id1], [symbol2, firm_id2]]
        for example:
            [['فولاد', '46348559193224090'], ['فملی', '35425587644337450'], ['شاراک', '7711282667602555']]

        """
        # Create a session for all requests
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0'
        })

        # 2) Fetch data for each firm in parallel
        all_data = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(cls._fetch_symbol_data, fi, session, True) for fi in symbol_id_list]
            for future in futures:
                all_data.extend(future.result())

        return all_data


    @staticmethod
    def get_index_history():
        index_url = 'http://old.tsetmc.com/tsev2/chart/data/Index.aspx?i=32097828799138957&t=value'
        res = requests.get(index_url)
        rows = res.text.split(';')
        index_data = []
        for row in rows:
            index_data.append(row.split(','))
        index_df = pd.DataFrame(index_data, columns=['date', 'index'])
        index_df['date'] = index_df['date'].replace(r'/', '', regex=True)
        return index_df

# result_json = PriceHistory.get(symbols=['فملی', 'فولاد'], max_workers=8)
# print(result_json)


# print(PriceHistory.get_history_by_symbol_id_list([['فولاد', '46348559193224090'], ['فملی', '35425587644337450'], ['شاراک', '7711282667602555']]))