from . import DataProvider
from typing import Optional, List
import requests
import time 
import pandas as pd

class AlphaVantage(DataProvider):

    # https://github.com/RomelTorres/alpha_vantage
    
    def __init__(self, apiKey):
        super().__init__()
        self.apiKey = apiKey
        self.endpoints = self.get_endpoints()
        self.name = "alphavantage"
        self.num_calls = 0

    def get_endpoints(self):
        website = "https://www.alphavantage.co/query?"
        endpoints = {}

        # Intraday ajusted intervals.
        intraday_api_intervals = ["1m", "5m", "15m", "30m", "60m"]
        for interval in intraday_api_intervals:
            endpoints[interval + "in"] = website \
                                  + "function=TIME_SERIES_INTRADAY" \
                                  + "&symbol=ticker" \
                                  + "&interval={interval}".format(interval = interval)  \
                                  + "&apiKey={apiKey}".format(apiKey = self.apiKey) 
        # Daily ajusted interval.
        endpoints["1d"] = '{website}/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=ticker&apikey={apiKey}'.replace(website = website, apiKey = self.apiKey)

        # Weekly ajusted interval.
        endpoints["1wk"] = '{website}/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol=ticker&apikey={apiKey}'.replace(website = website, apiKey = self.apiKey)

        # Monthly ajusted interval.
        endpoints["1mo"] = '{website}/query?function=TIME_SERIES_MONTHLY_ADJUSTED&symbol=ticker&apikey={apiKey}'.replace(website = website, apiKey = self.apiKey)

        return endpoints

    def historic_prices(self, 
                        intraday_intervals: Optional[list] = ['5min', '30min', '60min'],
                        no_intraday_intervals: Optional[list] = ['1d', '1wk', '3mo'],
                        sp_web: str = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'):

        stocks = DataProvider.get_SP500_list(sp_web, 'symbol', 'gics_sector', save_results = True)

        for time_period in ["intraday", "no_intraday"]:
            if time_period == 'intraday':
                interval = intraday_intervals
            else:
                interval = no_intraday_intervals
            for value in interval:
                print("Downloading data using interval of: ", value)
                dataframes = self.price_data(tickers=list(stocks.keys()),
                                                          interval=value)
                # Save data
                DataProvider.save_data(value, dataframes)

    def price_data(self, tickers, interval):
        dataframes = list()
        for ticker in tickers: 
            url = self.endpoints[interval].replace("ticker", ticker)
            r = requests.get(url)
            data = r.json()
            df = pd.DataFrame(data[data.keys()[1]])
            df = df["close"]
            df.columns = [ticker]
            #df.columns = ["open", "high", "low", "close", "volume"]
            dataframes.append(df)
            if self.num_calls % 4 == 0:
                print("Waiting a minute before making a new call...")
                time.sleep(61)
        return pd.concat(dataframes)