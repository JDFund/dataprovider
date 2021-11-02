import pandas as pd
from typing import Optional, List
from datetime import date
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
import yfinance as yf
import requests
from ratelimit import limits
import time

class DataProvider:
    def __init__(self):
        pass 

    @staticmethod
    def get_SP500_list(url: str, symbol_col: str, gics_sector: str, save_results = True):
        """
        Download SP500 tickers and wikipedia table
        Parameters
        ----------
        url: url to download S&P 500 tickers
        sym_col: symbol column (ticker)

        Returns
        df: dataframe with SP500 stocks from Wikipedia
        stocks: dictionaty
        -------
        """
        df = pd.read_html(url)[0]
        df.columns = [col.lower().replace(' ', '_') for col in df.columns]
        stocks = df[[symbol_col, gics_sector]].set_index(
            symbol_col).to_dict()[gics_sector]
        if save_results:
            df.to_csv('./data/df_sp500.csv', index=False)
        return stocks

    @staticmethod
    def plot_sectors(data: pd.DataFrame,
                     sector_column: str,
                     fig_size=(17, 11),
                     rotation=20,
                     save_folder="./data/plots/"):
        f, ax = plt.subplots(figsize=fig_size)
        data[sector_column].value_counts().plot.bar()
        plt.title(f'Bar plot S&P500 {sector_column}')
        plt.xticks(rotation=rotation)
        print(f"Printing barplot of {sector_column}...")
        plt.savefig(f'{save_folder}{sector_column}_distribution.png')
        plt.show()

    @staticmethod
    def save_data(value: str, df: List[pd.DataFrame]):
        """
        Save data
        """
        file_name = "./data/{value}.csv".format(value = value)
        df.to_csv(file_name, index = True)
        
    def historic_prices(self):
        pass

    def price_data(self):
        pass 

class YahooFinance(DataProvider):
        
    def __init__(self):
        super().__init__()
        self.name = "yahoofinance"

    def historic_prices(self, 
                        intraday_intervals: Optional[list] = ['5m', '30m', '1h'],
                        no_intraday_intervals: Optional[list] = ['1d', '1wk', '3mo'],
                        start=None,
                        sp_web: str = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
                        end=date.today().strftime('%Y-%m-%d'),
                        intraday_timeperiod: int = 59,
                        no_intraday_timeperiod: int = 10):
        """
        https://github.com/ranaroussi/yfinance
        fetch data by intervals (including intraday if period < 60 days)
        valid intervalss: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
        (optional, default is '1d')
        """
        stocks = DataProvider.get_SP500_list(sp_web, 'symbol', 'gics_sector', save_results = True)
        time_periods = {'intraday': intraday_timeperiod, 'no_intraday': no_intraday_timeperiod}

        for time_period in time_periods.keys():
            if time_period == 'intraday':
                interval = intraday_intervals
                start = (
                    date.today() - relativedelta(days=time_periods[time_period])).strftime('%Y-%m-%d')
            else:
                interval = no_intraday_intervals
                start = (date.today(
                ) - relativedelta(years=time_periods[time_period])).strftime('%Y-%m-%d')

            for value in interval:
                print("Downloading data using interval of: ", value)
                dataframe = self.price_data(tickers=list(stocks.keys()),
                                                                        interval=value,
                                                                        start=start,
                                                                        end=end)
                # Save data
                DataProvider.save_data(value, dataframe)
    
    def price_data(self, tickers, interval, start, end):
        df_hist = yf.download(tickers=tickers, interval=interval,
                              start=start, end=end)
        df_hist = df_hist["Adj Close"]
        #df_hist = df_hist[["Open", "High", "Low", "Adj Close", "Volume"]]
        #df_hist.columns = ["open", "high", "low", "close", "volume"]
        return df_hist

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