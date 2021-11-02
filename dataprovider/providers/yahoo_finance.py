from . import DataProvider
from typing import Optional, List
from datetime import date
from dateutil.relativedelta import relativedelta
import yfinance as yf

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