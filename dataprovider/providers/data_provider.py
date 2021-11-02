import pandas as pd
from typing import Optional, List
import matplotlib.pyplot as plt

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

