# scripts/price_data.py

import pandas as pd
import yfinance as yf
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Output paths
RAW_DATA_DIR = Path('..data/raw/')
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Extract Data
def extract_price_data(ticker: str, period: str = '2y') -> pd.DataFrame:
    '''
    Extracts 2 year price data from Yahoo Finance for given ticekr.
    :param ticker: selects stocks/company name like MSFT means Microsoft, AAPL means Apple stocks, and so on
    :param period: length of time of data
    :return: dataframe
    '''
    # logging info before data extraction
    logging.info(f'Extracting data for ticker: {ticker}')
    stock = yf.Ticker(ticker)
    data = stock.history(period=period)

    # logging info after data extraction
    logging.info(f'Data Shape: {data.shape}')

    return data

# Save the Data
def save_data(df: pd.DataFrame, filename: str):
    '''
    Saves the dataframe to CSV format.
    :param df: pandas dataframe with rows and columns
    :param filename: text file containing raw data
    :return: None
    '''
    filepath = RAW_DATA_DIR / filename
    df.to_csv(filepath)
    logging.info(f'Saving data to {filepath}...')

if __name__ == '__main__':
    ticker = 'AAPL'
    price_data = extract_price_data(ticker)
    save_data(price_data, f'price_{ticker}.csv')