# scripts/concat.py
import pandas as pd
import logging
from pathlib import Path


# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Directory Paths
NEWS_DIR = Path('../data/raw/news_files/')  # Relative path for flexibility
STOCK_DIR = Path('../data/raw/stock_files')
RAW_DATA_DIR = Path('../data/raw/')

def append_data(file: str, output_file: Path = RAW_DATA_DIR, news_api=False) -> None:
    '''
    Appends daily news and returns a concatenated Dataframe
    :param file: File containing news upto N days and also N + 1 days
    :param output_file: File containing news till date.
    :param news_api: Decides to pull news data or just the stocks
    :return: None
    '''
    try:
        if news_api:
            df1 = pd.read_csv(NEWS_DIR / file)  # Old Archive
            df2 = pd.read_csv(RAW_DATA_DIR / file)
        else:
            df1 = pd.read_csv(STOCK_DIR / file)
            df2 = pd.read_csv(RAW_DATA_DIR / file)  # New File
    except FileNotFoundError as e:
        logging.error(f"File not found: {e}")
        return

    # Combine & drop duplicates
    logging.info(f"Old file shape: {df1.shape}")
    logging.info(f"New file shape: {df2.shape}")

    merged_df = pd.concat([df1, df2]).drop_duplicates().reset_index(drop=True)
    logging.info(f"Merged shape (after deduplication): {merged_df.shape}")

    # Sort by date
    if 'publishedAt' in merged_df.columns:
        merged_df['publishedAt'] = pd.to_datetime(merged_df['publishedAt'], utc=True, errors='coerce')
        merged_df = merged_df.sort_values('publishedAt')
    else:
        merged_df['Date'] = pd.to_datetime(merged_df['Date'], errors='coerce')
        merged_df = merged_df.sort_values('Date')

    merged_df.to_csv(output_file / file, index=False)
    logging.info(f"Merged file saved to: {output_file}")


if __name__ == "__main__":
    append_data('news_Apple_AAPL.csv', news_api=True)
    append_data('price_AAPL.csv')
