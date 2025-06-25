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

    merged_df = pd.concat([df1, df2], ignore_index=True).drop_duplicates()
    logging.info(f"Merged shape (after deduplication): {merged_df.shape}")

    # Clean + parse dates
    if 'publishedAt' in merged_df.columns:
        date_col = 'publishedAt'
    else:
        date_col = 'Date'

    # Clean string formatting issues
    merged_df[date_col] = (
        merged_df[date_col]
        .astype(str)
        .str.strip()
        .str.replace('"', '')
    )

    # Parse datetime with fallback
    merged_df[date_col] = pd.to_datetime(
        merged_df[date_col],
        format='ISO8601',
        utc=True,
        errors='coerce'
    )

    null_count = merged_df[date_col].isna().sum()
    if null_count > 0:
        logging.warning(f"{null_count} rows dropped due to invalid dates.")
        merged_df = merged_df.dropna(subset=[date_col])

    # Sort by date
    merged_df = merged_df.sort_values(by=date_col).reset_index(drop=True)

    # Save
    merged_df.to_csv(output_file / file, index=False)
    logging.info(f"Merged file saved to: {output_file / file}")
    logging.info(
        f"Final row count: {merged_df.shape[0]} | Date range: {merged_df[date_col].min()} → {merged_df[date_col].max()}")

if __name__ == "__main__":
    append_data('news_Apple_AAPL.csv', news_api=True)
    append_data('price_AAPL.csv')
