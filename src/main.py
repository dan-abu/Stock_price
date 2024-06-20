"""Downloads NVDA previous day Ticker data from YFinance & Alpha vanta
and stores in S3"""
import yfinance as yf
import requests
import pandas as pd
import sys
from datetime import datetime as dt, timedelta

def yf_yday_tickerHist(ticker: str) -> pd.DataFrame:
    """Extract latest daily record of COP ticker position from YFinance.
    NB you might have to wait until the evening (UK time)
    for yesterday's COP data to refresh"""
    try:
        ticker = yf.Ticker(ticker)
    except Exception as e:
        print("Unable to extract ticker data.")
        print(e)
        raise e
    else:
        tickerHist = ticker.history(period="1d")
        tickerHist = tickerHist[["Open", "High", "Low", "Close", "Volume"]]
        tickerHist = tickerHist.reset_index()
        tickerHist["Date"] = tickerHist["Date"].dt.strftime("%Y-%m-%d")
        return tickerHist


def av_yday_tickerHist(ticker: str, api_key: str, yday_date: str) -> pd.DataFrame:
    """Extract yesterday's COP ticker position from Alpha Vantage.
    NB you might have to wait until the evening (UK time)
    for yesterday's COP data to refresh"""
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&apikey={api_key}"
    try:
        r = requests.get(url)
    except Exception as e:
        print("Unable to complete GET request.")
        print(e)
        raise e
    else:
        data = r.json()
        tickerHist = pd.DataFrame(data["Time Series (Daily)"])
        tickerHist = tickerHist.T
        tickerHist = tickerHist.reset_index()
        tickerHist = tickerHist.iloc[[0]]
        tickerHist.columns = ["Date", "Open", "High", "Low", "Close", "Volume"]
        return tickerHist


def merge_tickerData(table1: pd.DataFrame, tabl2: pd.DataFrame) -> None:
    """Merge data from two different ticker sources"""
    if table1["Date"].to_list() == tabl2["Date"].to_list():
        try:
            totalDF = pd.merge(
                left=table1, right=tabl2, how="left", on="Date", suffixes=["_yf", "_av"]
            )
        except Exception as e:
            print("Unable to merge DataFrames.")
            print(e)
            raise e
        else:
            return totalDF
    else:
        print("Table dates are not yet in sync. Re-run script in 1 hour.")


def put_s3_object(
    ticker: str, data: pd.DataFrame, s3_uri: str, key: str, exec_time: str
) -> bool:
    """Write data to desired S3 bucket as a CSV"""
    object_name = f"{ticker}_stock_data{exec_time}.csv"
    key = key + object_name
    dest_s3_uri = f"{s3_uri}{key}"

    print(f"Attempting to write data to S3 destination {dest_s3_uri}.")

    try:
        data.to_csv(dest_s3_uri, index=False)
    except Exception as e:
        print(f"Unable to write file to S3 location {dest_s3_uri}.")
        print(e)
        raise e
    else:
        print("Successfully uploaded file.")
        return True


if __name__ == "__main__":
    Ticker = sys.argv[1]
    Api_Key = sys.argv[2]
    S3_Uri = sys.argv[3]
    S3_Key = sys.argv[4]
    Yday_Date = (dt.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    Exec_Time = dt.now().strftime("%Y-%m-%d_%H:%M")
    yfTicker = yf_yday_tickerHist(ticker=Ticker)
    avTicker = av_yday_tickerHist(ticker=Ticker, api_key=Api_Key, yday_date=Yday_Date)
    mergedTickerData = merge_tickerData(table1=yfTicker, tabl2=avTicker)
    put_s3_object(
        ticker=Ticker,
        data=mergedTickerData,
        s3_uri=S3_Uri,
        key=S3_Key,
        exec_time=Exec_Time,
    )