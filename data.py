import requests
import json
import pandas as pd
from datetime import datetime, timedelta, timezone

def get_historical_data(symbol, timeframe="1Day", limit=1000):
    # Load API credentials from config.json
    with open("config.json", "r") as f:
        config = json.load(f)
    
    api_key = config["API_KEY"]
    api_secret = config["API_SECRET"]
    base_url = config["BASE_URL"]
    
    # Define the start and end dates for the historical data
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=7)  # Fetch data for the last 7 days
    
    # Convert dates to RFC-3339 format with a 'Z' suffix for UTC
    start_date = start_date.isoformat(timespec="seconds").replace("+00:00", "Z")
    end_date = end_date.isoformat(timespec="seconds").replace("+00:00", "Z")
    
    # Construct the request URL
    url = f"{base_url}?symbols={symbol}&timeframe={timeframe}&start={start_date}&end={end_date}&limit={limit}&sort=asc"
    
    # Set headers for authentication
    headers = {
        "accept": "application/json",
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": api_secret
    }
    
    try:
        # Make the request
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an error for bad status codes
        
        # Parse the response JSON
        data = response.json()
        
        # Convert the data to a DataFrame
        bars = pd.DataFrame(data["bars"])
        print(f"\nHistorical Data for {symbol}:\n")
        print(bars)
        return bars
    except Exception as e:
        print(f"Error fetching historical data for {symbol}: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    # Example usage
    symbol = "BTC/USD"  # Replace with the desired symbol
    timeframe = "1Day"  # Replace with the desired timeframe
    limit = 1000  # Number of data points to fetch
    get_historical_data(symbol, timeframe, limit)