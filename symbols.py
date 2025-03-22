import alpaca_trade_api as tradeapi
import json

def get_crypto_symbols():
    # Load API credentials from config.json
    with open("config.json", "r") as f:
        config = json.load(f)
    
    api_key = config["API_KEY"]
    api_secret = config["API_SECRET"]
    base_url = config["ALPACA_URL"]
    
    # Initialize Alpaca API
    api = tradeapi.REST(api_key, api_secret, base_url, api_version="v2")
    
    # Fetch all assets and filter for crypto
    assets = api.list_assets(asset_class="crypto")
    
    print("\nSupported Crypto Symbols:\n")
    for asset in assets:
        print(f"\"{asset.symbol}\",")

if __name__ == "__main__":
    get_crypto_symbols()