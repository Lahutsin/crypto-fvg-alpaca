import requests
import pandas as pd
import json
import time
import threading
from datetime import datetime, timedelta, timezone
import alpaca_trade_api as tradeapi  # Импортируем Alpaca API

class FVGTrader:
    def __init__(self, config_path="config.json"):
        with open(config_path, "r") as f:
            config = json.load(f)
        
        self.api_key = config["API_KEY"]
        self.api_secret = config["API_SECRET"]
        self.base_url = config["BASE_URL"]
        self.symbols = config["SYMBOLS"]
        self.timeframe = config["TIMEFRAME"]
        self.max_drawdown = config["MAX_DRAWDOWN"] / 100
        self.break_even_trigger = config["BREAK_EVEN_TRIGGER"] / 100
        self.take_profit_ratio = config["TAKE_PROFIT_RATIO"] / 100
        self.limit = config.get("LIMIT", 1000)

        # Инициализируем Alpaca API
        self.alpaca_api = tradeapi.REST(self.api_key, self.api_secret, base_url=config["ALPACA_URL"])

    def load_trades(self, symbol):
        try:
            with open(f"trades_{symbol.replace('/', '_')}.json", "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return {}
    
    def save_trades(self, symbol, data):
        with open(f"trades_{symbol.replace('/', '_')}.json", "w") as f:
            json.dump(data, f, indent=4)
    
    def get_historical_data(self, symbol):
        try:
            # Define the start and end dates for the historical data
            end_date = datetime.now(timezone.utc).replace(microsecond=0)
            start_date = (end_date - timedelta(days=30)).replace(microsecond=0)

            # Convert dates to RFC-3339 format with a 'Z' suffix for UTC
            start_date = start_date.isoformat().replace("+00:00", "Z")
            end_date = end_date.isoformat().replace("+00:00", "Z")

            # Construct the request URL
            url = f"{self.base_url}?symbols={symbol}&timeframe={self.timeframe}&start={start_date}&end={end_date}&limit={self.limit}&sort=asc"

            # Set headers for authentication
            headers = {
                "accept": "application/json",
                "APCA-API-KEY-ID": self.api_key,
                "APCA-API-SECRET-KEY": self.api_secret
            }

            # Make the request
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raise an error for bad status codes

            # Parse the response JSON
            data = response.json()

            # Check if the symbol exists in the response
            if symbol not in data["bars"]:
                print(f"Error: Symbol {symbol} not found in the data response.")
                return None

            # Extract bars data for the symbol
            bars_data = data["bars"][symbol]

            # Convert the data to a DataFrame
            bars = pd.DataFrame(bars_data)

            # Now ensure the columns have the correct names
            bars = bars.rename(columns={
                'l': 'low',   # Rename 'l' to 'low'
                'h': 'high',  # Rename 'h' to 'high'
                'c': 'close', # Rename 'c' to 'close'
                'o': 'open',  # Rename 'o' to 'open'
                't': 'timestamp',  # Rename 't' to 'timestamp'
                'v': 'volume', # Rename 'v' to 'volume'
                'vw': 'volume_weighted_avg'  # Rename 'vw' to 'volume_weighted_avg'
            })
            return bars
        except Exception as e:
            print(f"Error fetching historical data for {symbol}: {e}")
            return None
    
    def detect_fvg(self, data):
        for i in range(2, len(data)):
            _, high1 = data.iloc[i-2]["low"], data.iloc[i-2]["high"]
            low3, _ = data.iloc[i]["low"], data.iloc[i]["high"]
            if high1 < low3:
                return (high1, low3)
        return None

    def place_trade(self, symbol, qty):
        trades = self.load_trades(symbol)
        data = self.get_historical_data(symbol)
        if data is None:
            return
        
        fvg_zone = self.detect_fvg(data)
        if fvg_zone:
            entry_price = fvg_zone[0]
            stop_loss = entry_price * (1 - self.max_drawdown)
            take_profit = entry_price * (1 + self.take_profit_ratio)
            
            try:
                # Place an order with Alpaca to buy the symbol at entry price
                print(f"Placing order for {symbol}: Entry={entry_price}, Stop-Loss={stop_loss}, Take-Profit={take_profit}, QTY={qty}")
                order = self.alpaca_api.submit_order(
                    symbol=symbol,
                    qty=qty,  # Adjust the quantity based on your needs
                    side="buy",  # You can change this to "sell" when closing positions
                    type="market",  # You can use "limit" for more control
                    time_in_force="gtc"  # Good-Til-Cancelled order
                )

                trades[symbol] = {
                    "entry_price": entry_price,
                    "stop_loss": stop_loss,
                    "take_profit": take_profit,
                    "qty": qty,
                    "order_id": order.id
                }
                self.save_trades(symbol, trades)
                print(f"Trade placed for {symbol}: Entry={entry_price}, Stop-Loss={stop_loss}, Take-Profit={take_profit}, QTY={qty}")
            except Exception as e:
                print(f"Error placing trade for {symbol}: {e}")
    
    def manage_trade(self, symbol):
        trades = self.load_trades(symbol)
        if symbol not in trades:
            return
        
        trade = trades[symbol]
        entry_price, stop_loss, take_profit, order_id = trade["entry_price"], trade["stop_loss"], trade["take_profit"], trade["order_id"]
        qty = trade["qty"]
        
        while True:
            try:
                if self.process_order_status(symbol, trades, trade, order_id, entry_price, stop_loss, take_profit, qty):
                    break
                time.sleep(600)
                print(f"Processing for {symbol}...")
            except Exception as e:
                print(f"Error managing trade for {symbol}: {e}")
                break

    def process_order_status(self, symbol, trades, trade, order_id, entry_price, stop_loss, take_profit, qty):
        order = self.alpaca_api.get_order(order_id)
        if order.status == "filled":
            current_price = float(self.get_last_coin_price(symbol))
            if self.handle_loss(symbol, trades, trade, current_price, qty):
                return True
            if self.handle_stop_loss(symbol, trades, trade, current_price, stop_loss, qty):
                return True
            if self.handle_take_profit(symbol, trades, trade, current_price, take_profit, qty):
                return True
            self.handle_break_even(symbol, trades, trade, current_price, entry_price)
        elif order.status in ["canceled", "rejected"]:
            self.handle_order_cancellation(symbol, trades, order_id)
            return True
        return False

    def handle_loss(self, symbol, trades, trade, current_price, qty):
        entry_price = trade["entry_price"]
        loss = (entry_price - current_price) * qty
        max_loss = entry_price * self.max_drawdown * qty
        if loss > max_loss:
            print(f"Loss exceeded for {symbol}: Current Loss={loss}, Max Loss={max_loss}. Closing position.")
            self.close_position(symbol, qty)
            trades.pop(symbol, None)
            self.save_trades(symbol, trades)
            print(f"Position for {symbol} closed due to excessive loss.")
            return True
        return False

    def handle_stop_loss(self, symbol, trades, trade, current_price, stop_loss, qty):
        if current_price <= stop_loss:
            self.close_position(symbol, qty)
            trades.pop(symbol, None)
            self.save_trades(symbol, trades)
            print(f"Stop-loss triggered for {symbol}, trade removed")
            return True
        return False

    def handle_take_profit(self, symbol, trades, trade, current_price, take_profit, qty):
        if current_price >= take_profit:
            self.close_position(symbol, qty)
            trades.pop(symbol, None)
            self.save_trades(symbol, trades)
            print(f"Take-profit triggered for {symbol}, trade removed")
            return True
        return False

    def handle_break_even(self, symbol, trades, trade, current_price, entry_price):
        if current_price >= entry_price * (1 + self.break_even_trigger):
            trade["stop_loss"] = entry_price
            self.save_trades(symbol, trades)
            print(f"{symbol} moved to break-even")

    def handle_order_cancellation(self, symbol, trades, order_id):
        print(f"Order {order_id} for {symbol} was canceled or rejected. Removing trade.")
        trades.pop(symbol, None)
        self.save_trades(symbol, trades)

    def close_position(self, symbol, qty):
        self.alpaca_api.submit_order(
            symbol=symbol,
            qty=qty,
            side="sell",
            type="market",
            time_in_force="gtc"
        )
    
    def trade_symbol(self, symbol, qty):
        self.place_trade(symbol, qty)
        self.manage_trade(symbol)
    
    def get_symbols_length(self):
        return len(self.symbols)

    def get_available_balance_to_trade(self):
        try:
            # Fetch account information from Alpaca
            account = self.alpaca_api.get_account()
            
            # Extract the cash balance (float) and convert it to an integer (ignoring decimals)
            cash_balance = float(account.cash)  # Convert to float first
            available_balance = int(cash_balance)  # Convert to integer to avoid decimals
            
            return available_balance
        except Exception as e:
            print(f"Error fetching balance: {e}")
            return None

    def get_last_coin_price(self, symbol):
        url = f"https://data.alpaca.markets/v1beta3/crypto/us/latest/quotes?symbols={symbol}"
        try:
            headers = {"accept": "application/json"}
            response = requests.get(url, headers=headers)
            data = response.json()

            # Extract ask price
            ap_price = data["quotes"][symbol]["ap"]
            return ap_price
        except Exception as e:
            print(f"Error fetching last price for {symbol}: {e}")
            return None

    def run(self):
        threads = []
        for symbol in self.symbols:
            last_coin_price = self.get_last_coin_price(symbol)
            trade_capital = (self.get_available_balance_to_trade() / self.get_symbols_length()) / 5
            qty =  trade_capital / last_coin_price 
            thread = threading.Thread(target=self.trade_symbol, args=(symbol, qty))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()

if __name__ == "__main__":
    trader = FVGTrader()
    trader.run()
