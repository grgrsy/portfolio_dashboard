import pandas as pd
from datetime import datetime, timedelta, date
import yfinance as yf

class DataProcessor:
    def __init__(self, file_path:str):
        self.path = file_path
        self.raw_df = self.read_file()
    
    def read_file(self):
        try:
            df = pd.read_excel(self.path)
            df = df.sort_values('Date')
            return df
        except Exception as e:
            print(f"Error reading file: {e}")
            return None
    
    def write_file(self, data):
        try:
            data.to_excel(self.path, index=False)
        except Exception as e:
            print(f"Error writing file: {e}")


class Wallet(DataProcessor):
    def __init__(self, file_path:str):
        super().__init__(file_path)
        self.deposit = self.get_wallet_deposit()
        self.deposit_ts = self.to_timeseries(self.deposit)
        self.cashflow = self.get_wallet_cashflow()
        self.cashflow_ts = self.to_timeseries(self.cashflow)
        self.asset = self.get_wallet_asset()
        self.asset_ts = self.to_timeseries(self.asset)
        self.update_tickers_list()

    
    def get_wallet_deposit(self):
        deposit = self.raw_df.query("MovementType=='Deposit'")[['Date', 'Credit']].set_index('Date')
        deposit = deposit.rename(columns={'Credit' : 'Deposit'})
        return deposit
    
    def get_wallet_cashflow(self):
        cashflow = self.raw_df[['Date', 'Credit', 'Debit']].fillna(0)
        cashflow['cashflow'] = cashflow['Credit'] - cashflow['Debit']
        return cashflow[['Date', 'cashflow']]
    
    def get_wallet_asset(self):
        asset = self.raw_df[['Date', 'Ticker', 'Quantity']]
        asset = pd.pivot_table(asset, index='Date', columns=['Ticker'], values='Quantity')
        return asset
    
    def to_timeseries(self, df:pd.DataFrame):
        ts = df.groupby('Date').sum()
        start_date = ts.index.min()
        end_date = ts.index.max()
        ts = ts.cumsum().reindex(pd.date_range(start_date, end_date, freq='D')).ffill()
        return ts
    
    def update_tickers_list(self):
        ticker_name_path = 'data/tickers_name.xlsx'
        tickers = self.raw_df['Ticker'].dropna().unique()
        historical_tickers = pd.read_excel(ticker_name_path)
        for ticker in tickers:
            if ticker not in list(historical_tickers['Ticker']):
                yf_ticker = yf.Ticker(ticker)
                ticker_name = yf_ticker.info['shortName']
                df = pd.DataFrame(columns=historical_tickers.columns,
                                  data=[[ticker_name, ticker]])
                historical_tickers = pd.concat([historical_tickers, df])
        historical_tickers.to_excel(ticker_name_path, index=False)

class Price(DataProcessor):
    def __init__(self, file_path):
        super().__init__(file_path)
        self.today = date.today()
        self.yesterday = self.today - timedelta(days=1)
        self.raw_df = self.update_historical_price()
        self.raw_df = self.add_new_ticker_price()
    
    def update_historical_price(self):
        tickers = list(self.raw_df.columns[1:])
        last_date = self.raw_df['Date'].max().date()
        new_start_date = last_date + timedelta(days=1)
        if last_date < self.yesterday:
            print("=== Updating historical prices ===")
            print(f'from {last_date} to {self.yesterday}')
            historical_price = yf.download(
                tickers=tickers,
                start=new_start_date.strftime('%Y-%m-%d'),
                end=self.today,
                )
            historical_price = historical_price['Close'].reset_index()
            print(historical_price)
            self.raw_df = pd.concat([self.raw_df, historical_price])
            self.write_file(self.raw_df)
        return self.raw_df
    
    def add_new_ticker_price(self):
        tickers = pd.read_excel('data/tickers_name.xlsx')
        start_date = self.raw_df['Date'].min().date()
        new_tickers = [ticker for ticker in tickers['Ticker'] if ticker not in self.raw_df.columns]
        if new_tickers:
            print(new_tickers, "\n===== Will be updated =====")
            new_tickers_price = yf.download(
                tickers=new_tickers,
                start=start_date.strftime('%Y-%m-%d'),
                end=self.today,
            )
            new_tickers_price = new_tickers_price['Close']
            self.raw_df = pd.concat([self.raw_df, new_tickers_price], axis=1)
            self.write_file(self.raw_df)
        return self.raw_df