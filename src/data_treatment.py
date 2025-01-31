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
        self.tickers_names = self.get_tickers_names()
        self.raw_df = self.update_historical_price()
        self.raw_df = self.add_new_ticker_price()
        self.raw_df = self.raw_df.set_index('Date')
    
    def get_tickers_names(self):
        tickers = pd.read_excel('data/tickers_name.xlsx', index_col=0)
        tickers = tickers.to_dict()['Ticker']
        return tickers
    
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


class CompareWallet(Wallet):
    def __init__(self, wallet:Wallet, price:Price, ticker_name:str):
        self.ticker_code = self.get_ticker_code(ticker_name, price)
        self.dca = self.get_dca_wallet(wallet, price)
        self.dca_ts = self.get_dca_timeseries(wallet, price)
    
    def get_ticker_code(self, ticker_name:str, price:Price):
        assert ticker_name in price.tickers_names.keys()
        return price.tickers_names[ticker_name]

    def get_dca_wallet(self, wallet:Wallet, price:Price) -> pd.DataFrame:
        assert self.ticker_code in price.raw_df.columns
        dca = wallet.deposit.join(price.raw_df[self.ticker_code])#.rename(columns={ticker_code : ticker_code})

        buy_and_leftover = []
        for i, row in dca.iterrows():
            cash = row['Deposit']
            if buy_and_leftover:
                cash += buy_and_leftover[-1][1]
            buy_and_leftover.append((
                cash // row[self.ticker_code],
                cash % row[self.ticker_code]
            ))
        dca[['ticker_buy', 'leftover']] = buy_and_leftover
        dca['ticker_owned'] = dca['ticker_buy'].cumsum()
        return dca

    def get_dca_timeseries(self, wallet:Wallet, price:Price) -> pd.DataFrame:
        ts = self.dca.drop(columns=self.ticker_code)
        ts = ts.join(price.raw_df[self.ticker_code], how='right')
        ts[['Deposit', 'ticker_buy']] = ts[['Deposit', 'ticker_buy']].fillna(0)
        ts = ts.ffill()
        return ts