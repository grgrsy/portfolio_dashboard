from src.data_treatment import *
from src.streamlit_backend import *


wallet = Wallet("data/PEA-orders.xlsx")
price = Price('data/PEA-cours_historique_cache - Copie.xlsx')

compare = CompareWallet(wallet, price, "S&P 500")


print(compare.dca)
print(compare.dca_ts)

page_config() 
add_time_period()