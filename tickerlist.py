import pandas as pd

url = "https://www.cboe.com/us/equities/market_statistics/listed_symbols/csv"

tlist = pd.read_csv(url)
print(tlist)
