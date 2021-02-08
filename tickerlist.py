import pandas as pd

others_list = 'ftp://ftp.nasdaqtrader.com/symboldirectory/otherlisted.txt'
nasdaq_list = 'ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt'

def symbols_nyse():
    other = pd.read_csv(others_list, sep='|')
    #Get NYSE stuff
    company_nyse = other[other['Exchange']=='N'][['ACT Symbol', 'Security Name']]
    #ETFs include MYSE MKT, NYSE ARCA, and MATS.
    etf_other = other[other['ETF'] == 'Y'][['ACT Symbol', 'Security Name', 'Exchange']]   
    #index reset
    company_nyse = company_nyse.reset_index(drop=True)
    etf_other = etf_other.reset_index(drop=True)
    #ACT Symbol -> Symbol
    company_nyse = company_nyse.rename(columns={'ACT Symbol':'Symbol'})
    etf_other = etf_other.rename(columns={'ACT Symbol':'Symbol'})
    
    return company_nyse, etf_other

print(symbols_nyse())

def symbols_nasdaq():
    nasdaq = pd.read_csv(nasdaq_list, sep='|')
    #Get only those with Normal Status
    nasdaq_normal = nasdaq[nasdaq['Financial Status']=='N']
    #Select one that is not a Test issue
    nasdaq_normal = nasdaq_normal[nasdaq_normal['Test Issue']=='N']
    #Determined by ETF
    company_nasdaq = nasdaq_normal[nasdaq_normal['ETF']=='N'][['Symbol', 'Security Name']]
    etf_nasdaq = nasdaq_normal[nasdaq_normal['ETF']=='Y'][['Symbol', 'Security Name']]
    #index reset
    company_nasdaq = company_nasdaq.reset_index(drop=True)
    etf_nasdaq = etf_nasdaq.reset_index(drop=True)

    return company_nasdaq, etf_nasdaq

print(symbols_nasdaq())

def symbols_all():
    company_nyse, etf_other = symbols_nyse()
    company_nasdaq, etf_nasdaq = symbols_nasdaq()
    #Distinguish between NYSE and NASDAQ
    company_nyse['Market'] = 'NYSE'
    company_nasdaq['Market'] = 'NASDAQ'    
    #Also distinguish NASDAQ ETFs
    etf_nasdaq['Exchange'] = 'NASDAQ'#etf_Match to other colum name

    return (pd.concat([company_nyse, company_nasdaq], ignore_index=True, sort=False),
            pd.concat([etf_other, etf_nasdaq], ignore_index=True, sort=False))
