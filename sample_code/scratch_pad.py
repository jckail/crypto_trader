import pandas as p
cwd = '/Users/jkail/Documents/GitHub/lit_crypto/alpha/'
df = p.read_csv(cwd+'/data/coinlist_info.csv')
ls_has = df["Symbol"].tolist()
ls_has = ls_has[:100]
chunksize = 50
trade_pair = {}
exchange_trade_pair ={}
symbols = ['BTC','BCH','LTC','ETH']
exchanges = ['Bitfinex','Bitstamp','coinone','Coinbase','CCCAGG']
exchange = 'CCCAGG'

for symbol in symbols:
    df = p.read_csv(cwd+'/data/trading_pair/%s_trading_pair.csv' % symbol)

    x = set(df["exchange"].tolist())
    x = list(x)
    for exchange in x:
        raw_exchange = exchange
        exchange = "'"+exchange+"'"
        df = df.query('exchange == '+exchange)
        x = df["toSymbol"].tolist()
        df = df.reset_index(drop=True)
        print trade_pair
        trade_pair.update({symbol:x})
        dfs = []
        dfs.append(df)
        df = p.concat(dfs)
        exchange_trade_pair.update({raw_exchange:trade_pair})



