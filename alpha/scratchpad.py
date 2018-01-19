import quandl
import pandas
stock = 'CME/KWZ1991'



df = quandl.get(stock, authtoken="kzmH8ENEsNUc5GkS9bum")

print(df)