import quandl
quandl.ApiConfig.api_key = "kzmH8ENEsNUc5GkS9bum"
data = quandl.get_table('LME/ST', paginate=True)

print (data)