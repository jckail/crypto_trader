import requests
import pandas as p

url = "https://min-api.cryptocompare.com/data/pricemultifull"

querystring = {"fsyms":"BTC","tsyms":"USD,EUR"}

headers = {
    'cache-control': "no-cache",
    'postman-token': "f3d54076-038b-9e2d-1ff3-593ae13aabbf"
}

response = requests.request("GET", url, headers=headers, params=querystring)

data = response.json()


for key in data.keys():
    print(key)
data_extract = data["DISPLAY"]['BTC']



print type(data_extract)

df = p.DataFrame.from_dict(data_extract,orient='index', dtype=None)

print df