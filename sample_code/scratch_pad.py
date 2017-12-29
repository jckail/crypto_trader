import requests

url = "https://min-api.cryptocompare.com/data/histominute"

querystring = {"e":"Coinbase","fsym":"CCRB","toTs":"1514482833","limit":"2000","tsym":"USD","aggregate":"3"}

headers = {
    'cache-control': "no-cache",
    'postman-token': "57fcfe29-47e3-5b29-986d-34444dd1c71c"
}

response = requests.request("GET", url, headers=headers, params=querystring)

print response.status_code
print(response.text)