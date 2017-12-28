import requests
import json

url = 'https://min-api.cryptocompare.com/data/histohour?fsym=' \
      +'ETH'+'&tsym='+ 'USD' +'&limit='+'2'+'&aggregate=1&e='+ \
      'CCCAGG'
resp = requests.get(url=url)
data = json.loads(resp.text)

if data["Data"]:
    print 'x'