


xheaders = ['close', 'high', 'low', 'open', 'time', 'volumefrom', 'volumeto', 'coin_units', 'exchange', 'hostname', 'symbol', 'timestamp_api_call']
for header in xheaders:
    print('%'+'{PATTERN:%s}'% header)