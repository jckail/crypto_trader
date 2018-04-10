SELECT
  symbol
  , date_add('hour', -7, from_unixtime(utc)) AS "mst"
  ,from_unixtime(time) as "timestamp"
  , exchange

  , high

  , high - (lag(high, 1)
OVER (
  PARTITION BY symbol
  ORDER BY exchange, utc ))                     as "last_high_dff"

  , low
  , low - (lag(low, 1)
OVER (
  PARTITION BY symbol
  ORDER BY exchange, utc ))                     as "last_low_dff"

  , close
  , close - (lag(close, 1)
OVER (
  PARTITION BY symbol
  ORDER BY exchange, utc ))                     as "last_low_dff"

  , open
  , open - (lag(open, 1)
OVER (
  PARTITION BY symbol ,exchange
  ORDER BY  utc ))                    as "last_open_dff"

  , volumefrom
  , volumefrom - (lag(volumefrom, 1)
OVER (
  PARTITION BY symbol
  ORDER BY exchange, utc ))                     as "last_volumefrom_dff"

  , volumeto
  , volumeto - (lag(volumeto, 1)
OVER (
  PARTITION BY symbol
  ORDER BY exchange, utc ))                   as "last_volumeto_dff"


  , avg(high + low)                             as "avgpricelowhigh"
  , (volumeto - volumefrom)                     as "flowtodelta"
  , (close - open)                              as "closeopen"
  , souce
  , time
  , from_unixtime(utc)                       AS "gmt"
  , utc
  , hostname

FROM litcryptodata.minute_data

WHERE symbol IN ('BTC', 'BCH', 'LTC', 'ETH', 'XRP')

GROUP BY
  date_add('hour', -7, from_unixtime(utc))
  , from_unixtime(utc)
  , CLOSE
  , exchange
  , high
  , hostname
  , low
  , OPEN
  , souce
  , symbol
  , TIME
  , utc
  , volumefrom
  , volumeto
  , (volumeto - volumefrom)
  , (close - open)

ORDER BY symbol,exchange, utc DESC
