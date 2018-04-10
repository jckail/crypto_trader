SELECT DISTINCT
  a1.symbol
  , a1.points
  ,a1.likes
  , from_unixtime(a1.utc) AS "DATE"

FROM litcryptodata.facebook a1
WHERE from_unixtime(a1.utc)  IS NOT NULL
      and points > 0
      and symbol in (
  'BTC'
  ,'BCH'
  ,'LTC'
  ,'ETH'
  ,'XRP'



)
ORDER BY a1.symbol, from_unixtime(a1.utc);