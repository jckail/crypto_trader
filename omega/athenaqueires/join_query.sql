select
  distinct
  lg.asofdate
  ,lg."usd (pm)" gold_price
  ,dd.close btc_price
  ,dd2.close eth_price
from
  (
    SELECT
      *
      ,date(cast(substring(lg."date",1,10)as varchar)) asofdate
    FROM lbma_gold lg
    WHERE substring(lg."date",1,10) like '%-%'
          and substring(lg."date",1,1) != '-')lg

  join day_data dd
    ON date(from_unixtime (dd.time)) =  asofdate
       AND dd.symbol = 'BTC' AND dd.exchange = 'CCCAGG'

  join day_data dd2
    ON date(from_unixtime (dd2.time)) =  asofdate
       AND dd2.symbol = 'ETH' AND dd2.exchange = 'CCCAGG'

