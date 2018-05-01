select
  distinct
  lg.asofdate
  ,lg."usd (pm)" gold_price
  ,dd.close btc_price
  ,dd2.close eth_price
  ,dd3.close bch_price
  ,dd4.close ltc_price
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

  join day_data dd3
    ON date(from_unixtime (dd3.time)) =  asofdate
       AND dd3.symbol = 'BCH' AND dd3.exchange = 'CCCAGG'

  join day_data dd4
    ON date(from_unixtime (dd4.time)) =  asofdate
       AND dd4.symbol = 'LTC' AND dd4.exchange = 'CCCAGG'

where lg.asofdate >= date('2016-07-31') --firstday bitcoin cash was on cccagg

order by lg.asofdate

