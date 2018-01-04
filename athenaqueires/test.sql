
SHOW TABLES from litcryptodata
/*
SHOW COLUMNS FROM table facebook


SHOW TABLES from litcryptodata


select followers from cryptocompare


where symbol = 'XRP'



/*

select points, likes,talking_about from facebook


where symbol = 'XRP'

select points, active_users,comments_per_day, comments_per_hour,
 posts_per_day,posts_per_hour,subscribers

from reddit

where symbol = 'XRP'

select favourites,followers,statuses from twitter

where symbol = 'XRP'
*/
CREATE EXTERNAL TABLE IF NOT EXISTS integer_table (
KeyColumn STRING,
Column1 INT)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'

WITH SERDEPROPERTIES ('serialization.format' = ',',
'field.delim' = ',')
LOCATION 's3://litcryptodata/integer_table/'
*/
