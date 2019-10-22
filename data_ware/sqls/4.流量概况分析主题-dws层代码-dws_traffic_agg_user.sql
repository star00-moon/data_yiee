/*
	主题：流量概况分析主题
	源表：dws_traffic_agg_session
	目标表：dws_traffic_agg_user
*/
-- 首次创建目标表
drop table if exists dws_traffic_agg_user;
create table dws_traffic_agg_user(
uid string,
session_cnts int,
time_amt bigint,
pv_cnts int,
province string,
city string, 
district string,
manufacture string,
osname string,
osversion string
)
partitioned by (dt string)
stored as parquet
;

-- ETL计算
insert overwrite table dws_traffic_agg_user partition(dt='2019-06-16')
select
uid,
count(1) as session_cnts,
sum(end_time-start_time) as time_amt,
sum(pv_cnts) as pv_cnts,
max(province) as province,
max(city) as city,
max(district) as district,
max(manufacture) as manufacture,
max(osname) as osname,
max(osversion) as osversion
from dws_traffic_agg_session
where dt='2019-06-16'
group by uid;