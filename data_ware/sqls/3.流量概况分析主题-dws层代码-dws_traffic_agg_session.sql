/* 
	流量概况分析主题：DWS层，会话聚合表
*/
drop table if exists yiee.dws_traffic_agg_session;
create table yiee.dws_traffic_agg_session(
uid string,
sessionid string,
start_time bigint,
end_time bigint,
pv_cnts int,
province string,
city string,
district string,
manufacture string,
osname string,
osver string,
release_ch string
)
partitioned by (dt string)
stored as parquet
;

/*
   计算生成
*/

insert into table yiee.dws_traffic_agg_session partition(dt='2019-06-16')
select
uid,
sessionid,
min(commit_time) as start_time,
max(commit_time) as end_time,
count(1) as pv_cnts,
max(province) as province,
max(city) as city,
max(district) as district,
max(manufacture) as manufacture,
max(osname) as osname,
max(osver) as osversion,
max(release_ch) as release_ch
from yiee.dwd_traffic_dtl
where dt='2019-06-16'
group by uid,sessionid
;




