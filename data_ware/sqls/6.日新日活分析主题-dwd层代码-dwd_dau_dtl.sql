/*
	@src：dwd_traffic_dtl
	@dst: dwd_dau_dtl
*/
-- @dst表创建
drop table if exists dwd_dau_dtl;
create table dwd_dau_dtl(
uid string,
province string,
city string,
district string,
release_ch string,
manufacture string
)
partitioned by (dt string)
stored as parquet
;


-- etl计算
insert into table dwd_dau_dtl partition(dt='2019-06-16')
select
uid,
max(province) as province,
max(city) as city,
max(district) as district,
max(release_ch) as release_ch,
max(manufacture) as manufacture
from dwd_traffic_dtl
where dt='2019-06-16'
group by uid;

