/*
   @主题： 日新日活分析
   @dws层：日新数的多维cube计算流量概况统计主题：
   @src：  日新明细表：dwd_dnu_dtl
   @dst:   日新数cube：dws_dnu_cube
*/
-- @dst 建表
drop table if exists  dws_dnu_cube;
create table dws_dnu_cube(
province string,
city string,
district string,
release_ch string,
manufacture string,
dnu_cnt int
)
partitioned by (dt string)
stored as parquet
;

-- etl计算
insert into table dws_dnu_cube partition(dt='2019-06-16')
select
province,
city,
district,
release_ch,
manufacture,
count(1) as dnu_cnt
from dwd_dnu_dtl
where dt = '2019-06-16'
group by province,city,district,release_ch,manufacture
with cube
;