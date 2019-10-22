/*
	@src：dwd_hisu_dtl历史 ^ dwd_dau_dtl日活
	@dst: dwd_dnu_dtl日新
*/
-- @dst 日新表创建
drop table if exists dwd_dnu_dtl;
create table dwd_dnu_dtl(
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
with dau as(
select * from dwd_dau_dtl where dt='2019-06-16'
)
insert into table dwd_dnu_dtl partition(dt='2019-06-16')
select 
  uid,
  province,
  city,
  district,
  release_ch,
  manufacture
from 
   (
      select
      a.uid,
      b.uid as b_uid,
      a.province,
      a.city,
      a.district,
      a.release_ch,
      a.manufacture
      from dau a
      left join 
       dwd_hisu_dtl b
      on a.uid = b.uid  
   ) o
where o.b_uid is null;








