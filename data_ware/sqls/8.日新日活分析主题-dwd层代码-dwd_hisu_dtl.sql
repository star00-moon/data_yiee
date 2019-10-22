/*
	@src：dwd_dau_dtl
	@dst: dwd_hisu_dtl
*/
drop table if exists dwd_hisu_dtl;
create table dwd_hisu_dtl(
uid string,
first_login string,
last_login string
)
stored as parquet
;

-- etl 计算
-- 逻辑： 用历史表的上一天的状态  full join 当日的日活表
                   -- 左右都有的，就是历史用户今天又活跃，最后登陆=当日
                   -- 左有，右没有，就是历史用户今日没活跃，取左表
				   -- 左没有，右有，就是一个新用户，首次登陆=当日，最后登陆=当日
with dau as (select * from dwd_dau_dtl where dt='2019-06-16')

insert overwrite table dwd_hisu_dtl
select
if(uid is not null,uid,b_uid) as uid,
if(first_login is not null,first_login,b_dt) as first_login,
if(b_uid is not null,b_dt,last_login) as last_login

from 
(
select
a.uid,
a.first_login,
a.last_login,
b.uid as b_uid,
b.dt as b_dt
from dwd_hisu_dtl a
full join 
dau b
on a.uid = b.uid
) o
;
