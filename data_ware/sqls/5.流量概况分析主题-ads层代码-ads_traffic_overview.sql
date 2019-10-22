/*
	@src：dws_traffic_agg_user
	@dst: ads_traffic_overview
*/

-- @dst表创建
drop table if exists ads_traffic_overview;
create table ads_traffic_overview(
dt string comment '日期',
pv_cnts int comment 'pv总数',
uv_cnts int comment 'uv总数',
session_cnts int,
avg_time_session double,
avg_session_user double,
avg_depth_user double,
avg_time_user double,
recome_user_ratio double
)
;


-- ETL计算
insert into table ads_traffic_overview
select
'2019-06-16' as dt,
sum(pv_cnts) as pv_cnts,
count(1) as uv_cnts,
sum(session_cnts) as session_cnts,
sum(time_amt)/sum(session_cnts) as avg_time_session,
sum(session_cnts)/count(1) as avg_session_user,
sum(pv_cnts)/count(1) as avg_depth_user,
sum(time_amt)/count(1) as avg_time_user,
-- sum(if(session_cnts>1,1,0))/count(1) as recome_user_ratio
count(if(session_cnts>1,1,null))/count(1) as recome_user_ratio
from dws_traffic_agg_user 
where dt='2019-06-16'
