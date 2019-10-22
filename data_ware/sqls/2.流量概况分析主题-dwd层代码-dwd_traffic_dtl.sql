/*
   流量概况统计主题： DWD层建模及计算
*/
drop table if exists yiee.dwd_traffic_dtl;
create external table yiee.dwd_traffic_dtl
(
uid  string ,
osName string,
osVer  string,
resolution string      ,
manufacture             string     ,
appid                  string           ,
appVer                  string          ,
release_ch   string      ,
promotion_ch   string    ,
carrier   string         , -- 运营商
netType   string         , -- 网络类型
sessionId   string       , -- 会话id
commit_time   bigint     ,
province  string        ,
city   string            ,
district   string   ,
url string
)
partitioned by (dt string)
stored as parquet
;     

/*
    计算
*/
insert into table yiee.dwd_traffic_dtl partition(dt='2019-06-16')
select
COALESCE(
 if(trim(account) ='',null,account),
 if(trim(imei) = '',null,imei),
 if(trim(androidid) = '',null,androidid),
 if(trim(deviceid) = '',null,deviceid),
 if(trim(cookieid) = '',null,cookieid)
) as uid,
osName               ,
osVer               ,
resolution         ,
manufacture        ,
appid               ,
appVer               ,
release_ch         ,
promotion_ch       ,
carrier            ,
netType            ,
sessionId          ,
commit_time        ,
province           ,
city               ,
district           ,
event['url'] as url
from yiee.ods_eventlog
where dt='2019-06-16' and eventType='pg_view'
;







