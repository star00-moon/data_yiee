/*
   ODS层建模及数据导入

*/

drop table if exists yiee.ods_eventlog;
create external table if not exists yiee.ods_eventlog
(
cookieid   string        ,
account   string         ,
imei   string            ,
osName   string          ,
osVer   string           ,
resolution   string      ,
androidId   string       ,
manufacture   string     ,
deviceId   string        ,
appid   string           ,
appVer   string          ,
release_ch   string      ,
promotion_ch   string    ,
areacode   bigint          ,
longtitude   double      ,
latitude   double        ,
carrier   string         ,
netType   string         ,
sessionId   string       ,
eventType   string       ,
commit_time   bigint     ,
event  map<string,string> ,
province  string        ,
city   string            ,
district   string   
)
partitioned by (dt string)
stored as parquet
location '/eventlog'
;     


load data inpath '/eventlog/2019-06-16' into table yiee.ods_eventlog partition(dt='2019-06-16');
alter table yiee.ods_eventlog add partition(dt='2019-06-16') location '/eventlog/2019-06-16';
alter table yiee.ods_eventlog drop partition(dt='2019-06-16');






















