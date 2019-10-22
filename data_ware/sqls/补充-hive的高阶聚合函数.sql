create table demo2(
name string,
prov string,
sex string,
job string,
score int
)
row format delimited fields terminated by ','
;

load data local inpath '/root/demo2.dat' into table demo2;

U1,HN,M,STU,10
U2,HN,M,STU,12
U3,HN,F,TEA,20
U4,HB,M,STU,8
U5,HB,F,TEA,2
U6,HB,M,TEA,12


--得分总数的数据cube构建
select
prov,
sex,
job,
sum(score) as amt
from demo2
group by prov,sex,job
with cube
;

--
select
prov,
sex,
job,
sum(score) as amt
from demo2
group by prov,sex,job
grouping sets((prov),(sex),(job),(prov,sex),())
;

--
select
prov,
sex,
job,
sum(score) as amt
from demo2
group by prov,sex,job
with rollup
;







