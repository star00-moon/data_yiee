/*
   造数据1： 用户访问路径记录表，在hive数仓 dws_user_acc_route
*/
uid,sid,step,url,ref
u01,s01,1,X
u01,s01,2,Y,X
u01,s01,3,A,Y
u01,s01,4,B,A
u01,s01,5,C,B
u01,s01,6,B,C
u01,s01,7,o,B
u02,s02,1,A
u02,s02,2,C,A
u02,s02,3,A,C
u02,s02,4,B,A
u02,s02,5,D
u02,s02,6,B,D
u02,s02,7,C,B


/*
    造数据2： 业务转化路径定义表，在元数据管理中  transaction_route
       T101	1	步骤1	A	null
       T101	2	步骤2	B	A
       T101	3	步骤3	C	B
       T102	1	步骤1	D	null
       T102	2	步骤2	B	D
       T102	3	步骤3	C	B
*/


/*
	计算步骤：
		1. 加载 元数据库中  transaction_route 表，整理格式
		2. 读取 数仓中的  dws_user_acc_route 表
		3. 对用户访问路径数据，按session分组，将一个人的一次会话中的所有访问记录整合到一起
		        u01,s01,1,X
                u01,s01,2,Y,X
                u01,s01,3,A,Y
                u01,s01,4,B,A
                u01,s01,5,C,B
                u01,s01,6,B,C
                u01,s01,7,o,B
		
		4. 然后依照公司定义的规则，判断这个人是否完成了 ？ 业务路径中的 ？ 步骤，如果完成了，则输出如下数据：
				u01   t101   step_1
				u01   t101   step_2
				u01   t102   step_1
				u02   t101   step_1
				u02   t102   step_1
				u02   t102   step_2		
				......
				
		5. 对上述数据，按业务id和步骤号，分组统计人数，得到结果：
				t101  step_1   2
				t101  step_2   1
				t101  step_3   1
				t102  step_1   3
				t102  step_2   2
				......
*/



-- 写代码







