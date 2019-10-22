## 代码和数据加载说明
注意：标记为数字的是类名称，空心点的是类所需要加载的数据

### 第一天
1. demo.graphx.Demo1    图计算入门demo
2. demo.graphx.Demo1_2  利用idmapping（id映射字典）来对日志进行加工，为每条日志添加一个gid字段
    - user_profile/demodata/graphx/input    图计算日志

### 第二天
1. cn.doitedu.profile.idmp.TdayIdmp      T日IDMP计算程序     [????  映射边集合 edges]
    - user_profile/demodata/idmp/input/day01/cmcclog
    - user_profile/demodata/idmp/input/day01/dsplog
    - user_profile/demodata/idmp/input/day01/eventlog
1. cn.doitedu.commons.utils.IdsExtractor        【yiee_commons】各类源数据的id标识抽取程序    
1. cn.doitedu.commons.utils.ShowParquet         【yiee_commons】查看Parquet格式文件类
1. cn.doitedu.profile.idmp.RegularIdmp          常规的id映射字典计算程序
    - user_profile/data/cmcclog/day01
    - user_profile/data/dsplog/day01
    - user_profile/data/eventlog/day01
1. cn.doitedu.commons.utils.EventJson2Bean      【yiee_commons】事件日志json转成EventLogBean
1. cn.doit.crawler.demo.HttpClientDemo          【yiee_crawler】用于理解啥叫爬虫
1. cn.doitedu.profile.preprocess.DspLogPre      DSP竞价日志预处理
    - user_profile/data/dsplog/day01  加载原始数据
    - user_profile/data/appdict 加载app信息字典
    - user_profile/data/areadict 加载地域信息字典
    - user_profile/data/output/idmp/day01 加载idmp映射字典
1. cn.doitedu.profile.preprocess.DspLogBean.scala     DspLogPre类的bean对象
1. cn.doitedu.commons.utils.DictsLoader     各类字典加载工具

### 第三天    
1. cn.doitedu.profile.preprocess.EventLogPre    公司商城系统用户行为事件日志预处理       （DspLogPre和EventLogPre代码基本一样）
    - user_profile/data/eventlog/day01    事件日志文件
    - user_profile/data/areadict 地域字典
    - user_profile/data/output/idmp/day01   加载idmp字典
1. cn.doitedu.commons.beans.EventLogBean
1. demo.hanlp.HanLpDemo     HanLp自然语言处理工具包应用示例
1. cn.doit.crawler.demo.HttpClientDemo          【yiee_crawler】用于理解啥叫爬虫
1. cn.doit.crawler.demo.JianDanPic              【yiee_crawler】煎蛋网妹子图爬取

### 第四天
1. cn.doit.crawler.appinfo.AppChina             【yiee_crawler】appchina应用下载市场app信息爬取程序
1. cn.doit.crawler.goodsinfo.JingDongGoods      京东葡萄酒类商品信息抓取程序，保存【商品标题】和【商品详情页地址】
1. cn.doitedu.profile.preprocess.CmccLogPre cmcc第三方数据预处理
    - user_profile/data/output/idmp/day01    
    - yiee_crawler/data/jdgoods
    - user_profile/data/cmcclog/day01
2. cn.doitedu.profile.tagextract.DspTagExtractor    dsp竞价日志数据标签抽取程序

### 第五天
1. cn.doitedu.profile.tagextract.CmccTagExtractor   CMCC流量数据的标签抽取
1. cn.doitedu.profile.tagextract.EventLogTagExtractor   商城系统用户行为日志标签抽取
    - user_profile/data/t_user_goods 商品信息表
1. test.java.Df2Ds.scala    dataframe  转 dataset
1. cn.doitedu.profile.tagextract.UserOrderTagsExtractor     数仓报表：用户订单统计表 标签抽取程序
    - user_profile/data/output/idmp/day01
    - user_profile/data/t_ads_user_order_tag
1. cn.doitedu.profile.tagextract.AdsUserGoodsTagExtractor       用户订单商品退拒分析报表 标签抽取
6. cn.doitedu.profile.tagextract.DemoTagsReader     //加载处理好的测试数据
    - user_profile/demodata/tags/day02/cmcctags     
    - user_profile/demodata/tags/day02/dsptags
    - user_profile/demodata/tags/day02/eventtags
    - user_profile/demodata/tags/day02/usergoodstags
    - user_profile/demodata/tags/day02/userordertags
1. cn.doitedu.profile.tagcombine.CurrentDayTagsCombiner     当日各数据源所抽取的标签的聚合程序
    - user_profile/data/output/eventlog/day01
    - user_profile/data/output/cmcc/day01
    - user_profile/data/output/dsplog/day01
    - user_profile/data/t_user_goods
    - user_profile/data/t_ads_user_order_tag
    - user_profile/data/output/idmp/day01
1. cn/doitedu/profile/tagcombine/HisAndTodayTagCombiner.scala       将当日的标签计算结果，整合历史（前一日）标签结果
    - user_profile/data/output/tags/day01
    - user_profile/data/output/tags/day02

### 第六天
5. cn.doitedu.profile.tagexport.ProfileTags2Hbase

### 第七天
1. cn.doitedu.profile.tagexport.ProfileIndex2Hbase
1. cn.doitedu.profile.modeltags.LossProbTagModuleTrainer    
    - user_profile/data/modeltags/lossprob/modeltag_sample
2. cn.doitedu.profile.modeltags.LossProbTagPredict
    - user_profile/data/modeltags/lossprob/modeltag_test

### controller层
1. cn.doitedu.course.data_service.controller.DetailTagsQueryController
2. cn.doitedu.course.data_service.controller.HelloController

### dao层
1. cn.doitedu.course.data_service.dao.impl.TagsQueryDaoImpl
2. cn.doitedu.course.data_service.dao.ITagsQueryDao

### service层
1. cn.doitedu.course.data_service.service.impl.TagsQueryServiceImpl
2. cn.doitedu.course.data_service.service.ITagsQueryService

### 页面
1. static/echarts.min.js
2. static/index.html


## 日志数据说明
1. user_profile/data/appdict        //appdict字典数据
1. user_profile/data/areadict       //地域字典数据
1. user_profile/data/cmcclog        //移动运行商数据
1. user_profile/data/dsplog         //dsp日志
1. user_profile/data/eventlog       //
1. user_profile/data/idmpdict
1. user_profile/data/modeltags/lossprob/modeltag_sample
1. user_profile/data/modeltags/lossprob/modeltag_test
1. user_profile/data/t_ads_user_order_tag
1. user_profile/data/t_user_goods
2. user_profile/demodata/graphx/input      //graphx图数据
2. user_profile/demodata/idmp/input/day01/cmcclog/day01  用户画像-数据源说明-运营商流量日志
2. user_profile/demodata/idmp/input/day01/dsplog
2. user_profile/demodata/idmp/input/day01/eventlog
2. user_profile/demodata/idmp/input/day02/cmcclog       demo数据
2. user_profile/demodata/idmp/input/day02/dsplog        demo数据
2. user_profile/demodata/idmp/input/day02/eventlog      demo数据
2. user_profile/demodata/tags/day01/cmcctags        //移动运行商标签数据
2. user_profile/demodata/tags/day01/dsptags
2. user_profile/demodata/tags/day01/eventtags
2. user_profile/demodata/tags/day01/usergoodstags
2. user_profile/demodata/tags/day01/userordertags
2. user_profile/demodata/tags/day02/cmcctags
2. user_profile/demodata/tags/day02/dsptags
2. user_profile/demodata/tags/day02/eventtags
2. user_profile/demodata/tags/day02/usergoodstags
2. user_profile/demodata/tags/day02/userordertags