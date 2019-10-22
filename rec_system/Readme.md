## 代码和数据加载说明

### 第一天
1. cn.doitedu.sparkml.demos.VectorDemo      spark mllib 底层编程接口:Vector向量示例程序,欧氏距离和余弦距离实现
1. cn.doitedu.sparkml.demos.VectorDemo2
	- rec_system/demodata/vecdata/vec.csv
1. cn.doitedu.sparkml.demos.VectorNormDemo  向量规范化api示例
1. cn.doitedu.sparkml.demos.MatrixDemo      spark mllib底层编程接口：矩阵Matrix
	- rec_system/demodata/vecdata/vec.csv

### 第二天
1. cn.doitedu.sparkml.knn.HandWritingFeature    手写数字识别算法
	- G:\\testdata\\knn\\\\testDigits"
1. cn.doitedu.smallfiles.TestSmallFile      如何处理大量小文件
	- G:\testdata\knn\trainingDigits
1. cn.doitedu.sparkml.knn.HandWritingRecognize  手写数字识别
	- rec_system/demodata/digitTranningVec
	- rec_system/demodata/digitTestVec
1. cn.doitedu.sparkml.bayes.BayesTest       文本分类，bayes示例程序
	- rec_system/demodata/bayes_demo_data/sample.txt  
1. cn.doitedu.sparkml.bayes.BayesCommentModelTrainer	    评论分类贝叶斯模型训练器
	- G:\testdata\comment\poor
	- G:\testdata\comment\general
	- G:\testdata\comment\good
1. cn.doitedu.sparkml.bayes.BayesCommentClassify    用训练好的bayes模型来预测未知类别的评论
	- rec_system/data/test_comment/u.comment.dat
	- rec_system/data/comment_bayes_model
1. cn.doitedu.sparkml.kmeans.KMeansDemo		    利用kmeans做人群聚类分析  示例程序
	- rec_system/data/kmeans/a.txt

### 第三天
1. cn.doitedu.sparkml.LogisticRegressionGender  利用逻辑回归分类算法来对用户进行消费行为性别预测
	- rec_system/data/lgreg/lgreg_sample.csv
	- rec_system/data/lgreg/lgreg_test.csv
1. cn.doitedu.recomment.cb.RateScore        用户对物品的喜好度计算
	- rec_system/data/ui_rate/event.log
	- rec_system/data/ui_rate/u.comment.dat
1. cn.doitedu.recomment.cb.ItemSimilarity   物品相似度计算
	- rec_system/data/ui_rate/item.profile.dat	
1. cn.doitedu.recommend.cf.CFRec
    - rec_system/data/cb_out/ui
1. cn.doitedu.sparkml.fpgrowth.RecFpGrowth  基于关联规则分析算法fp-growth进行推荐计算
	- rec_system/data/fpgrowth_data/input/sample.dat
	- rec_system/data/fpgrowth_data/input/test.data
1. cn.doitedu.recomment.cb.RecForUser       根据用户评分UI矩阵，物品物品相似矩阵，做推荐(学生自己写)


	
	
