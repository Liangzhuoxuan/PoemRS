# PoemRS

## 1 推荐服务
- 相似古诗推荐
- 猜你喜欢推荐
- 每日推荐

## 2 推荐服务架构设计
- 使用基于大数据的推荐架构
  - 基于 `Hadoop` 的数据存储
  - 基于 `Spark` 的数据预处理和特征工程（离线层）
  - 部分模型基于`Spark ML` 建模
  - 基于 `Hive` 的数据管理
  - 基于 `Flume + Spark-Streaming + Redis` 的数据采集及传输（在线层）


![推荐架构图](https://img-blog.csdnimg.cn/20201121110524763.jpg?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3FxXzQzMzkxMzgz,size_10,color_FFFFFF,t_70)


## 3 推荐算法
- 使用带偏置的 `MF` 模型 `SVD` 作为召回模型
- 使用 `Xgboost  + LR` 作为排序模型
- 使用 `Word2Vec` 计算古诗的词向量，与每个词的 `TF-IDF` 值作为参数，进行古诗文本相似度计算
- 提供 经典排序模型 `LR` 、`FM`、`Xgb+LR`、`Xgb+FM` 的模型测试对比


## 4 所使用的语言及框架版本
```
jdk-1.8.0_172
python 3.6.4
hadoop-2.6.5
spark-2.2.0-bin-hadoop2.6
scala-2.11.12
apache-flume-1.9.0
mongodb-linux-x86_64-4.0.13
redis-5.0.7
```
