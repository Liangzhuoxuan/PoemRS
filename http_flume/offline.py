from pyspark.sql import SQLContext, SparkSession, HiveContext
from hdfs.client import Client
from pyspark.sql import functions
from pyspark.sql import Row
import pyspark.sql.functions as F
from pyspark.sql.types import *
import pymongo
import pandas as pd
from sqlalchemy import create_engine
import os
import re

from surprise import SVD
from surprise import Dataset
from surprise import Reader
from surprise.model_selection import cross_validate
from surprise.model_selection import GridSearchCV
from pprint import pprint
import sys

os.environ['JAVA_HOME'] = "/usr/local/src/jdk1.8.0_172"
os.environ["SPARK_HOME"] = "/usr/local/src/spark-2.2.0-bin-hadoop2.6"
os.environ["PYTHONPATH"] = "/home/liang/miniconda3/bin/python"


class SparkPreProcessing(object):
    '''基于 Spark 的数据预处理'''

    def __init__(self):
        self.spark = SparkSession.builder.appName("ddd").getOrCreate()
        sc = self.spark.sparkContext
        sqlContext = SQLContext(sc)
        hive_context = HiveContext(sc)
        # hive_context.setConf("hive.metastore.uris", "thrift://localhost:9083")


        # 读取用户信息表
        self.user_info = sqlContext.read.format("jdbc"). \
            option("url", "jdbc:mysql://39.96.165.58:3306/huamanxi"). \
            option("driver", "com.mysql.cj.jdbc.Driver"). \
            option("dbtable", "user"). \
            option("user", "root"). \
            option("password", "12345678").load()

        # 用户行为数据表，后面改为从 HDFS 读取
        self.accesslog = sqlContext.read.format("jdbc"). \
            option("url", "jdbc:mysql://39.96.165.58:3306/huamanxi"). \
            option("driver", "com.mysql.cj.jdbc.Driver"). \
            option("dbtable", "accesslog"). \
            option("user", "root"). \
            option("password", "12345678").load()
        # self.accesslog = hive_context.sql("SELECT * FROM flume_sink.useraccesslog")
        # # print(self.accesslog.count())
        # self.accesslog.show()
        # print(self.accesslog.columns)
        # sys.exit("憨憨")

        self.item_info = self.spark.read.format("csv") \
            .option("header", True) \
            .load("/home/liang/Desktop/python_file/source.csv")

    def read_accesslog_from_hdfs(self):
        # 实时日志流的存储是每5个点击数据存储一次
        client = Client("http://localhost:50070")
        file_names = client.list("/hadoop_file")
        ss = ""
        for file_name in file_names:
            with client.read("/hadoop_file/" + file_name, encoding="utf-8") as reader:
                for line in reader:
                    # 去除测试数据
                    if line.startswith("filed1"):
                        continue
                    ss += line



    def preparation(self):
        # 去除暂时用不上的字段
        accesslog = self.accesslog.drop('logId', 'username', \
                                        'accessIP', 'executionTime', "visitTime")
        # 去除 关于search 的 row
        accesslog = accesslog.filter(~accesslog.url.like("search%"))

        def get_last(row):
            '''
            只知道要处理的那一列的另外两列的名字，
            要处理那一列的名字不知道干啥了，所以用此方法
            '''
            no_idea = row.asDict()
            temp = None
            for k, v in no_idea.items():
                if k != "userId" and k != "paramter":
                    temp = v
            ans = temp[2]
            return Row(userId=row.userId, parameter=row.parameter, real_method=ans)

        # 截取 spring-aop 记录的方法名
        accesslog = accesslog.select(accesslog.userId, accesslog.parameter, \
                                     functions.split(accesslog.method, "]")).rdd.map(get_last).toDF()

        poem_related = ["showPoetryDetails", "collectPoem", "evaluatePoem", "toPoemDetails"]
        poet_related = ["collectAuthor", "showAuthorDetails"]

        # 自定义函数 udf
        def should_remove(method):
            if method in poem_related:
                return method
            return '-1'

        # 定义返回值类型为 pyspark.sql.Type.StringType
        check = F.udf(should_remove, StringType())

        # 注意使用sql筛选，String类型要加 ""
        accesslog = accesslog.withColumn('poem_related', \
                                         check(accesslog['real_method'])). \
            filter("poem_related <> '-1'")

        accesslog = accesslog.drop("real_method")
        self.accesslog = accesslog

        self.showPoetryDetails = accesslog.filter(accesslog.poem_related \
                                                  == "showPoetryDetails")

        self.collectPoem = accesslog.filter(accesslog.poem_related \
                                            == "collectPoem")
        
        self.evaluatePoem = accesslog.filter(accesslog.poem_related \
                                             == "evaluatePoem")

        self.toPoemDetails = accesslog.filter(accesslog.poem_related \
                                              == "toPoemDetails")

    def process_toPoemDetails(self):
        def reg_extract(string):
            ans = re.findall("poemId=(.+)", string)[0]
            return ans

        use_reg = F.udf(reg_extract, StringType())
        toPoemDetails = self.toPoemDetails
        toPoemDetails = toPoemDetails.select(toPoemDetails.userId, \
                                             use_reg(toPoemDetails.parameter).alias("itemId"))
        toPoemDetails = toPoemDetails. \
            withColumn("userId", toPoemDetails.userId.cast(LongType())). \
            withColumn("itemId", toPoemDetails.itemId.cast(IntegerType()))
        toPoemDetails = toPoemDetails.withColumn("rating", toPoemDetails.itemId*0+1)

        return toPoemDetails

    def process_showPoetryDetails(self):
        # 提取 parameter 字符串里边的 itemId
        def reg_extract(string):
            ans = re.findall("d=(.*?)&", string)[0]
            return ans

        get_reg_info = F.udf(reg_extract, StringType())

        showPoetryDetails = self.showPoetryDetails
        showPoetryDetails = showPoetryDetails.select(showPoetryDetails.userId, \
                                                     get_reg_info(showPoetryDetails.parameter).alias("itemId"))

        # 修改 schema 使得某个字段为 int，方便造出一个值全为 1 列
        # 时间戳用长整型
        showPoetryDetails = showPoetryDetails. \
            withColumn("userId", showPoetryDetails.userId.cast(LongType())). \
            withColumn("itemId", showPoetryDetails.itemId.cast(IntegerType()))

        # 点击行为评分为 1分
        showPoetryDetails = showPoetryDetails. \
            withColumn("rating", showPoetryDetails.itemId * 0 + 1)

        return showPoetryDetails

    def process_collectPoem(self):
        # 提取字符串里面的数据
        def reg_extract2(string):
            ans = re.findall("poemId=(.*?)&collection=(.+)", string)
            rating = None
            if ans[0][1] == '0':
                rating = '-2'
            else:
                rating = '2'
            return [ans[0][0], rating]

        get_reg_info2 = F.udf(reg_extract2, ArrayType(StringType()))

        collectPoem = self.collectPoem
        collectPoem = collectPoem.select(collectPoem.userId, \
                                         get_reg_info2(collectPoem.parameter).alias("info"))

        # 从某列的 array 中取出指定下标的值
        def get_array_element(row):
            return Row(userId=row.userId, \
                       itemId=row.info[0], rating=row.info[1])

        collectPoem = collectPoem.rdd.map(get_array_element).toDF()

        # userId 转 LongType()，保持每个分组的字段类型一致
        collectPoem = collectPoem. \
            withColumn("userId", collectPoem.userId.cast(LongType())). \
            withColumn("itemId", collectPoem.itemId.cast(IntegerType())). \
            withColumn("rating", collectPoem.rating.cast(IntegerType()))

        return collectPoem

    def process_evaluatePoem(self):
        def reg_extract3(string):
            ans = re.findall("like=(.+)&poemId=(.+)", string)
            rating = None
            if ans[0][0] == '0':
                rating = '-2'
            else:
                rating = '2'
            return [rating, ans[0][1]]

        get_reg_info3 = F.udf(reg_extract3, ArrayType(StringType()))

        evaluatePoem = self.evaluatePoem
        evaluatePoem = evaluatePoem.select(evaluatePoem.userId, \
                                           get_reg_info3(evaluatePoem.parameter).alias("info"))

        def get_array_element(row):
            return Row(userId=row.userId, \
                       itemId=row.info[1], rating=row.info[0])

        evaluatePoem = evaluatePoem.rdd.map(get_array_element).toDF()

        evaluatePoem = evaluatePoem. \
            withColumn("userId", evaluatePoem.userId.cast(LongType())). \
            withColumn("itemId", evaluatePoem.itemId.cast(IntegerType())). \
            withColumn("rating", evaluatePoem.rating.cast(IntegerType()))

        return evaluatePoem

    def processAndUnion(self):
        # 分组处理用户行为，并为不同的用户行为打分
        showPoetryDetails = self.process_showPoetryDetails()
        collectPoem = self.process_collectPoem()
        evaluatePoem = self.process_evaluatePoem()
        toPoemDetails = self.process_toPoemDetails()


        # 纵向合并分组处理的数据，保持列的顺序对齐
        user_log = evaluatePoem.union(showPoetryDetails. \
                                      select(showPoetryDetails.itemId, showPoetryDetails.rating,
                                             showPoetryDetails.userId)).union(collectPoem)

        user_log = user_log.union(toPoemDetails.select(toPoemDetails.itemId,\
                                                       toPoemDetails.rating, toPoemDetails.userId))

        # 根据 每个用户-物品对进行分组，求行为分数的总和
        user_log = user_log.groupBy(["userId", "itemId"]).sum("rating")

        # 根据评分打上标签
        user_log = user_log.select(user_log.userId, user_log.itemId, user_log["sum(rating)"].alias("rating"),
                                   F.when(user_log["sum(rating)"] > 0, 1).otherwise(0).alias("label"))

        return user_log

    def add_negative_sample(self, user_log):
        # 算出要填补的 负样本 的数量
        label_count = user_log.groupBy("label").count()
        row1, row2 = label_count.collect()
        diff = row1["count"] - row2["count"]

        # 从实时推荐处收集负样本，使得正负样本的比例为 2:1
        # TODO(laoliang):换到本地的时候记得修改 ip 和 collection 的名字
        client = pymongo.MongoClient("120.26.89.198", 27017)
        db = client["Recommendation"]
        mycol = db["rs_fucaiyang"]

        # 从负样本所在的集合随机选出文档
        # 使用 python 操作 mongodb 一定要保证传入的 json 里面的数据类型用的是 python 内置类型
        random_document = mycol.aggregate([{"$sample": {"size": int(diff // 10)}}])
        # TODO(laoliang):加入通过文本相似度计算得到的每首古诗 Top-100

        # 要向 user_log 里面添加负样本，构造数据结构 [{}, {}, {}]，后面转换为 dataframe
        # 为保证训练集和测试集要独立同分布，从实时推荐的列表里面随机选出制定数量的 user-item对
        temp_list = list()

        for document in random_document:
            userId = document.get("userId")
            fuyangben = document.get("fuyangben")

            for poemId in fuyangben:
                temp_list.append({"userId": int(userId), "itemId": poemId, \
                                  "rating": 0, "label": 0})

        # 要保证 python 数据的内置类型和 spark指定的类型相同
        schema = StructType([
            StructField("userId", LongType()),
            StructField("itemId", IntegerType()),
            StructField("rating", LongType()),
            StructField("label", IntegerType()),
        ]
        )

        # 将负样本转换为 spark dataframe
        negative_sample = self.spark.createDataFrame(temp_list, schema=schema)
        # 纵向合并原始样本集和负样本集
        user_log = user_log.union(negative_sample)

        # 此处合并了负样本和正样本，但是负样本是从实时推荐列表中保存下来的
        # 所有可能有的是用户点击了的样本，找出这些样本并去除
        user_log = user_log.groupBy(["userId", "itemId"]).sum("rating")

        user_log = user_log.select(user_log.userId, user_log.itemId, user_log["sum(rating)"].alias("rating"),
                                   F.when(user_log["sum(rating)"] > 0, 1).otherwise(0).alias("label"))

        return user_log

    def merge_userInfo_itemData(self, user_log):
        '''将行为数据与用户信息、物品信息进行合并'''
        one_hot_df = self.spark.read.format("csv") \
            .option("header", True) \
            .load("/home/liang/Desktop/python_file/one_hot_df.csv")

        one_hot_df = one_hot_df.drop("poemDynasty")
        # 因为要根据主键进行合并，要保证主键的数据类型一致
        one_hot_df = one_hot_df \
            .withColumn("poemId", one_hot_df.poemId.cast(IntegerType())) \
            .withColumn("poemStar", one_hot_df.poemStar.cast(FloatType()))

        # 相当于 inner join，但是 inner 会报错，所以用这个方法吧
        train_sort_model_df = user_log.join(one_hot_df, user_log.itemId \
                                            == one_hot_df.poemId, "outer").dropna()

        user_info = self.user_info
        user_info = user_info \
            .withColumn("userId", user_info.userId.cast(LongType()))
        # 合并用户数据
        train_sort_model_df = train_sort_model_df.join(user_info.select(user_info.userId, user_info.userSex),\
                                                       user_info.userId == train_sort_model_df.userId)


        # 经过处理后的数据往往就小很多了，转为pandas处理
        # 将 spark dataframe 转换为 pandas dataframe
        dataset = train_sort_model_df.toPandas()
        # 从 MySql 读取 用户喜欢的诗人数据，构造特征
        engine = create_engine('mysql+pymysql://root:12345678@39.96.165.58:3306/huamanxi')
        userandauthor = pd.read_sql_table('userandauthor', engine)

        # 为了区分诗人id和古诗id，诗人id加20w
        userandauthor.userId = userandauthor.userId.astype("int64")
        userandauthor["authorId"] += 200000

        temp_list = []

        # 对比两个表看当前操作的古诗的诗人是否是用户收藏了的诗人
        for row in dataset.iterrows():
            temp = row[1].tolist()
            userId = temp[0]
            authorId = int(temp[5])
            if authorId in userandauthor[userandauthor["userId"] \
                                         == userId]["authorId"].values:
                temp.append(1)
            else:
                temp.append(0)
            temp_list.append(temp)

        columns = dataset.columns.tolist()
        columns.append('collectAuthor')
        df = pd.DataFrame(temp_list, columns=columns)
        # 去除排序模型建模不需要的列
        df.drop(columns=["userId", "itemId", "poemAuthorId", "poemId"], inplace=True)
        # 将 one-hot编码的特征转换为 int，为 Xgboost 建模需要
        # print(list(df.columns)[3:-2])
        # print(df.info())

        for i in df.columns[3:-2]:
            df[i] = df[i].astype("int64")
        # sys.exit("yundan")
        return df

    def preprocessing(self):
        self.preparation()
        user_log = self.processAndUnion()  # pyspark.sql.dataframe
        user_log = self.add_negative_sample(user_log)
        train_sort_model_dataset = self.merge_userInfo_itemData(user_log)  # pandas.dataframe
        print(train_sort_model_dataset)
        return user_log, train_sort_model_dataset


class CollaborativeFilteringBaseModel(object):
    '''基于模型的协同过滤'''

    def __init__(self, user_log):
        self.user_log = user_log.toPandas()
        print(self.user_log.shape)

    def train_model(self) -> dict:
        reader = Reader(rating_scale=(-2, 5))
        data = Dataset.load_from_df(self.user_log[["userId", "itemId", \
                                                   "rating"]], reader)


        # 搜索的参数范围
        param_grid = {'n_factors': list(range(20, 110, 10)), 'n_epochs': list(range(10, 30, 2)),
                      'lr_all': [0.002, 0.005, 0.01], 'reg_all': [0.02, 0.1, 0.6, 1, 2, 3, 4, 5]}

        gs = GridSearchCV(SVD, param_grid, measures=['rmse', 'mae'], cv=5)
        gs.fit(data)

        print(gs.best_score["rmse"])
        # 用 网格搜索出来的最佳参数训练模型, 因为在gs里面直接取出estimator会报错
        best_param = gs.best_params["rmse"]
        trainset = data.build_full_trainset()
        model = SVD(n_factors=best_param["n_factors"], n_epochs=best_param["n_epochs"], \
                    lr_all=best_param["lr_all"], reg_all=best_param["reg_all"])
        model.fit(trainset)

        # TODO(laoliang):为了增加古诗推荐的新颖性, 会将每个用户对应的 top-100 放到 Mongodb, 在实时推荐的时候，作为固定的召回集
        itemId_set = list(set(self.user_log.itemId))
        userId_set = list(set(self.user_log.userId))

        # 记录每位用户对应的粗排候选集 Top-100
        est_dict = dict()

        for uid in userId_set:
            est_dict[uid] = list()
            for iid in itemId_set:
                est = model.predict(uid, iid).est
                est_dict[uid].append((iid, est))
            # 根据矩阵的评分进行排序
            est_dict[uid] = [rs_iid[0] for rs_iid in \
                             sorted(est_dict[uid], key=lambda x: x[1], reverse=True)]

        return est_dict  # uid: iid_list


if __name__ == '__main__':
    preprocessing = SparkPreProcessing()
    # TODO(laoliang): 修改  训练SVD的数据集不应该加入负样本
    CollaborativeFiltering_dataset, sortModel_dataset = preprocessing.preprocessing()
    CollaborativeFiltering_dataset.show()
    # 基于模型的协同过滤
    # base_model = CollaborativeFilteringBaseModel(CollaborativeFiltering_dataset)
    # model = base_model
    # candidate_set = model.train_model()
    # pprint(candidate_set)
