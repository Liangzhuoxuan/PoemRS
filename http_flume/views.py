from django.shortcuts import HttpResponse, render
import json
import requests
import random
import pymongo
from django_redis import get_redis_connection
import pickle
import pymysql
# from .offline import SparkPreProcessing
import numpy as np
from sklearn.feature_extraction import DictVectorizer
from pyfm import pylibfm
import xgboost as xgb
from sklearn.preprocessing import OneHotEncoder

# 预加载模型
feature_encoder = pickle.load(open("/usr/local/http_flume/http_flume/Xgboost_model.pk", "rb"))
fm = pickle.load(open("/usr/local/http_flume/http_flume/fm_model.pk", "rb"))
v = pickle.load(open("/usr/local/http_flume/http_flume/DictVectorizer.pk", "rb"))

# 加载特证名列表
column_name = pickle.load(open("/usr/local/http_flume/http_flume/column_name.pk", "rb"))[2:]

# 连接 Mongodb
client = pymongo.MongoClient("120.26.89.198", 27017)
mycol = client['Recommendation'] # 数据库
houxuanji = mycol['find_query'] # 分类->古诗 表
rs_fucaiyang = mycol['rs_fucaiyang'] # 负采样记录表
Word2Vec_sim = mycol['Word2Vec_sim'] # 文本相似度最高的 古诗id->Top-100 表


# 连接 mysql
connection = pymysql.connect(host='39.96.165.58',  
                            user='root',  
                            password='12345678',  
                            db='huamanxi',  
                            charset='utf8mb4',  
                            )
connection.ping()

# 在服务器上没办法运行离线计算程序，只能在自己的电脑上运行之后，把结果放到服务器上
rijian_dict = pickle.load(open("/usr/local/http_flume/http_flume/rijian.pk", "rb"))

def rijian(request):
    userId = request.GET.get("userId")
    res = rijian_dict.get(userId)
    if res:
        res = [int(i) for i in res]
    _json = {"rijian":res}
    return HttpResponse(json.dumps(_json), content_type="application/json")



def get_json(request):
    # 需要注意flume传输的json的格式
    j = [{
        "headers": {
            "userId": '233',
            "tags": "唐代-杜甫-七夕节"
        },
        "body": "filed1, filed2, filed3" 
    }]
    json_data = json.dumps(j)
    # 发送请求
    headers = {'Content-Type': 'application/json;charset=UTF-8'}
    res = requests.post('http://120.26.89.198:5555', data=json_data, headers=headers)
    print(res)

    return HttpResponse(json_data, content_type='application/json')


def online_server(request):
    # global model 模型的使用需要 global 一下
    

    # 需要传过来用户的 id
    userId = request.GET.get("userId") # str 类型
    #  userId = str(int(usrId) + 100000)
    
    redis_client = get_redis_connection("default")
    tags_list = redis_client.lrange(userId, 0, 10)

    # 记录要实时从 Mongodb 的 Word2Vec_sim表 进行搜索的古诗id
    search_w2v = []
    tags = []

    for _tags in tags_list:
        real_list = _tags.decode().split("-")
        if real_list[0] != "NoPoemID":
            search_w2v.append(int(real_list[0]))
        for i in real_list[1:]:
            tags.append(i)

   #  print(search_w2v)
   #  print(tags)
    tags = list(set(tags))
    try:
        tags = random.choices(tags, k=5)
    except:
        pass

    # 如果用户还没有在这个网站发生过行为，返回热门数据
    if len(tags) == 0:
        tags = ["唐代", "近代", "初中古诗"]
    
    unique_item = set()

    for search_id in search_w2v:
        query_dict = {
            "poemId":search_id,
        }
        found_json = Word2Vec_sim.find(query_dict).limit(1)
        for _json in found_json:
            unique_item = unique_item.union(_json["top100"][:10])

    # print("from w2v_sim ---->", unique_item)

    
    for tag in tags:
        query_dict = {
            "cateId":tag,
        }
        found_json = houxuanji.find(query_dict).limit(1)
        for _json in found_json:
            items = _json.get("incluted")
            items_len = _json.get("len")
            if items_len < 5:
                pass
            else:
                items = random.choices(items, k=5)
            unique_item = unique_item.union(items)


    # print('tags------->',tags)
    # TODO(liang):使用模型进行排序
    poemIds = []
    features = []

    
    for i in unique_item:
        # 通过mysql查询出古诗的朝代和标签
        connection.ping()
        cursor = connection.cursor()
        query = "select poemStar, poemDynasty, poemTagNames from poem where poemId=%s limit 1" % i
        cursor.execute(query)
        result = cursor.fetchone()
        poemIds.append(i)
        features.append(result)
    
    feature_matrix = []
    
    for tup in features:
        poemStar, poemDynasty, poemTagNames = tup
        try:
            poemTagNames = poemTagNames.split(',')
        except:
            poemTagNames = []
        # print(poemDynasty, poemTagNames)
        row = [poemStar]
        for j in column_name:
            if j == poemDynasty:
                row.append(1)
            elif j in poemTagNames:
                row.append(1)
            else:
                row.append(0)
        feature_matrix.append(row)

    feature_matrix = np.array(feature_matrix)
    

    train_new_feature = feature_encoder.apply(feature_matrix)
    train_new_feature = train_new_feature.reshape(-1, 50) # 50个基模型
    enc = OneHotEncoder()
    enc.fit(train_new_feature)
    train_new_feature2 = np.array(enc.transform(train_new_feature).toarray())

    store_list2 = []

    for i in train_new_feature2:
        d = dict()
        for index, j in enumerate(i):
            d[index] = j
        store_list2.append(d)

    v = DictVectorizer()
    feature_matrix = v.fit_transform(store_list2)
    predict_proba = fm.predict(feature_matrix)

    zip_list = zip(poemIds, predict_proba)
    sort_ids = sorted(zip_list, key=lambda tup:tup[1], reverse=True)
    sort_ids = [i[0] for i in sort_ids][:10]
    # print(sort_ids)

    # print(unique_item)
    
    # 负样本采集，并存到 Mongodb
    fucaiyang_dict = {}
    fucaiyang_dict['userId'] = userId
    fucaiyang_dict['fuyangben'] = sort_ids
    rs_fucaiyang.insert(fucaiyang_dict)

    
    laoliang_dict = {"rs":sort_ids}
    # 使用模型对 unique_item里面的

    return HttpResponse(json.dumps(laoliang_dict),\
        content_type='application/json')

def mostSim(request):
    poemId = request.GET.get("id")
    query_dict = {
        "poemId":int(poemId),
    }
    found_json = Word2Vec_sim.find(query_dict)
    store = []
    for _json in found_json:
        store.append(_json["top100"][:10])

    return HttpResponse(json.dumps(store[0]),\
        content_type='application/json')
