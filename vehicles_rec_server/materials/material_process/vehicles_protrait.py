# -*- coding: utf-8 -*-
from re import S
import json
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
# from material_process.utils import get_key_words
from dao.mongo_server import MongoServer
from dao.redis_server import RedisServer

class VehiclesPortraitServer:
    def __init__(self):
        """初始化相关参数
        """
        self.mongo_server = MongoServer()   
        self.vehicles_collection = self.mongo_server.get_vehicles_collection()
        self.material_collection = self.mongo_server.get_feature_protrail_collection()
        self.redis_mongo_collection = self.mongo_server.get_redis_mongo_collection()
        self.vehicles_dynamic_feature_redis = RedisServer().get_dynamic_vehicles_info_redis()

    def _find_by_name(self, collection, name):
        """从数据库中查找是否有相同名称的车辆
        数据库存在当前标题的数据返回True, 反之返回Flase
        """
        # find方法，返回的是一个迭代器
        find_res = collection.find({"name": name})
        if len(list(find_res)) != 0:
            return True
        return False

    def _generate_feature_protrail_item(self, item):
        """生成特征画像数据，返回一个新的字典
        """
        vehicle_item = dict()
        vehicle_item['vehicle_id'] = item['vehicle_id']
        vehicle_item['name'] = item['name']
        vehicle_item['year'] = item['year']
        vehicle_item['make'] = item['make']
        vehicle_item['model'] = item['model']
        vehicle_item['price'] = item['price']
        vehicle_item['kbb_url'] = item['kbb_url']
        vehicle_item['rate'] = item['rate']
        vehicle_item['cnd_url'] = item['cnd_url']
        vehicle_item['specs'] = item['specs']
        vehicle_item['likes'] = 0
        vehicle_item['collections'] = 0
        vehicle_item['read_num'] = 0
        vehicle_item['hot_value'] = 1000 # 初始化一个比较大的热度值，会随着时间进行衰减
        
        return vehicle_item

    def update_vehicle_items(self):
        """将今天爬取的数据构造画像存入画像数据库中
        """
        # 遍历今天爬取的所有数据
        for item in self.vehicles_collection.find():
            # 根据标题进行去重
            if self._find_by_name(self.material_collection, item["name"]):
                continue

            vehicles_item = self._generate_feature_protrail_item(item)

            # 插入物料池
            self.material_collection.insert_one(vehicles_item)
        
        print("run update_new_items success.")

    def update_redis_mongo_protrail_data(self):
        """每天都需要将新闻详情更新到redis中，并且将前一天的redis数据删掉
        """
        # 每天先删除前一天的redis展示数据，然后再重新写入
        self.redis_mongo_collection.drop()
        print("delete RedisProtrail ...")
        # 遍历特征库
        for item in self.material_collection.find():
            vehicle_item = dict()
            vehicle_item['vehicle_id'] = item['vehicle_id']
            vehicle_item['name'] = item['name']
            vehicle_item['year'] = item['year']
            vehicle_item['make'] = item['make']
            vehicle_item['model'] = item['model']
            vehicle_item['price'] = item['price']
            vehicle_item['kbb_url'] = item['kbb_url']
            vehicle_item['rate'] = item['rate']
            vehicle_item['cnd_url'] = item['cnd_url']
            vehicle_item['specs'] = item['specs']
            vehicle_item['likes'] = 0
            vehicle_item['collections'] = 0
            vehicle_item['read_num'] = 0

            self.redis_mongo_collection.insert_one(vehicle_item)
        print("run update_redis_mongo_protrail_data success.")

    def update_dynamic_feature_protrail(self):
        """用redis的动态画像更新mongodb的画像
        """
        # 遍历redis的动态画像，将mongodb中对应的动态画像更新        
        vehicles_list = self.vehicles_dynamic_feature_redis.keys()
        for vehicles_key in vehicles_list:
            vehicles_dynamic_info_str = self.vehicles_dynamic_feature_redis.get(vehicles_key)
            vehicles_dynamic_info_str = vehicles_dynamic_info_str.replace("'", '"' ) # 将单引号都替换成双引号
            vehicles_dynamic_info_dict = json.loads(vehicles_dynamic_info_str)
            
            # 查询mongodb中对应的数据，并将对应的画像进行修改
            vehicle_id = vehicles_key.split(":")[1]
            mongo_info = self.material_collection.find_one({"vehicle_id": vehicle_id})
            vehicle_mongo_info = mongo_info.copy()
            vehicle_mongo_info['likes'] = vehicles_dynamic_info_dict["likes"]
            vehicle_mongo_info['collections'] = vehicles_dynamic_info_dict["collections"]
            vehicle_mongo_info['read_num'] = vehicles_dynamic_info_dict["read_num"]

            self.material_collection.replace_one(mongo_info, vehicle_mongo_info, upsert=True) # upsert为True的话，没有就插入
        print("update_dynamic_feature_protrail success.")



if __name__ == "__main__":
    # TODO 需要放到 其他逻辑中，将物料这块的逻辑打通
    vehicles_protrait = VehiclesPortraitServer()
    # news_protrait.update_new_items()
    vehicles_protrait.update_redis_mongo_protrail_data()
    # news_protrait.update_dynamic_feature_protrail()
