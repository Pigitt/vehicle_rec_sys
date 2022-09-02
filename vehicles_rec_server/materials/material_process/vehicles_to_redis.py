import sys
sys.path.append("/git/Pigitt/vehicle_rec_sys/vehicles_rec_server/")
from dao.mongo_server import MongoServer
from dao.redis_server import RedisServer


class VehiclesRedisServer(object):
    def __init__(self):
        self.rec_list_redis = RedisServer().get_reclist_redis()
        self.static_vehicles_info_redis = RedisServer().get_static_vehicles_info_redis()
        self.dynamic_vehicles_info_redis = RedisServer().get_dynamic_vehicles_info_redis()

        self.redis_mongo_collection = MongoServer().get_redis_mongo_collection()

        # 删除前一天redis中的内容
        self._flush_redis_db()

    def _flush_redis_db(self):
        """每天都需要删除redis中的内容，更新当天新的内容上去
        """
        try:
            self.rec_list_redis.flushall()
        except Exception:
            print("flush redis fail ... ")

    def _get_vehicles_id_list(self):
        """获取物料库中所有的新闻id
        """
        # 获取所有数据的news_id,
        # 暴力获取，直接遍历整个数据库，得到所有新闻的id
        # TODO 应该存在优化方法可以通过查询的方式只返回new_id字段
        vehicles_id_list = []
        for item in self.redis_mongo_collection.find():
            vehicles_id_list.append(item["vehicle_id"])
        return vehicles_id_list

    def _set_info_to_redis(self, redisdb, content):
        """将content添加到指定redis
        """
        try: 
            redisdb.set(*content)
        except Exception:
            print("set content fail".format(content))

    def vehicles_detail_to_redis(self):
        """将需要展示的画像内容存储到redis
        静态不变的特征存到static_news_info_db_num
        动态会发生改变的特征存到dynamic_news_info_db_num
        """ 
        vehicles_id_list = self._get_vehicles_id_list()

        for vehicle_id in vehicles_id_list:
            vehicles_item_dict = self.redis_mongo_collection.find_one({"vehicle_id": vehicle_id}) # 返回的是一个列表里面套了一个字典  
            vehicles_item_dict.pop("_id")

            # 分离动态属性和静态属性
            static_vehicles_info_dict = dict()
            # static_vehicles_info_dict['vehicle_id'] = vehicles_item_dict['vehicle_id']
            static_vehicles_info_dict['name'] = vehicles_item_dict['name']
            static_vehicles_info_dict['year'] = vehicles_item_dict['year']
            static_vehicles_info_dict['make'] = vehicles_item_dict['make']
            static_vehicles_info_dict['model'] = vehicles_item_dict['model']
            static_vehicles_info_dict['price'] = vehicles_item_dict['price']
            static_vehicles_info_dict['kbb_url'] = vehicles_item_dict['kbb_url']
            static_vehicles_info_dict['rate'] = vehicles_item_dict['rate']
            static_vehicles_info_dict['cnd_url'] = vehicles_item_dict['cnd_url']
            static_vehicles_info_dict['specs'] = vehicles_item_dict['specs']
            static_content_tuple = "static_vehicles_info_dict:" + str(vehicle_id), str(static_vehicles_info_dict)
            # r.set(*static_content_tuple)
            self._set_info_to_redis(self.static_vehicles_info_dict, static_content_tuple)

            dynamic_vehicles_info_dict = dict()
            # dynamic_vehicles_info_dict['vehicle_id'] = vehicles_item_dict['vehicle_id']
            dynamic_vehicles_info_dict['likes'] = vehicles_item_dict['likes']
            dynamic_vehicles_info_dict['collections'] = vehicles_item_dict['collections']
            dynamic_vehicles_info_dict['read_num'] = vehicles_item_dict['read_num']
            dynamic_content_tuple = "dynamic_vehicles_detail:" + str(vehicle_id), str(dynamic_vehicles_info_dict)
            self._set_info_to_redis(self.dynamic_vehicles_info_redis, dynamic_content_tuple)

        print("news detail info are saved in redis db.")


if __name__ == "__main__":
    # 每次创建这个对象的时候都会把数据库中之前的内容删除
    news_redis_server = VehiclesRedisServer()
    # 将最新的前端展示的画像传到redis
    news_redis_server.vehicles_detail_to_redis()

    