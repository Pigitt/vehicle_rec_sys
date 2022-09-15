import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath('__file__')))))
from conf.dao_config import make_dict
from dao.mongo_server import MongoServer
from dao.redis_server import RedisServer
from datetime import datetime
from dateutil.relativedelta import relativedelta


# 这里需要从物料库中获取物料的信息，然后更新物料当天最新的热度信息
# 最终将计算好的物料热度，对物料进行排序

class HotRecall(object):
    def __init__(self):
        self.feature_protrail_collection = MongoServer().get_feature_protrail_collection()
        self.reclist_redis = RedisServer().get_reclist_redis()
        self.make_dict = make_dict

    def update_hot_value(self):
        """更新物料库中所有新闻的热度值
        """
        # 遍历物料池里面的所有文章
        for item in self.feature_protrail_collection.find():
            vehicle_id = item['vehicle_id']
            vehicle_make = item['make']
            vehicle_year = item['year']
            vehicle_likes_num = item['likes']
            vehicle_collections_num = item['collections']
            vehicle_read_num = item['read_num']
            vehicle_hot_value = item['hot_value']

            # 时间转换与计算时间差   前提要保证当前时间大于新闻创建时间，目前没有捕捉异常
            vehicle_year_standard = datetime.strptime(vehicle_year, "%Y-%m-%d %H:%M")
            cur_year_standard = datetime.now()
            time_year_diff = relativedelta(cur_year_standard - vehicle_year_standard).years
            # time_hour_diff = (cur_year_standard - vehicle_year_standard).seconds / 3600

            # 只要最近3天的内容
            if time_year_diff >= 6:
                continue

            # 72 表示的是3天，
            vehicle_hot_value = (vehicle_likes_num * 0.6 + vehicle_collections_num * 0.3 + vehicle_read_num * 0.1) * 10 / (1 + time_year_diff ) 

            # 更新物料池的文章hot_value
            item['hot_value'] = vehicle_hot_value
            self.feature_protrail_collection.update({'vehicle_id':vehicle_id}, item)

    def group_make_for_vehicle_list_to_redis(self, ):
        """将每个用户的推荐列表按照类别分开，方便后续打散策略的实现
        对于热门页来说，只需要提前将所有的类别新闻都分组聚合就行，后面单独取就可以
        注意：运行当前脚本的时候需要需要先更新新闻的热度值
        """
        # 1. 按照类别先将所有的新闻都分开存储
        for make_id, make_name in self.make_dict.items():
            redis_key = "hot_list_vehicle_make:{}".format(make_id)
            for item in self.feature_protrail_collection.find({"make": make_name}):
                self.reclist_redis.zadd(redis_key, {'{}_{}'.format(item['make'], item['vehicle_id']): item['hot_value']})
        

if __name__ == "__main__":
    hot_recall = HotRecall()
    # 更新物料的热度值
    hot_recall.update_hot_value()
    # 将新闻的热度模板添加到redis中
    hot_recall.group_make_for_vehicle_list_to_redis()


