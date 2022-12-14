import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath('__file__')))))

from sqlalchemy.sql.functions import user
from conf.dao_config import make_dict
from dao.mongo_server import MongoServer
from dao.redis_server import RedisServer
from dao.postgresql_server import PostgresqlServer
from dao.entity.register_user import RegisterUser
from collections import defaultdict


# 这里需要从物料库中获取物料的信息，把物料按冷启用户分组
# 对不同组的冷启用户推荐对应物料50条 + 10条本地新闻 按热度值排序 并去重 

"""
第一版先不考虑用户位置和新闻位置的关系，因为目前新闻的数据量太少了
冷启动用户分组，最终的内容按照热度进行排序：
"""

class ColdStart(object):
    def __init__(self):
        self.feature_protrail_collection = MongoServer().get_feature_protrail_collection()
        self.reclist_redis = RedisServer().get_reclist_redis()
        self.register_user_sess = PostgresqlServer().get_register_user_session()
        self.make_dict = make_dict
        self.name2id_make_dict = {v: k for k, v in self.make_dict.items()}
        self._set_user_group()

    def _set_user_group(self):
        """将用户进行分组
        1. age < 23 && gender == female  
        2. age >= 23 && gender == female 
        3. age < 23 && gender == male 
        4. age >= 23 && gender == male  
        """
        self.user_group = {
            "1": ["bugatti","audi","ferrari","bentley","tesla","bmw"],
            "2": ["jeep","kia","hyundai","honda","toyota"],
            "3": ["lamborghini","land-rover","ferrari","aston-martin","cadillac"],
            "4": ["dodge","fiat","gmc","honda","hyundai","suzuki","volkswagen","toyota"]
        }
        self.group_to_make_id_dict = defaultdict(list)
        for k, make_list in self.user_group.items():
            for make in make_list:
                self.group_to_make_id_dict[k].append(self.name2id_make_dict[make])

    def _copy_cold_start_list_to_redis(self, user_id, group_id):
        """将确定分组后的用户的物料添加到redis中，并记录当前用户的所有新闻类别id
        """
        # 遍历当前分组的新闻类别
        for make_id in self.group_to_make_id_dict[group_id]:
            group_redis_key = "cold_start_group:{}:{}".format(group_id, make_id)
            user_redis_key = "cold_start_user:{}:{}".format(user_id, make_id)
            self.reclist_redis.zunionstore(user_redis_key, [group_redis_key])
        # 将用户的类别集合添加到redis中
        make_id_set_redis_key = "cold_start_user_make_set:{}".format(user_id)
        self.reclist_redis.sadd(make_id_set_redis_key, *self.group_to_make_id_dict[group_id])

    def user_vehicle_info_to_redis(self):
        """将每个用户涉及到的不同的新闻列表添加到redis中去
        """
        for user_info in self.register_user_sess.query(RegisterUser).all():
            # 年龄不正常的人，随便先给一个分组，后面还需要在前后端补充相关逻辑
            try:
                age = int(user_info.age)
            except:
                self._copy_cold_start_list_to_redis(user_info.userid, group_id="4")
                print("user_info.age: {}".format(user_info.age)) 

            if age < 23 and user_info.gender == "female":
                self._copy_cold_start_list_to_redis(user_info.userid, group_id="1")
            elif age >= 23 and user_info.gender == "female":
                self._copy_cold_start_list_to_redis(user_info.userid, group_id="2")
            elif age < 23 and user_info.gender == "male":
                self._copy_cold_start_list_to_redis(user_info.userid, group_id="3")
            elif age >= 23 and user_info.gender == "male":
                self._copy_cold_start_list_to_redis(user_info.userid, group_id="4")
            else:
                pass 

    # 当先系统使用的方法
    def generate_cold_user_strategy_templete_to_redis_v2(self):
        """冷启动用户模板，总共分成了四类人群
        每类人群都把每个类别的新闻单独存储
        """
        for k, item in self.user_group.items():
            for make in item:
                make_cnt = 0
                make_id = self.name2id_make_dict[make]
                # k 表示人群分组
                redis_key = "cold_start_group:{}:{}".format(str(k), make_id)
                for vehicle_info in self.feature_protrail_collection.find({"make": make}):
                    make_cnt += 1
                    self.reclist_redis.zadd(redis_key, {vehicle_info['make'] + '_' + vehicle_info['vehicle_id']: vehicle_info['hot_value']}, nx=True)
                print("类别 {} 的 新闻数量为 {}".format(make, make_cnt))


if __name__ == "__main__":
    # ColdStart().generate_cold_user_strategy_templete_to_redis_v2()
    # ColdStart().generate_cold_start_news_list_to_redis_for_register_user()
    cold_start = ColdStart()
    cold_start.generate_cold_user_strategy_templete_to_redis_v2()
    cold_start.user_vehicle_info_to_redis()

