import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import redis 
from conf.dao_config import redis_hostname, redis_port, static_vehicles_info_db_num, dynamic_vehicles_info_db_num, reclist_redis_db_num
from conf.dao_config import user_exposure_db_num


class RedisServer(object):
    def __init__(self, _redis_hostname=redis_hostname, _port=redis_port, _static_vehicles_info_db_num=static_vehicles_info_db_num,
        _dynamic_vehicles_info_db_num = dynamic_vehicles_info_db_num, _reclist_redis_db_num=reclist_redis_db_num,
        _user_exposure_db_num=user_exposure_db_num):
        self.hostname = _redis_hostname
        self.port = _port
        self.static_vehicles_info_db_num = _static_vehicles_info_db_num
        self.dynamic_vehicles_info_db_num = _dynamic_vehicles_info_db_num
        self.reclist_redis_db_num = _reclist_redis_db_num
        self.user_exposure_db_num = _user_exposure_db_num

    def _redis_db(self, db_num=0):
        res_db = redis.StrictRedis(host=self.hostname, port=self.port, db=db_num, decode_responses=True)
        return res_db

    def get_static_vehicles_info_redis(self):
        """获取静态汽车信息数据库
        """
        return self._redis_db(self.static_vehicles_info_db_num)

    def get_dynamic_vehicles_info_redis(self):
        """获取动态汽车信息数据库
        """
        return self._redis_db(self.dynamic_vehicles_info_db_num)

    def get_reclist_redis(self):
        """用户推荐列表redis数据库
        """
        return self._redis_db(self.reclist_redis_db_num)

    def get_exposure_redis(self):
        """用户曝光列表redis数据库
        """
        return self._redis_db(self.user_exposure_db_num)