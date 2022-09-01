import sys
import datetime
sys.path.append("/git/Pigitt/vehicle_rec_sys/vehicles_rec_server/")
import pymongo
from conf.dao_config import mongo_hostname, mongo_port
from conf.dao_config import vehicle_db_name, vehicle_collection_name_prefix
from conf.dao_config import material_db_name, feature_protrail_collection_name
from conf.dao_config import redis_mongo_collection_name
from conf.dao_config import user_protrail_collection_name
from conf.dao_config import contest_user_protrail_collection_name
from conf.dao_config import contest_feature_protrail_collection_name


class MongoServer(object):
    def __init__(self, 
        _mongo_hostname=mongo_hostname, 
        _mongo_port=mongo_port, 
        _vehicle_db_name=vehicle_db_name,
        _vehicle_collection_name_prefix=vehicle_collection_name_prefix, _material_db_name=material_db_name,
        _feature_protrail_collection_name=feature_protrail_collection_name, 
        _redis_mongo_collection_name=redis_mongo_collection_name,
        _user_protrail_collection_name=user_protrail_collection_name,
        _contest_user_protrail_collection_name=contest_user_protrail_collection_name,
        _contest_feature_protrail_collection_name=contest_feature_protrail_collection_name):
        self._hostname = _mongo_hostname
        self._port = _mongo_port
        self._vehicle_db_name = vehicle_db_name
        self._vehicle_collection_name_prefix = _vehicle_collection_name_prefix
        self._material_db_name = _material_db_name
        self._feature_protrail_collection_name = _feature_protrail_collection_name
        self._redis_mongo_collection_name = _redis_mongo_collection_name
        self._user_protrail_collection_name = user_protrail_collection_name
        self._contest_user_protrail_collection_name = contest_user_protrail_collection_name
        self._contest_feature_protrail_collection_name = contest_feature_protrail_collection_name

        self._mongo_client = self._mongodb()

    def _mongodb(self):
        """连接mongo数据库，并返回数据库
        """
        client = pymongo.MongoClient(self._hostname, self._port)
        return client

    def get_feature_protrail_collection(self):
        """特征画像集合
        """
        return self._mongo_client[self._material_db_name][self._feature_protrail_collection_name]

    def get_vehicles_collection(self):
        """原始新闻画像集合, 新闻爬取数据collection会以当天的时间命名
        """
        vehicle_collection_name = self._vehicle_collection_name_prefix + "_" + \
                            "".join(str(datetime.date.today()).split('-'))
        return self._mongo_client[self._vehicle_db_name][vehicle_collection_name]

    def get_redis_mongo_collection(self):
        """redis中的mongo备份数据集合
        """
        return self._mongo_client[self._material_db_name][self._redis_mongo_collection_name]
 
    def get_user_protrail_collection(self):
        """用户画像的数据集合
        """
        return self._mongo_client[self._material_db_name][self._user_protrail_collection_name] 

    def get_contest_user_potrial_collection(self):
        """获取竞赛数据中的用户画像集合
        """
        return self._mongo_client[self._material_db_name][self._contest_user_protrail_collection_name] 
    
    def get_contest_feature_potrial_collection(self):
        """获取竞赛数据中的文章画像集合
        """
        return self._mongo_client[self._material_db_name][self._contest_feature_protrail_collection_name] 