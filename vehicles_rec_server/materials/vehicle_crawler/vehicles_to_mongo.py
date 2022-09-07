import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath('__file__')))))
import time
import pymongo
from pymongo.errors import DuplicateKeyError
from conf.dao_config import mongo_hostname, mongo_port
from conf.dao_config import vehicle_db_name, vehicle_collection_name_prefix
from vehicle import Vehicle




# item持久化
class VehiclePipeline:
    """数据持久化：将数据存放到mongodb中
    """
    def __init__(self, 
        _mongo_hostname=mongo_hostname, 
        _mongo_port=mongo_port, 
        _vehicle_db_name=vehicle_db_name,
        _vehicle_collection_name_prefix=vehicle_collection_name_prefix):
        self.host = _mongo_hostname
        self.port = _mongo_port
        self.db_name = _vehicle_db_name
        self.collection_name = _vehicle_collection_name_prefix+"_"+ time.strftime("%Y%m%d", time.localtime())
        self.vehicle=Vehicle().get_data()
        self.client = pymongo.MongoClient(self.host, self.port)
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]

    def process_item(self):
        if self.vehicle:
            for item in self.vehicle:
                self.collection.insert_one(dict(item))
        else:
            pass
        # return self.vehicle

if __name__ == "__main__":
    url=VehiclePipeline().process_item()