import os
import sys
sys.path.append("/git/Pigitt/vehicle_rec_sys/vehicles_rec_server")
from conf.dao_config import dynamic_vehicles_info_db_num

class Vehicle():
    def __init__(self):
        self.domain = 'https://www.caranddriver.com'

    def get_url(self):
        return self.domain