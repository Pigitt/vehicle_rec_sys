import sys
sys.path.append("/git/Pigitt/vehicle_rec_sys/vehicles_rec_server/")
from dao.postgresql_server import PostgresqlServer
from dao.entity.logitem import LogItem

import time

class LogController():
    def __init__(self) -> None:
        self.log_info_sql_session = PostgresqlServer().get_loginfo_session()

    def save_one_log(self,log):
        try:
            self.log_info_sql_session.add(log)
            self.log_info_sql_session.commit()
        except Exception as e:
            print(str(e))
            return False
        return True