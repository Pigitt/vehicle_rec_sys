import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from dao.postgresql_server import PostgresqlServer
from dao.entity.user_read import UserRead
from dao.entity.logitem import LogItem
from controller.user_action_controller import UserAction


class LogProcess(object):
    def __init__(self):
        # 建立获取日志信息表的会话
        self.user_log_sql_session = PostgresqlServer().get_loginfo_session()

    def readlog_to_postgresql(self):
        # 拿到当天的用户阅读日志表
        logtables = self.user_log_sql_session.query(LogItem).filter_by(actiontype="read").all()

        # 遍历当天的用户日志表
        # 对于每条数据， 建立UserRead类， 通过UserAction保存到数据库里面去
        for log in logtables:
            #print(log.userid, log.vehicle_id, log.actiontype, log.actiontime)
            user_read = UserRead()
            user_read.new(log.userid, log.vehicle_id, log.actiontime)
            UserAction().save_one_action(user_read)

if __name__ == "__main__":
    LogProcess().readlog_to_postgresql()

        


