import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import time
from sqlalchemy import Column, String, Integer,DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

from dao.postgresql_server import PostgresqlServer
from conf.dao_config import loginfo_db_name, loginfo_table_name_prefix
 
# 定义基类
Base = declarative_base()

# 定义映射关系
class LogItem(Base):
    """log日志数据
    """
    postfix = time.strftime("%Y_%m_%d", time.localtime())  

    # 每天都会创建一个新的表，带有时间信息
    __tablename__ = '{}_{}'.format(loginfo_table_name_prefix, postfix) 
    index = Column(Integer(), primary_key=True)
    userid = Column(String(30))
    vehicle_id = Column(String(100))
    
    # 阅读、点赞、收藏
    actiontype = Column(String(20)) 
    actiontime = Column(DateTime(timezone=True), server_default=func.now())

    def __init__(self):
        # 与数据库绑定映射关系
        engine = PostgresqlServer().get_loginfo_engine()
        Base.metadata.create_all(engine)
    
    def new(self,userid,vehicle_id,actiontype):
        self.userid = userid  
        self.vehicle_id = vehicle_id  
        self.actiontype =  actiontype