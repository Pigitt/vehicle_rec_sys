from sqlalchemy import Column, String, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.sqltypes import BigInteger
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from conf.dao_config import user_likes_table_name
from dao.postgresql_server import PostgresqlServer
from sqlalchemy.sql import func

Base = declarative_base()

class UserLikes(Base):
    """用户喜欢新闻数据
    """
    __tablename__ = user_likes_table_name 
    index = Column(Integer(), primary_key=True,autoincrement=True)
    userid = Column(BigInteger())
    username = Column(String(30))
    newid = Column(String(100))
    curtime =  Column(DateTime(timezone=True), server_default=func.now())

    def __init__(self):
        # 与数据库绑定映射关系
        engine = PostgresqlServer().get_user_like_engine()
        Base.metadata.create_all(engine)

    def new(self,userid,username,vehicle_id):
        self.userid = userid  
        self.username = username  
        self.vehicle_id =  vehicle_id  
        # self.curtime = curtime