from sqlalchemy import Column, String, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.sqltypes import BigInteger, DateTime
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from conf.dao_config import user_read_table_name
from dao.postgresql_server import PostgresqlServer
from sqlalchemy.sql import func

Base = declarative_base()

class UserRead(Base):
    """用户阅读新闻数据
    """
    __tablename__ = user_read_table_name
    index = Column(Integer(), primary_key=True, autoincrement=True)
    userid = Column(BigInteger())
    vehicle_id = Column(String(100))
    curtime = Column(DateTime(timezone=True), server_default=func.now())

    def __init__(self):
        # 与数据库绑定映射关系
        engine = PostgresqlServer().get_user_read_engine()
        Base.metadata.create_all(engine)
    
    def new(self, userid, vehicle_id, actiontime):
        self.userid = userid
        self.vehicle_id = vehicle_id
        self.curtime = str(actiontime)