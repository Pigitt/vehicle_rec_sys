from user_process.user_to_postgresql import UserPostgresqlServer
from user_process.user_protrail import UserProtrail

"""
1. 将用户的曝光数据从redis落到Postgresql中。
2. 更新用户画像
"""

    
def process_users():
    """将用户数据落 Postgresql
    """
    # 用户Postgresql存储
    user_postgresql_server = UserPostgresqlServer()
    # 用户曝光数据落Postgresql
    user_postgresql_server.user_exposure_to_postgresql()

    # 更新用户画像
    user_protrail = UserProtrail()
    user_protrail.update_user_protrail_from_register_table()


if __name__ == "__main__":
    process_users() 

