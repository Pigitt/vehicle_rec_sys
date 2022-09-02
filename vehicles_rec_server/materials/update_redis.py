from material_process.vehicles_protrait import VehiclesPortraitServer
from material_process.vehicles_to_redis import VehiclesRedisServer


def update():
    """物料处理函数
    """
    # 新闻数据写入redis, 注意这里处理redis数据的时候是会将前一天的数据全部清空
    vehicles_redis_server = VehiclesRedisServer()
    # 将最新的前端展示的画像传到redis
    vehicles_redis_server.vehicles_detail_to_redis()

if __name__ == "__main__":
    update()

