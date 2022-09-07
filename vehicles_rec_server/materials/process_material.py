from material_process.vehicles_protrait import VehiclesPortraitServer
from material_process.vehicles_to_redis import VehiclesRedisServer


def process_material():
    """物料处理函数
    """
    # 画像处理
    portrail_server = VehiclesPortraitServer()
    # 处理最新爬取汽车的画像，存入特征库
    portrail_server.update_new_items()
    # 更新汽车动态画像, 需要在redis数据库内容清空之前执行
    portrail_server.update_dynamic_feature_protrail()
    # 生成前端展示的汽车画像，并在mongodb中备份一份
    portrail_server.update_redis_mongo_protrail_data()


if __name__ == "__main__":
    process_material() 

