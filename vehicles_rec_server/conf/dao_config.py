# 数据库相关的配置文件
user_info_db_name = "userinfo" # 用户数据相关的数据库
register_user_table_name = "register_user" # 注册用户数据表
user_likes_table_name = "user_likes" # 用户喜欢数据表
user_collections_table_name = "user_collections" # 用户收藏数据表
user_read_table_name = "user_read"   # 用户阅读数据表
exposure_table_name_prefix = "exposure" # 用户曝光数据表的前缀

# log数据，每天都会落一个盘，并由时间信息进行命名
loginfo_db_name = "loginfo" # log数据库
loginfo_table_name_prefix = "log" # log数据表的前缀

contest_loginfo_db_name = "contest_loginfo"
contest_loginfo_table_name_prefix = "contest_log"

# 默认配置
postgresql_username = "postgres"
postgresql_passwd = "123456"
postgresql_hostname = "localhost"
postgresql_port = "5432"

# MongoDB
mongo_hostname = "127.0.0.1"
mongo_port = 27017
# 原始数据
vehicle_db_name= "vehicledb"
vehicle_collection_name_prefix= "vehicles"
# 物料池db name 
material_db_name = "VehicleRecSys"

# 特征画像 集合名称
feature_protrail_collection_name = "FeatureProtrail"
redis_mongo_collection_name = "RedisProtrail"
user_protrail_collection_name = "UserProtrail"
contest_user_protrail_collection_name = "ContestUserProtrail"
contest_feature_protrail_collection_name = "ContestFeatureProtrail"

# Redis
redis_hostname = "127.0.0.1"
redis_port = 6379

reclist_redis_db_num = 0
static_vehicles_info_db_num = 1
dynamic_vehicles_info_db_num = 2
user_exposure_db_num = 3

make_dict = {
    '2510':'Acura',
    '2511':'Alfa-Romeo',
    '2512':'Aston-Martin',
    '2513':'Audi',
    '2514':'Bentley',
    '2515':'BMW',
    '2516':'Bugatti',
    '2517':'Buick',
    '2518': 'Cadillac',
    '2519':'Chevrolet',
    '2520':'Chrysler',
    '2521':'Dodge',
    '2522':'Ferrari',
    '2523':'Fiat',
    '2524':'Ford',
    '2525':'Genesis',
    '2526':'GMC',
    '2527':'Honda',
    '2528':'Hyundai',
    '2529':'Infiniti',
    '2530':'Jaguar',
    '2531':'Jeep',
    '2532':'Kia',
    '2533':'Lamborghini',
    '2534':'Land-Rover',
    '2535':'Lexus',
    '2536':'Lincoln',
    '2537':'Lotus',
    '2538':'Maserati',
    '2539':'Mazda',
    '2540':'McLaren',
    '2541':'Mercedes-Benz',
    '2542':'Mini',
    '2543':'Mitsubishi',
    '2544':'Nissan',
    '2545':'Polestar',
    '2546':'Porsche',
    '2547':'Ram',
    '2548':'Rolls-Royce',
    '2549':'Scion',
    '2550':'Smart',
    '2551':'Subaru',
    '2552':'Suzuki',
    '2553':'Tesla',
    '2554':'Toyota',
    '2555':'Volkswagen',
    '2556':'Volvo'
}