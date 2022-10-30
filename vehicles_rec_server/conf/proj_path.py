import os

# home_path = os.environ['HOME']
proj_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath('__file__'))))
stop_words_path = proj_path + "conf/stop_words.txt"
bad_case_vehicle_log_path = proj_path + "logs/vehicle_bad_cases.log"

root_data_path = "D:/git/Pigitt/vehicle_rec_sys/off_data"
log_data_path = root_data_path + "train_data_30w.txt"
doc_info_path = root_data_path + "doc_info.txt"
user_info_path = root_data_path + "user_info.txt" 