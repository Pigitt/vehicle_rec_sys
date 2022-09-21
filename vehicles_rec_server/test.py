import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import json, time
from flask_cors import * 
import snowflake.client  
from dao.postgresql_server import PostgresqlServer
from dao.entity.register_user import RegisterUser
from dao.entity.logitem import LogItem
from dao.entity.user_likes import UserLikes
from dao.entity.user_collections import UserCollections
from controller.user_action_controller import UserAction
from controller.log_controller import LogController
from recprocess.online import OnlineServer as RecsysServer
from flask import Flask, jsonify,request,redirect, url_for

app = Flask(__name__)

@app.route('/')
def hello():
    return 'Hello world!'

@app.route('/user/<uname>')
def get_userInfo(uname=None):
    if uname:
        return '%s\'s Informations' % uname
    else:
        return 'this is all informations of users'

@app.route('/test')
def test_url_for():
    print(url_for('get_userInfo', uname='zhangsan'))  # 输出

@app.route('/recsys/login', methods=["POST"])
def login():
    """用户登录
    """
    request_str = request.get_data()
    request_dict = json.loads(request_str)
    print(request_dict)

if __name__ == '__main__':
   app.run(debug=True, host='127.0.0.1', port=10086, threaded=True)