from ctypes import c_int, pointer
import zugbruecke as ctypes
import datetime
import redis
import sys
sys.path.insert(0, '/workspace/influx')
sys.path.insert(0, '/workspace/redis')
from time_trans import *
import time
from  client import InfluxDBClient
import math
import multiprocessing as mp
import logging
import logging.handlers
import datetime
import traceback
import numpy as np
import json

logging.basicConfig(level='WARNING')
fileHandler = logging.handlers.TimedRotatingFileHandler('./op-log-his4.txt', when='H', backupCount=24)
formatter = logging.Formatter("%(asctime)s-%(filename)s[line:%(lineno)d]-%(levelname)s:%(message)s")
fileHandler.setFormatter(formatter)
logger = logging.getLogger('op_to_influx')
logger.addHandler(fileHandler)

short_array_20000_type = ctypes.c_short*20000
int_array_20000_type = ctypes.c_int*20000
double_array_20000_type = ctypes.c_double*20000
ulong_array_20000_type = ctypes.c_ulong*20000

short_array_30000_type = ctypes.c_short*30000
int_array_30000_type = ctypes.c_int*30000
double_array_30000_type = ctypes.c_double*30000
ulong_array_30000_type = ctypes.c_ulong*30000


points_num = 0
point_names = []
point_names_str = []

ip = '172.17.51.169'
REDIS_EXPIRE_TIME = 7200
measurement = 'sis_data'

# r = redis.Redis(host=ip, port=port, db=0, decode_responses=True)
pool = redis.ConnectionPool(host=ip, port=6385, decode_responses=True)
r = redis.Redis(connection_pool=pool, health_check_interval=30, socket_keepalive=True, socket_connect_timeout = 10, socket_timeout=10, retry_on_timeout=True)
client = InfluxDBClient(host=ip, port=8091, username='root', password='root', database = 'test')


class openPlant():
    def __init__(self, dll_name:str):
        self.sess = ctypes.session()
        self.lib = self.sess.load_library(dll_name, 'windll')

        option = 1
        host = ctypes.c_char_p(str.encode('172.17.55.222'))
        port = 8200
        time_out = 120
        user = ctypes.c_char_p(str.encode('sis'))
        password = ctypes.c_char_p(str.encode('openplant'))
        buffer_path = ctypes.c_char_p(str.encode(''))
        buffer_size = 0
    
        op2_init = self.lib.op2_init
        op2_init.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_int, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int)
        op2_init.restype = ctypes.c_ulong

        self.op = op2_init(option, host, port, time_out, user, password, buffer_path, buffer_size)
        print(self.op)
        if self.op == None:
            print('连接失败:{}'.format(self.op))
        else:
            print('连接对象创建成功', self.op)
            logger.warning('连接对象创建成功')
        
        op2_new_group = self.lib.op2_new_group
        op2_new_group.restype = ctypes.c_ulong
        self.og = op2_new_group()

        op2_add_group_point = self.lib.op2_add_group_point
        op2_add_group_point.argtypes = (ctypes.c_ulong, ctypes.c_char_p)
        for point_name in point_names:
            op2_add_group_point(self.og, point_name)
        
        # op2_group_size = self.lib.op2_group_size
        # op2_group_size.argtypes = (ctypes.c_ulong,)
        # op2_group_size.restype = ctypes.c_int

        # size = op2_group_size(self.og)
        # print('size:',size)
        logger.warning('添加点名成功')

    
    def read_his(self, start_date_time, end_date_time):
        op2_encode_time = self.lib.op2_encode_time
        op2_encode_time.argtypes = (ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int)
        op2_encode_time.restype = ctypes.c_int


        op2_get_snap_byname = self.lib.op2_get_snap_byname
        op2_get_snap_byname.argtypes = (ctypes.c_ulong, ctypes.c_ulong, ctypes.c_int, short_array_20000_type, double_array_20000_type,
        int_array_20000_type)
        op2_get_snap_byname.restype = ctypes.c_int

        op = self.op
        og = self.og
        
        status = short_array_20000_type()
        read_value = double_array_20000_type()
        errors = int_array_20000_type()

        # for i in range(points_num):
        #     print(read_value[i])

        try:
            json_body = []
            start_time_stamp = time.mktime(start_date_time.timetuple())
            end_time_stamp = time.mktime(end_date_time.timetuple())
            duration = int(end_time_stamp - start_time_stamp)
            temp = start_time_stamp
            logger.warning(str(start_date_time))

            for i in range(duration):
                x = DT.utcfromtimestamp(float(temp)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                print(x, temp)
                body = {
                        "measurement": measurement,
                        "time": x,
                        "fields": {"Time": int(temp)}
                        }
                json_body.append(body)
                temp += 1
            client.write_points(json_body)
            print('insert timestamp successfully')
            
            sample_step = 10
            while int(start_time_stamp) % sample_step != 0:
                start_time_stamp += 1
            
            while start_time_stamp < end_time_stamp:
                temp = start_time_stamp + sample_step
                if temp > end_time_stamp:
                    temp = end_time_stamp
                start_time_stamp =  temp

                temp_date_time = datetime.datetime.fromtimestamp(temp)
                t = op2_encode_time(temp_date_time.year, temp_date_time.month, temp_date_time.day, temp_date_time.hour, temp_date_time.minute, temp_date_time.second)
                print(t)
                
                t1 = time.time()
                ret = op2_get_snap_byname(op, og, t, status, read_value, errors)
                if ret != 0:
                    logger.warning("read history data failed")
                else:
                    t2 = time.time()
                    print('read history data successfully, use:', t2-t1)
                    
                    json_body = []
                    for i in range(points_num):
                        # print(point_names_str[i], read_value[i])
                        ti = DT.utcfromtimestamp(float(temp)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                        body = {
                                "measurement": measurement,
                                "tags": {},
                                "time": ti,
                                "fields": {point_names_str[i]:read_value[i]}
                                }
                        json_body.append(body)
                    client.write_points(json_body)
                    t3 = time.time()
                    print('write history data successfully, use:', t3-t2)
        except Exception as e:
            logger.warning(e)
            traceback.print_exc()
        

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, exc_tb):
        op2_close = self.lib.op2_close
        op2_close.argtypes = (ctypes.c_ulong,)
        op2_close.restype = None

        op2_close(self.op)
        print("关闭连接成功")
        logger.warning("关闭连接成功")
        self.sess.terminate()


    
def get_tags():
    global points_num
    # with open('points_latest.txt', 'r') as f:
    with open('points_test1.txt', 'r') as f:
        for line in f.readlines():
             line = line.strip('\n')  #去掉列表中每一个元素的换行符
             point_names.append(ctypes.c_char_p(str.encode(line)))
             point_names_str.append(line)
             points_num += 1
        logger.warning('读取点名列表成功,数量为{}'.format(points_num))

# def get_tags():
#     global points_num
#     latest_points = r.get('origin_point_names').split(',')
#     for point in latest_points:
#         point_names.append(ctypes.c_char_p(str.encode(point)))
#         point_names_str.append(point)
#         points_num += 1
#     logger.warning('读取标签成功,数量为{}'.format(points_num))

def worker(start_time, end_time):
     with openPlant('opapi2.dll') as op:
        op.read_his(start_time, end_time)

if __name__ == '__main__':
    get_tags()

    dt_string = "19/12/2022 00:00:00"
    d_time = datetime.datetime.strptime(dt_string, "%d/%m/%Y %H:%M:%S")
    
    # cur_time = datetime.datetime.now()
    # end_time = cur_time + datetime.timedelta(days = -40)

    end_time = d_time
    start_time = end_time + datetime.timedelta(days = -46)
    # start_time = end_time + datetime.timedelta(hours=-1)
    print(start_time, end_time)

    start_time_stamp = time.mktime(start_time.timetuple())
    end_time_stamp = time.mktime(end_time.timetuple())

    pool = mp.Pool(6)
    t1 = time.time()

    while start_time_stamp < end_time_stamp:
        temp = start_time_stamp + 7200                    #每7200s开一个进程
        if temp > end_time_stamp:
            temp = end_time_stamp
        pool.apply_async(worker, (datetime.datetime.fromtimestamp(start_time_stamp), 
        datetime.datetime.fromtimestamp(temp)), error_callback=lambda x: print(x))
        start_time_stamp = temp
    pool.close()
    pool.join()

    t2 = time.time()
    logger.warning(str(t2-t1))
    # print("read his data finish! use:", t2-t1)