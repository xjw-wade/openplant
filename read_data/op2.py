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
fileHandler = logging.handlers.TimedRotatingFileHandler('./op-log-his.txt', when='H', backupCount=24)
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
            print('????????????:{}'.format(self.op))
        else:
            print('????????????????????????', self.op)
            logger.warning('????????????????????????')
        
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
        logger.warning('??????????????????')

    
    def read_his(self, start_date_time, end_date_time, interval=1):
        op2_encode_time = self.lib.op2_encode_time
        op2_encode_time.argtypes = (ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int)
        op2_encode_time.restype = ctypes.c_int

        op2_get_history_byname = self.lib.op2_get_history_byname
        op2_get_history_byname.argtypes = (ctypes.c_ulong, ctypes.c_ulong, int_array_20000_type, ctypes.c_int, ctypes.c_int, ctypes.c_int, ulong_array_20000_type, int_array_20000_type)
        op2_get_history_byname.restype = ctypes.c_int

        op = self.op
        og = self.og
        valtype = int_array_20000_type()
        
        beg_time = op2_encode_time(start_date_time.year, start_date_time.month, start_date_time.day, start_date_time.hour, start_date_time.minute, start_date_time.second)
        end_time = op2_encode_time(end_date_time.year, end_date_time.month, end_date_time.day, end_date_time.hour, end_date_time.minute, end_date_time.second)
        interval = interval
        result = ulong_array_20000_type()
        errors = int_array_20000_type()

        try:
            json_body = []
            start_time_stamp = time.mktime(start_date_time.timetuple())
            end_time_stamp = time.mktime(end_date_time.timetuple())
            duration = int(end_time_stamp - start_time_stamp)
            temp = start_time_stamp
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
            latest_points = r.get('origin_point_names').split(',')
            difference = list(set(latest_points).difference(set(point_names_str)))
            # if len(latest_points) != len(point_names_str):
            if difference:
                op2_add_group_point = self.lib.op2_add_group_point
                op2_add_group_point.argtypes = (ctypes.c_ulong, ctypes.c_char_p)

                op2_free_group = self.lib.op2_free_group
                op2_free_group.argtypes = (ctypes.c_ulong,)

                op2_free_group(self.og)
                logger.warning('??????????????????')

                op2_new_group = self.lib.op2_new_group
                op2_new_group.restype = ctypes.c_ulong
                self.og = op2_new_group()


                global points_num
                points_num = 0
                point_names_str.clear()
                point_names.clear()
                for point in latest_points:
                    op2_add_group_point(self.og, ctypes.c_char_p(str.encode(point)))
                    point_names.append(ctypes.c_char_p(str.encode(point)))
                    point_names_str.append(point)
                    points_num += 1

                print('Changing detected!', len(point_names_str))
                # difference = list(set(latest_points).difference(set(sname_str)))
                # for point in difference:
                #     sname_str.append(point)
                #     sname[key_num] = ctypes.c_char_p(str.encode(point))
                #     key_num += 1
            t1 = time.time()
            ret = op2_get_history_byname(op, og, valtype, beg_time, end_time, interval, result, errors)
            if ret != 0:
                logger.warning("read history data failed")
            else:
                t2 = time.time()
                print('read history data successfully, use:', t2-t1)

                op2_num_rows = self.lib.op2_num_rows
                op2_num_rows.argtypes = (ctypes.c_ulong,)
                op2_num_rows.restype = ctypes.c_int

                op2_fetch_timed_value = self.lib.op2_fetch_timed_value
                op2_fetch_timed_value.argtypes = (ctypes.c_ulong, ctypes.POINTER(ctypes.c_int), ctypes.POINTER(ctypes.c_short), ctypes.POINTER(ctypes.c_double))
                op2_fetch_timed_value.restype = ctypes.c_int
                
                for i in range(points_num):
                    n = op2_num_rows(result[i])
                    print(n)
                    tm_tag = ctypes.POINTER(ctypes.c_int)(ctypes.c_int())
                    stat_ = ctypes.POINTER(ctypes.c_short)(ctypes.c_short())
                    value_ = ctypes.POINTER(ctypes.c_double)(ctypes.c_double())
                    
                    t3 = time.time()
                    json_body = []
                    for j in range(n):
                        ret = op2_fetch_timed_value(result[i],tm_tag, stat_, value_)
                        # print(value_[0], tm_tag[0])
                        t = DT.utcfromtimestamp(float(tm_tag[0])).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                        body = {
                                "measurement": measurement,
                                "tags": {},
                                "time": t,
                                "fields": {point_names_str[i]:value_[0]}
                                }
                        json_body.append(body)
                    client.write_points(json_body)
                    t4 = time.time()
                    print('write history data successfully, use:', t4-t3)
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
        print("??????????????????")
        logger.warning("??????????????????")
        self.sess.terminate()


    
# def get_tags():
#     global points_num
#     # with open('points_list.txt', 'r') as f:
#     with open('points_latest.txt', 'r') as f:
#         for line in f.readlines():
#              line = line.strip('\n')  #??????????????????????????????????????????
#              point_names.append(ctypes.c_char_p(str.encode(line)))
#              point_names_str.append(line)
#              points_num += 1
#         logger.warning('????????????????????????,?????????{}'.format(points_num))

def get_tags():
    global points_num
    latest_points = r.get('origin_point_names').split(',')
    for point in latest_points:
        point_names.append(ctypes.c_char_p(str.encode(point)))
        point_names_str.append(point)
        points_num += 1
    logger.warning('??????????????????,?????????{}'.format(points_num))

def worker(time_interval, interval):
        with openPlant('opapi2.dll') as op:
            end_time = datetime.datetime.now()
            # start_time = end_time + datetime.timedelta(minutes=-1)
            start_time = end_time + datetime.timedelta(hours=-1)
            print(start_time, end_time)
            op.read_his(start_time, end_time, interval)

if __name__ == '__main__':
    get_tags()
    time_interval = 3600
    interval = 1
    while True:
        p = mp.Process(target=worker, args=(time_interval, interval))
        p.start()
        time.sleep(time_interval)
