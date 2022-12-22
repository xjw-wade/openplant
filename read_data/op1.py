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
from iapws import iapws97
from iapws import IAPWS97
import numpy as np
import json
from functools import wraps
import signal
from redis_pub_sub import redis_pub, RedisPub

logging.basicConfig(level='WARNING')
fileHandler = logging.handlers.TimedRotatingFileHandler('./op-log-real.txt', when='H', backupCount=24)
formatter = logging.Formatter("%(asctime)s-%(filename)s[line:%(lineno)d]-%(levelname)s:%(message)s")
fileHandler.setFormatter(formatter)
logger = logging.getLogger('op_to_redis')
logger.addHandler(fileHandler)

short_array_20000_type = ctypes.c_short*20000
int_array_20000_type = ctypes.c_int*20000
double_array_20000_type = ctypes.c_double*20000


points_num = 0
point_names = []
point_names_str = []
CDATA = {}
MID = {}
inter_variables_dict = {}

ip = '172.17.51.169'
REDIS_EXPIRE_TIME = 7200
measurement = 'sis_data'

# r = redis.Redis(host=ip, port=port, db=0, decode_responses=True)
pool = redis.ConnectionPool(host=ip, port=6385, decode_responses=True)
r = redis.Redis(connection_pool=pool, health_check_interval=30, socket_keepalive=True, socket_connect_timeout = 10, socket_timeout=10, retry_on_timeout=True)
client = InfluxDBClient(host=ip, port=8091, username='root', password='root', database = 'test')
#------------------------------------------自定义函数------------------------------------------------------------------
def Sfunline(A,B):#折线函数
    C= len(B)
    n=int(C/2)
    m=int(C/2-1)
    a=np.zeros(m)
    b=np.zeros(m)
    X=np.zeros(n)
    Y=np.zeros(n)
    if A<=B[0] or A>=B[-2]:
        optimal = B[1] if A<B[0] else B[-1]
    else :
        for i in range (n):
            X[i]=B[i*2]
            Y[i]=B[i*2+1]
        for i in range( m ) :
            a[i] =(Y[i+1] -Y[i]) / (X[i+1] -X[i])
            b[i] =Y[i]-X[i]*a[i]
        for i in range ( m ):
            if A>=X[i] and A<X[i+1]:
                optimal = A*a[i]+b[i]
    return (optimal)

def PackRE(pointname,bite):#打包点解析，点名，第几位
    strPackname= bin(int(CDATA[pointname]))
    # print(Currentdata[pointname], pointname,strPackname)
    Packbite=len(strPackname)-1-bite
    if len(strPackname)<=bite:
        Packdata=0
    else :
        Packdata =strPackname[Packbite]
    return (Packdata)    

def get_his(pointname: str, timen: list):
    '''
    从redis中获取过去一段时间某个点的值
    ----------
    Args:
        pointname: 点名
        timen: 长度为1时timen[0]表示指定时间点(过去第timen[0]秒)；长度为2时表示[结束时间偏移(较小值)，开始时间偏移(较大值)]
    Returns:
        res: 若timen长度为2则返回list，为1则返回float
    '''
    latest_ts = r.get('latest')
    if len(timen) == 1:
        target_ts = int(latest_ts) - timen[0]
        # res = redis.hget(str(target_ts), pointname)
        res = redis.read('{}@{}'.format(target_ts, pointname))
        res = float(res) if res else None
        return (res)

    elif len(timen) == 2:
        start_ts = int(latest_ts) - timen[1]
        end_ts = int(latest_ts) - timen[0]
        key_list = ['{}@{}'.format(ts, pointname) for ts in range(end_ts - 1, start_ts - 1, -1)]
        p_res = r.mget(*key_list)
        # with redis.redis_client.pipeline() as p:
        #     for timestamp in range(end_ts - 1, start_ts - 1, -1):
        #         # p.hmget(max_key-i, *model_handler.use_cols)
        #         p.hmget(str(timestamp), pointname)
        #     p_res = p.execute()
        res = list()
        for pv in p_res:
            if pv is not None:
                res.append(float(pv))
        return (res)

def AVG(pointname, LDtime):
    '''
    求点名历史均值
    '''
    his_data = get_his(pointname, [0, LDtime])
    AVG = np.average(his_data) if len(his_data) > 0 else np.nan
    return (AVG)
#------------------------------------------自定义函数------------------------------------------------------------------

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

    
    def read_real(self):
        global points_num
        data_dict = {}

        op2_get_value_byname = self.lib.op2_get_value_byname
        op2_get_value_byname.argtypes = (ctypes.c_ulong, ctypes.c_ulong, int_array_20000_type, short_array_20000_type,double_array_20000_type,
        int_array_20000_type)
        op2_get_value_byname.restype = ctypes.c_int

        op = self.op
        og = self.og
        
        time_out_flag = False

        def time_out_handler():
            try:
                nonlocal time_out_flag
                time_out_flag = True
                logger.warning('Time out')
                p1 = mp.Process(target=write_2_redis)
                p2 = mp.Process(target=write_2_channel)
                p1.start()
                p2.start()
                print(str(int(time.time())), ':time_out') 
            except Exception as e:
                traceback.print_exc()
                logger.warning(str(e))

        def Timeout(seconds, callback=None):
            """Add a timeout parameter to a function and return it.
            :param seconds: float: 超时时间
            :param callback: func|None: 回调函数，如果为None则会直接抛异常
            :raises: HTTPException if time limit is reached
            """
            def decorator(function):
                def handler(signum, frame):
                    """超时处理函数"""
                    if callback is None:
                        raise Exception("Request timeout")
                    else:
                        # 超时回调函数
                        callback()

                @wraps(function)
                def wrapper(*args, **kwargs):
                    nonlocal time_out_flag
                    time_out_flag = False
                    # SIGALRM: 时钟中断(闹钟)
                    old = signal.signal(signal.SIGALRM, handler)
                    # ITIMER_REAL: 实时递减间隔计时器，并在到期时发送 SIGALRM 。
                    signal.setitimer(signal.ITIMER_REAL, seconds)
                    try:
                        return function(*args, **kwargs)
                    finally:
                        # 如果没有下面这两行，当function异常导致被捕获后，
                        # 还可能会触发超时异常。
                        # seconds=0: 意为清空计时器
                        signal.setitimer(signal.ITIMER_REAL, 0)
                        # 还原时钟中断处理
                        signal.signal(signal.SIGALRM, old)

                return wrapper
            return decorator

        current_time = int(time.time())
        while True:
            try:
                previous_time = current_time
                current_time = int(time.time())
                if current_time - previous_time >= 1:
                    origin_point_flag = r.get('origin_point_flag')
                    inter_variable_flag = r.get('inter_variable_flag')
                    if origin_point_flag == 'True':
                        r.set('origin_point_flag', 'False')
                        latest_points = r.get('origin_point_names').split(',')
                        difference = list(set(latest_points).difference(set(point_names_str)))

                        op2_free_group = self.lib.op2_free_group
                        op2_free_group.argtypes = (ctypes.c_ulong,)

                        op2_free_group(self.og)
                        logger.warning('点组释放成功')

                        op2_new_group = self.lib.op2_new_group
                        op2_new_group.restype = ctypes.c_ulong
                        self.og = op2_new_group()

                        op2_add_group_point = self.lib.op2_add_group_point
                        op2_add_group_point.argtypes = (ctypes.c_ulong, ctypes.c_char_p)
                        points_num = 0
                        point_names_str.clear()
                        point_names.clear()
                        
                        for point in latest_points:
                            point_names.append(ctypes.c_char_p(str.encode(point)))
                            point_names_str.append(point)
                            op2_add_group_point(self.og, ctypes.c_char_p(str.encode(point)))
                            points_num += 1
                        
                        print('Changing detected!', len(point_names_str))
                    if inter_variable_flag == 'True':
                        global inter_variables_dict
                        r.set('inter_variable_flag', 'False')
                        inter_variables_dict = json.loads(r.get('inter_variable_names'))
                        print('Changing detected!', len(inter_variables_dict))
                    t = int_array_20000_type()
                    status = short_array_20000_type()
                    read_value = double_array_20000_type()
                    errors = int_array_20000_type()

                    read_op = Timeout(0.5, callback=time_out_handler)(op2_get_value_byname)
                    
                    t1 = time.time()
                    # ret = op2_get_value_byname(op, og, t, status, read_value, errors)
                    ret = read_op(op, og, t, status, read_value, errors)

                    if ret != 0:
                        logger.warning("read real time data failed")
                    else:
                        for i in range(points_num):
                            CDATA[point_names_str[i]] = read_value[i]
                        t2 = time.time()
                        print('read real time data successfully, use:', t2-t1)

                        for k in inter_variables_dict.keys():
                            latest = r.get('latest')
                            key = str(latest) + '@' + k
                            res = r.get(key)
                            if res is not None:
                                CDATA[k] = float(res)
                        
                        CDATA['latest'] = str(int(time.time()))

                        p1 = mp.Process(target=write_2_redis)
                        p2 = mp.Process(target=write_2_channel)
                        p1.start()
                        p2.start()
                        print(str(int(time.time())), ':main')

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


def write_2_channel():
    '''
    将数据写入redis channel

    '''
    try:
        t1 = time.time()
        with r.pipeline() as p:
            redis_pub.publish_realtime('1', CDATA)
            # redis_pub.publish_realtime('2', CDATA)
            # redis_pub.publish_realtime('3', CDATA)
            # redis_pub.publish_realtime('4', CDATA)
            p.execute()
        t2 = time.time()
        print(f'write to channel use:{t2-t1}')
    except Exception as e:
        logger.warning(e)

def write_2_redis():
    '''
    将数据写入redis
    '''
    try:
        json_body = []
        time_stamp = int(time.time())
        x = DT.utcfromtimestamp(float(time_stamp)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        for point_name, code_str in inter_variables_dict.items():
            if code_str and code_str != '' and code_str[0] != '[':
                try:
                    res = eval(code_str)
                    MID[point_name] = res
                    CDATA[point_name] = res
                    body = {
                            "measurement": measurement,
                            "time": x,
                            "fields": {point_name:res}
                            }
                    json_body.append(body)
                except KeyError as e:
                    logger.warning('point: {} "{}" exec error! {} not found'.format(point_name, code_str, e))
                except Exception as e:
                    logger.warning('point: {} "{}" exec error! e: {}'.format(point_name, code_str, e))
        temp_dict ={}
        t1 = time.time()
        temp_dict['latest'] = str(time_stamp)
        for k, v in CDATA.items():
            key = str(time_stamp) + '@' + k
            temp_dict[key] = v
        r.mset(temp_dict)
        t2 = time.time()
        print(f'write to redis use:{t2-t1},time_stamp:{time_stamp}')

        with r.pipeline() as p:
            for k in CDATA.keys():
                key = str(time_stamp) + '@' + k
                p.expire(key, REDIS_EXPIRE_TIME)
            p.execute()
        print(f'redis expire success,time_stamp:{time_stamp}')
        client.write_points(json_body)
    except Exception as e:
        logger.warning(str(e))
    
# def get_tags():
#     global points_num
#     # with open('points_list.txt', 'r') as f:
#     with open('points_latest.txt', 'r') as f:
#         for line in f.readlines():
#              line = line.strip('\n')  #去掉列表中每一个元素的换行符
#              point_names.append(ctypes.c_char_p(str.encode(line)))
#              point_names_str.append(line)
#              points_num += 1
#         logger.warning('读取点名列表成功,数量为{}'.format(points_num))

def get_tags():
    global points_num, inter_variables_dict
    latest_points = r.get('origin_point_names').split(',')
    inter_variables_dict = json.loads(r.get('inter_variable_names'))
    r.set('origin_point_flag', 'False')
    r.set('inter_variable_flag', 'False')
    for point in latest_points:
        point_names.append(ctypes.c_char_p(str.encode(point)))
        point_names_str.append(point)
        points_num += 1
    logger.warning('读取标签成功,数量为{}'.format(points_num))


if __name__ == '__main__':
    get_tags()
    with openPlant('opapi2.dll') as op:
        op.read_real()