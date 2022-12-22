import redis
import math
import time

ip = '172.17.86.17'
REDIS_EXPIRE_TIME = 7200

r = redis.Redis(host=ip, port=6379, db=0, decode_responses=True)

r.flushdb()
