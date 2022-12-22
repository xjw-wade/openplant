import redis
import math
import time
import json

ip = '172.17.51.169'
port = 6385
REDIS_EXPIRE_TIME = 7200
r = redis.Redis(host=ip, port=port, db=0, decode_responses=True)

print(r.get('xjw') is None)
# print(r.get('origin_point_flag'))
# inter_variables_dict = json.loads(r.get('inter_variable_names'))
# for k, v in inter_variables_dict.items():
#     print(k, v)

# print(inter_variables_dict, len(inter_variables_dict))
# latest_points = r.get('origin_point_names').split(',')
# print(len(latest_points))
# latest_points.append('N4DCS.4NAADVOD02DI')
# r.set('origin_point_names', ','.join(latest_points))
# latest_points = r.get('origin_point_names').split(',')
# print(len(latest_points))

# latest_points = r.get('all_points').split(',')
# print('N4DCS.4NAADVOD02DI' in latest_points)
# # print(len(latest_points))
# point_names = ['W3.UNIT2.FDMH']

latest_points = r.get('origin_point_names').split(',')
# # inter_variables_dict = json.loads(r.get('inter_variable_names'))
# # print('W3.UNIT2.2PMULDCBAAO' in latest_points)
# # print(len(latest_points))

# point_names = []
# tmp = []
# with open('keys_test.txt', 'r') as f:
#     for line in f.readlines():
#             line = line.strip('\n')  #去掉列表中每一个元素的换行符
#             point_names.append(line)
#             if line not in latest_points:
#                 tmp.append(line)

# with open('keys_not_exit.txt', 'w') as f:
#     for point in tmp:
#         f.write(point)
#         f.write('\n')
# while True:
#     for i, point in enumerate(point_names):
#         latest = r.get('latest')
#         key = str(latest)+'@'+ point
#         key = key.strip()
#         print(key)
#         print(r.get(key), point, latest)
#     time.sleep(1)
# inter_variables_dict = json.loads(r.get('inter_variable_names'))
# for k, v in inter_variables_dict.items():
#     print(k,v)

# while True:
#     with r.pipeline() as p:
#         t1 = time.time()
#         ts = int(r.get('latest'))
#         for i in range(ts-40, ts+1):
#             keys = ['{}@{}'.format(i, point_name) for point_name in point_names[0:50]]
#             p.mget(*keys)
#         res = p.execute()
#         t2 = time.time()
#     print(len(res))
#     # count = 0
#     # for item in res:
#     #     if len(item):
#     #         count += 1
#     # print(count)
#     time.sleep(1)

