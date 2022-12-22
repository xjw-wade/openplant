import json
from re import I

from util import Redis

host = '172.17.51.169'
port = 6385
redis_obj = Redis(host=host, port=port, db=0)

CHANNEL = {
    'realtime_channel': 'realtime',
    'predict_channel': 'predict',
    'warning_channel': 'warning',
    'selection_channel': 'selection',
    'optimize_channel': 'optimize',
    'train_channel': 'train',
    'autorun_channel': 'auto_run',
    'detection_channel': 'detection',
    'add_dataset_channel': 'add_dataset',
}

ROOMS = set({'1', '2', '3', '4'})

class RedisPub(object):
    def __init__(self, rs: Redis):
        self.rs = rs
        self.channels = CHANNEL

    def _supp_data(self, unit, data):
        self._check_data(data)

        data = {
            'unit': unit,
            'data': data
        }
        return json.dumps(data)

    def _check_data(self, data):
        # if not isinstance(data,dict):
        #     raise Exception("publish_x function data params type error,not a dict")
        try:
            json.dumps(data)
        except Exception:
        #    raise Exception("data is not a serializable object")
            raise Exception(f"data is not a serializable object,data is : {data}")

    def publish_realtime(self, unit, data):
        data = self._supp_data(unit, data)
        self.rs.publish(self.channels['realtime_channel'], data)

    def publish_predict(self, unit, data):
        data = self._supp_data(unit, data)
        self.rs.publish(self.channels['predict_channel'], data)

    def publish_warning(self, unit, data):
        data = self._supp_data(unit, data)
        self.rs.publish(self.channels['warning_channel'], data)

    def publish_data(self, channel, unit, data):
        data = self._supp_data(unit, data)
        self.rs.publish(channel, data)


# 发布端
redis_pub = RedisPub(redis_obj)


def subscribe_redis(redis_db):
    channels = list(CHANNEL.values())
    pubsub = redis_db.subscribe(channels[0])
    for channel in channels:
        pubsub.subscribe(channel)
    return pubsub


# 订阅端
redis_sub = subscribe_redis(redis_obj)
