import logging


class ManagerDescriptor(object):

    def __init__(self, manager):
        self.manager = manager

    def __get__(self, instance):
        if instance != None:
            raise AttributeError
        return self.manager


class Manager(object):

    def __init__(self, model_class):
        self.model_class = model_class

    def get_by_id(self, id):
        if self.model_class.exists(id):
            instance = self.model_class()
            instance._id = str(id)
            return instance

    def get_by_id_async(self, id, callback=None):
        obj = self.model_class()

        def on_response(res):
            if isinstance(res, Exception):
                logging.error(res)
                callback(None)
                return
            if any(res.values()):
                callback(obj.typecast_for_read(res))
            else:
                callback(None)

        obj.id = id
        key = obj.key()
        fields = obj.attributes.keys()
        obj.db.hmget(key, fields, on_response)

    def get_by_ids_async(self, ids, callback=None):
        obj = self.model_class()
        ids = list(ids)

        def on_response(res):
            if isinstance(res, Exception):
                logging.error(res)
                callback([])
                return
            if isinstance(res, list):
                ods = []
                for id, d in zip(ids, res):
                    o = self.model_class()
                    o.id = id
                    od = o.typecast_for_read(d).object_dict
                    if any(od.values()):
                        ods.append(od)
                callback(ods)
                return
            logging.error('wrong type of res: %s', res)
            callback([])

        pipeline = obj.db.pipeline()
        for id in ids:
            o = self.model_class()
            o.id = id
            key = o.key()
            fields = o.attributes.keys()
            pipeline.hmget(key, fields)
        pipeline.execute(callback=on_response)

    def get_sort_list_async(self, key, start=None, end=None, count=None,
            reverse=False, callback=None):
        obj = self.model_class()

        def on_response(res):
            if isinstance(res, Exception):
                logging.error(res)
                callback([])
                return
            callback(res)

        start = '-INF' if start is None else start
        end = '+INF' if end is None else end
        offset = None if count is None else 0

        # the difference between zrange and zrevrange is the sorted order.
        if reverse:
            obj.db.zrevrangebyscore(key, end, start,
                    offset, count, False, callback=on_response)
        else:
            obj.db.zrangebyscore(key, start, end,
                    offset, count, False, callback=on_response)

    def get_set_async(self, key, callback=None):
        obj = self.model_class()

        def on_response(res):
            if isinstance(res, Exception):
                logging.error(res)
                callback([])
                return
            callback(res)

        obj.db.smembers(key, callback=on_response)

    def get_sets_async(self, keys, callback=None):
        obj = self.model_class()

        def on_response(res):
            if isinstance(res, Exception):
                logging.error(res)
                callback([])
                return
            callback([list(r) for r in res])

        pipeline = obj.db.pipeline()
        for key in keys:
            pipeline.smembers(key)
        pipeline.execute(callback=on_response)

