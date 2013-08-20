import redis
from tornadoredis import Client as AsyncClient
from tornadoredis import ConnectionPool


class Client(object):

    def __init__(self, **kwargs):
        self.connection_settings = kwargs

    def redis(self):
        return redis.Redis(**self.connection_settings)

    def update(self, d):
        self.connection_settings.update(d)


def setup_connection(host, port, db=None, async=False):
    global connection, client, async_client
    if async:
        if async_client is None:
            pool = ConnectionPool(host=host, port=port,
                    max_connections=100, wait_for_available=True)
            async_client = AsyncClient(connection_pool=pool)
        connection = async_client
    else:
        kwargs = {
            'host': host,
            'port': port,
            'db': db,
        }
        if client:
            client.update(kwargs)
        else:
            client = Client(**kwargs)
        connection = client.redis()


def release_connection(callback=None):
    global async_client
    if is_async():
        async_client.disconnect(callback)


def get_client():
    global connection
    return connection


def is_async():
    global async_client
    return async_client is not None


client = Client()
async_client = None
connection = client.redis()

