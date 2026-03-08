import json

class RedisCRUD:
    def __init__(self, manager):
        self.manager = manager

    def create(self, key, value, ttl=3600):
        client = self.manager.get_redis_client()
        data = json.dumps(value) if isinstance(value, dict) else value
        return client.setex(key, ttl, data)

    def read(self, key):
        client = self.manager.get_redis_client()
        res = client.get(key)
        try: return json.loads(res) 
        except: return res

    def delete(self, key, table_ignored=None): 
        client = self.manager.get_redis_client()
        return client.delete(key)