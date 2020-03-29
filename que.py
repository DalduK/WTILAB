import datetime

import redis


class Que:

    def __init__(self):
        self.r = redis.StrictRedis(port=6381)

    def put(self,name,list):
        self.r.lpush(name, list)

    def get(self,name):
        return self.r.rpop(name)

    def isem(self,name):
        print(self.r.llen(name))

    def prt(self,name):
        l = self.r.lrange(name, 0, -1)
        for x in l:
            print(x)

    def fifo(self,name):
        l = self.r.lrange(name, 0, -1)
        for x in l:
            return self.get(name)

    def firstem(self,name):
        return self.r.lrange(name,0,-1)
    def flush(self,name):
        self.r.flushdb()