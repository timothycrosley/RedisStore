"""
    name:
        RedisStore.py
    Description:
        Offers a simpler more robust way to store and retrieve redis data from within python.
"""

import cPickle as pickle
import os
import threading
import time
import types

import redis

from config import cfgDict
from IteratorUtils import SortedSet
from StringUtils import listReplace

REDIS_NUM_DBS = 1

class Redis(redis.Redis):
    def reconnect(self):
        ''' Reconnect to redis '''
        self.connection.disconnect()
        self.connection.connect(self) # select is done here.

    def flushKeys(self, keyPattern):
        keys = self.keys(listReplace(keyPattern, KEY_REPLACEMENTS[0], KEY_REPLACEMENTS[1]))
        if len(keys) > 0:
            self.delete(*keys)
        return len(keys)
redis.Redis = Redis


LONG_CACHE_TIME = 60 * 60 * 24 * 7
SHORT_CACHE_TIME = 60 * 60 * 8
VERY_SHORT_CACHE_TIME = 60 * 5
FOREVER_CACHE_TIME = 0 # If you use this, make sure you have a way to clear them yourself!

def cache(func, rstoreType, cacheTime=LONG_CACHE_TIME, dontCreateIfReturnedIn=[]):
    def _cache(* args, **kw):
        startPos = 1
        if func.func_code.co_varnames and func.func_code.co_varnames[0] != 'self':
            startPos = 0
        keyargs = ",".join([str(arg) for arg in args[startPos:]] + \
                [str(kw.get(func.func_code.co_varnames[i], getDefault(i, func))) for i in range(len(args), func.func_code.co_argcount)])
        key = "%s.%s(%s)" % (func.__module__, func.func_name, keyargs)

        rstore = rstoreType(key, create=False)
        try:
            storeValue = rstore.value()
            if storeValue != None:
                if cacheTime:
                    rstore.expire(cacheTime)
                return storeValue
        except:
            pass

        res = func(*args, **kw)
        if res != None and res not in dontCreateIfReturnedIn:
            rstore.resetValue(res)
            if cacheTime:
                rstore.expire(cacheTime)
        return res

    def getDefault(i, func):
        if not func.func_defaults:
            return ""
        index = i - (func.func_code.co_argcount - len(func.func_defaults))
        if index >= 0:
            return func.func_defaults[index]
        return ""

    return _cache

def cacheString(func):
    return cache(func, RedisString)

def cacheStringShort(func):
    return cache(func, RedisString, SHORT_CACHE_TIME)

def cacheStringVeryShort(func):
    return cache(func, RedisString, VERY_SHORT_CACHE_TIME)

def cacheStringForever(func):
    return cache(func, RedisString, FOREVER_CACHE_TIME)

def cacheList(func):
    cachedList = cache(func, RedisObject)
    return cachedList or []

def cacheListShort(func):
    return cache(func, RedisObject, SHORT_CACHE_TIME)

def cacheListVeryShort(func):
    return cache(func, RedisObject, VERY_SHORT_CACHE_TIME)

def cacheListForever(func):
    return cache(func, RedisObject, FOREVER_CACHE_TIME)

def cacheDict(func):
    return cache(func, RedisDict)

def cacheDictShort(func):
    return cache(func, RedisDict, SHORT_CACHE_TIME)

def cacheDictVeryShort(func):
    return cache(func, RedisDict, VERY_SHORT_CACHE_TIME)

def cacheDictForever(func):
    return cache(func, RedisDict, FOREVER_CACHE_TIME)

def cacheSet(func):
    cachedSet = cache(func, RedisObject)
    return cachedSet or set()

def cacheSetShort(func):
    cachedSet = cache(func, RedisObject, SHORT_CACHE_TIME)
    return cachedSet or set()

def cacheSetVeryShort(func):
    cachedSet = cache(func, RedisObject, VERY_SHORT_CACHE_TIME)
    return cachedSet or set()

def cacheSetForever(func):
    cachedSet = cache(func, RedisObject, FOREVER_CACHE_TIME)
    return cachedSet or set()

def cacheSortedSet(func):
    cachedSet = cache(func, RedisObject)
    return cachedSet or set()

def cacheSortedSetShort(func):
    cachedSet = cache(func, RedisObject, SHORT_CACHE_TIME)
    return cachedSet or set()

def cacheSortedSetVeryShort(func):
    cachedSet = cache(func, RedisObject, VERY_SHORT_CACHE_TIME)
    return cachedSet or set()

def cacheSortedSetForever(func):
    cachedSet = cache(func, RedisObject, FOREVER_CACHE_TIME)
    return cachedSet or set()

def cacheObject(func):
    return cache(func, RedisObject)

def cacheObjectShort(func):
    return cache(func, RedisObject, SHORT_CACHE_TIME)

def cacheObjectVeryShort(func):
    return cache(func, RedisObject, VERY_SHORT_CACHE_TIME)

def cacheObjectForever(func):
    return cache(func, RedisObject, FOREVER_CACHE_TIME)

def cacheBool(func):
    return cache(func, RedisBoolean)

def cacheBoolShort(func):
    return cache(func, RedisBoolean, SHORT_CACHE_TIME)

def cacheBoolVeryShort(func):
    return cache(func, RedisBoolean, VERY_SHORT_CACHE_TIME)

def cacheBoolForever(func):
    return cache(func, RedisBoolean, FOREVER_CACHE_TIME)

def cacheInt(func):
    return cache(func, RedisInteger)

def cacheIntShort(func):
    return cache(func, RedisInteger, SHORT_CACHE_TIME)

def cacheIntVeryShort(func):
    return cache(func, RedisInteger, VERY_SHORT_CACHE_TIME)

def cacheIntForever(func):
    return cache(func, RedisInteger, FOREVER_CACHE_TIME)

dbNum = 0
if not os.environ["USER"].startswith("at"):
    class RegularRedis(Redis):
        def __init__(self, host='localhost', port=6379,
                        db=0, password=None, socket_timeout=None, connection_pool=None,
                        charset='utf-8', errors='strict', useATDB=False):
            super(RegularRedis, self).__init__(host, port, db, password, socket_timeout, connection_pool, charset, errors)
    Redis = RegularRedis
    redis.Redis = RegularRedis
    redisClient = Redis(cfgDict['redis']['redisHost'])
else:
    # Ensure that AT always gets on the right DB, whether they import redis, Redis, or redisClient.
    dbNum = int(os.environ["USER"][-1]) + REDIS_NUM_DBS
    class ATRedis(Redis):
        def __init__(self, host='localhost', port=6379,
                        db=0, password=None, socket_timeout=None, connection_pool=None,
                        charset='utf-8', errors='strict', useATDB=True):
            if useATDB:
                db = dbNum
            super(ATRedis, self).__init__(host, port, db, password, socket_timeout, connection_pool, charset, errors)

    Redis = ATRedis
    redis.Redis = ATRedis
    redisClient = Redis(cfgDict['redis']['redisHost'])



KEY_REPLACEMENTS = ([" ", "(", ")", "[", "]"], ["%20", "%28", "%29", "%5b", "%5d"])

class RedisItem(threading.local):
    """The base redis item type"""
    dataType = None

    def __init__(self, key, defaultValue="", expires=False, rclient=None, create=True):
        self.rclient = rclient or redisClient
        if isinstance(key, RedisItem):
            key = key.key
        else:
            key = self.inKey(key)
        self.key = key
        if not self.__correctTypeInRedis__():
            self.rclient.delete(key)
            if create:
                self.resetValue(defaultValue)
        if expires:
            self.rclient.expire(self.key, expires)

    def __correctTypeInRedis__(self):
        return self.rclient.type(self.key) == self.dataType

    def __cmp__(self, value):
        if isinstance(value, RedisItem):
            value = value.value()

        storedValue = self.value()
        if hasattr(storedValue, '__cmp__'):
            return storedValue.__cmp__(value)
        else:
            # 0 is equal
            return self.value() == value and 0 or 1

    def __eq__(self, value):
        return self.value() == value

    def __ne__(self, value):
        return self.value() != value

    def __call__(self):
        return self.value()

    @staticmethod
    def inKey(key):
        key = str(key)
        return listReplace(key, KEY_REPLACEMENTS[0], KEY_REPLACEMENTS[1])

    @staticmethod
    def outKey(key):
        key = str(key)
        return listReplace(key, KEY_REPLACEMENTS[1], KEY_REPLACEMENTS[0])

    @staticmethod
    def __pickleIfNeeded__(value):
        if type(value) not in types.StringTypes:
            value = "<pckl:>" + pickle.dumps(value)
        return value

    @staticmethod
    def __unpickleIfNeeded__(value):
        if value and type(value) in types.StringTypes and value.startswith("<pckl:>"):
            return pickle.loads(str(value[7:]))

        return value

    def __nonzero__(self):
        return bool(self.value())

    def rename(self, key):
        """ Renames this key """
        self.rclient.rename(self.key, key)
        self.key = key

    def delete(self):
        """ Deletes a key """
        return self.rclient.delete(self.key)

    def expire(self, ttl=0):
        """ Sets key expiration in redis """
        return self.rclient.expire(self.key, ttl)

    def ttl(self):
        """ Returns the time for the key to live in redis """
        return self.rclient.ttl(self.key)

    def resetValue(self, value):
        pass

    def value(self):
        pass

    def __str__(self):
        return str(self.value())

    def __repr__(self):
        return repr(self.value())

    def __contains__(self, value):
        return value in self.value()

    def __iter__(self):
        return self.value().__iter__()

    def __getitem__(self, index):
        return self.value().__getitem__(index)


class RedisObject(RedisItem):
    """RedisObject allows you to set and get pickled objects out of redis"""
    dataType = "string"

    def resetValue(self, value):
        return self.rclient.set(self.key, self.__pickleIfNeeded__(value))

    def value(self):
        value = self.rclient.get(self.key)
        if value == None:
            return None
        else:
            return self.__unpickleIfNeeded__(value)

class RedisPickledDict(RedisItem):
    #1. Faster read than RedisDict
    #2. Slower write than RedisDict
    #3. Atomic read/write for entire dict (RedisDict does not have this)
    dataType = "string"

    def __init__(self, key, parent=None, defaultValue=None, rclient=None, create=True):
        if parent != None:
            self.key = parent.key + '.' + key
            key = self.key
        if not defaultValue:
            defaultValue = {}
        RedisItem.__init__(self, key, defaultValue=defaultValue, rclient=rclient, create=create)

    @staticmethod
    def __vpickleIfNeeded__(value):
        return "<vpckl:>" + pickle.dumps(value)


    @staticmethod
    def __unvpickleIfNeeded__(value):
        if value.startswith("<vpckl:>"):
            return pickle.loads(str(value[8:]))

    def value(self):
        value = self.rclient.get(self.key)
        if value == None:
            return None
        else:
            return self.__unvpickleIfNeeded__(value)

    def resetValue(self, value):
        return self.rclient.set(self.key, self.__vpickleIfNeeded__(value))

    def update(self, value):
        existingValue = self.value()
        if type(existingValue) == types.DictType:
            existingValue.update(value)
            value = existingValue
        return self.rclient.set(self.key, self.__vpickleIfNeeded__(value))

    def __setitem__(self, key, value):
        current = self.value()
        key = self.inKey(key)
        current[key] = value
        return self.resetValue(current)

    def __getitem__(self, key):
        key = self.outKey(key)
        current = self.value()
        return current[key]

    def __dict__(self):
        return dict(self.value())



class RedisString(RedisItem):
    """Provides an easy way to manipulate a redis string"""
    dataType = "string"

    def __correctTypeInRedis__(self):
        if RedisItem.__correctTypeInRedis__(self):
            value = self.rclient.get(self.key)
            if value == None:
                return None
            return not str(value).startswith('<pckl:>')
        return False

    def resetValue(self, value):
        return self.rclient.set(self.key, str(value))

    def value(self):
        value = self.rclient.get(self.key)
        if value == None:
            return None
        return str(value)

    def __iadd__(self, stringToAppend):
        self.resetValue(self.value() + stringToAppend)

    def __int__(self):
        return int(self.value())

    def __float__(self):
        return float(self.value())

    def __len__(self):
        return len(self.value())

    def lower(self):
        return self.value().lower()

    def upper(self):
        return self.value().upper()


class RedisInteger(RedisItem):
    """Provides an easy way to manipulate redis integers"""
    dataType = "string"

    def __init__(self, key, defaultValue=0, expires=False, rclient=None, create=True):
        RedisItem.__init__(self, key, defaultValue, expires=expires, rclient=rclient, create=create)

    def resetValue(self, value):
        return self.rclient.set(self.key, int(value))

    def value(self):
        value = self.rclient.get(self.key)
        if value == None:
            return None
        return int(value or 0)

    def __int__(self):
        return self.value()

    def __float__(self):
        return float(self.value())

    def __correctTypeInRedis__(self):
        if RedisItem.__correctTypeInRedis__(self):
            value = self.rclient.get(self.key)
            if value == None:
                return True
            else:
                return str(value).isdigit()
        return False

    def increment(self, amount=1):
        return self.rclient.incr(self.key, amount)

    def decrement(self, amount=1):
        return self.rclient.decr(self.key, amount)

    def __iadd__(self, amount):
        self.increment(amount)
        return self

    def __isub__(self, amount):
        self.decrement(amount)
        return self


class RedisBoolean(RedisItem):
    """Provides an easy way to manipulate redis strings as if they where booleans"""
    dataType = "string"

    def __init__(self, key, defaultValue=False, expires=False, rclient=None, create=True):
        RedisItem.__init__(self, key, defaultValue, expires, rclient=rclient, create=create)

    def __correctTypeInRedis__(self):
        if RedisItem.__correctTypeInRedis__(self):
            return self.rclient.get(self.key) in ('True', 'False', None)
        return False

    def resetValue(self, value):
        return self.rclient.set(self.key, bool(value))

    def value(self):
        value = self.rclient.get(self.key)
        if value == None:
            return None
        elif value == "True":
            return True
        else:
            return False

    def toggle(self):
        self.resetValue(not self.value())

class RedisIterable(RedisItem):
    """ AbstractClass - Provides common methods for redis Iterable types """

    def __init__(self, key, defaultValue="", expires=False, rclient=None, create=True):
        if create:
            self.rclient = rclient or redisClient
            self.expirationTime = self.rclient.ttl(key)
            self.expirationTimestamp = int(time.time())
        else:
            self.expirationTime = None
            self.expirationTimestamp = int(time.time())
        RedisItem.__init__(self, key, defaultValue, expires, rclient, create)

    def __unpickledList__(self, pythonList):
        pythonList = pythonList or []
        for index, item in enumerate(pythonList):
            pythonList[index] = self.__unpickleIfNeeded__(item)
        return pythonList

    def __unpickledSet__(self, pythonSet):
        return_set = set([])
        for member in pythonSet or []:
            return_set.add(self.__unpickleIfNeeded__(member))

        return return_set

    def expire(self, ttl=0):
        RedisItem.expire(self, ttl)
        self.expirationTime = ttl
        self.expirationTimestamp = int(time.time())

    def __nonzero__(self):
        return bool(len(self))

    def asList(self):
        pass

    def asSet(self):
        pass

    def handleExpiration(self):
        if self.expirationTime:
            now = int(time.time())
            self.expirationTime -= (now - self.expirationTimestamp)
            self.expirationTimestamp = now
            if self.expirationTime <= 0:
                self.expire(0) # We expired - clear the key.
            else:
                self.expire(self.expirationTime)




class RedisList(RedisIterable):
    """Provides a way to manipulate a redis list that feels and acts like a python list"""
    dataType = "list"

    def __init__(self, key, defaultValue="", expires=False, rclient=None, create=True):
        if not defaultValue and create:
            # Create is a no-op on a list with an empty data set
            create = False
        RedisIterable.__init__(self, key, defaultValue, expires, rclient, create)

    def resetValue(self, pythonList):
        retVal = True
        self.rclient.delete(self.key)
        if pythonList:
            for value in pythonList:
                if not self.append(value):
                    retVal = False
                    break
        self.handleExpiration()

        return retVal

    def value(self):
        return self.asList()

    def __nonzero__(self):
        return bool(self.__len__())

    def __len__(self):
        return self.rclient.llen(self.key)

    def __getitem__(self, index):
        if type(index) == slice:
            start, stop = (index.start or 0, index.stop)
            if stop == None:
                stop = self.rclient.llen(self.key)
            return self.__unpickledList__(self.rclient.lrange(self.key, start, stop))

        return self.__unpickleIfNeeded__(self.rclient.lindex(self.key, index))

    def __setitem__(self, index, value):
        ret = self.rclient.lset(self.key, index, self.__pickleIfNeeded__(value))
        self.handleExpiration()
        return ret

    def __delitem__(self, index):
        ret = self.rclient.lrem(self.key, index)
        self.handleExpiration()
        return ret

    def asList(self):
        return self.__unpickledList__(self.rclient.lrange(self.key, 0, self.rclient.llen(self.key)))

    def asSet(self):
        return self.__unpickledSet__(set(self.rclient.lrange(self.key, 0, self.rclient.llen(self.key))))

    def sorted(self):
        return self.__unpickledList__(self.rclient.sort(self.key))

    def pop(self):
        ret = self.__unpickleIfNeeded__(self.rclient.lpop(self.key))
        self.handleExpiration()
        return ret

    def append(self, value):
        ret = self.rclient.rpush(self.key, self.__pickleIfNeeded__(value))
        self.handleExpiration()
        return ret

    def remove(self, value):
        ret = self.rclient.lrem(self.key, self.__pickleIfNeeded__(value))
        self.handleExpiration()
        return ret

    def extend(self, valueList):
        for value in valueList:
            self.rclient.rpush(self.key, self.__pickleIfNeeded__(value))
        self.handleExpiration()

        return valueList

    def count(self, value):
        return self.asList().count(self.__pickleIfNeeded__(value))


class RedisSet(RedisIterable):
    """Provides a way to manipulate a redis set that feels and acts like a python set"""
    dataType = "set"

    def __init__(self, key, defaultValue="", expires=False, rclient=None, create=True):
        if not defaultValue and create:
            # Create is a no-op on a set with an empty data set
            create = False
        RedisIterable.__init__(self, key, defaultValue, expires, rclient, create)

    def resetValue(self, pythonSet):
        self.rclient.delete(self.key)
        if pythonSet:
            for value in pythonSet:
                self.add(value)
        self.handleExpiration()

    def __len__(self):
        return self.rclient.scard(self.key)

    def __str__(self):
        return str(self.asList())

    def __repr__(self):
        return str(self.asList())

    def value(self):
        return self.asSet()

    def asSet(self):
        return self.__unpickledSet__(self.rclient.smembers(self.key))

    def asList(self):
        return self.__unpickledList__(list(self.rclient.smembers(self.key) or []))

    def __iter__(self):
        return self.asList().__iter__()

    def clear(self):
        ret = self.delete()
        self.handleExpiration()
        return ret

    def add(self, value):
        ret = self.rclient.sadd(self.key, self.__pickleIfNeeded__(value))
        self.handleExpiration()
        return ret

    def ismember(self, value):
        return self.rclient.sismember(self.key, self.__pickleIfNeeded__(value))

    def remove(self, value):
        ret = self.rclient.srem(self.key, self.__pickleIfNeeded__(value))
        self.handleExpiration()
        return ret

    def intersection(self, value):
        if type(value) in types.StringTypes + (RedisSet,):
            if type(value) == RedisSet:
                return self.__unpickledSet__(self.rclient.sinter((self.key, value.key)))
            return self.__unpickledSet__(self.rclient.sinter((self.key, value)))

        return self.asSet().intersection(value)

    def intersection_update(self, value):
        if type(value) in types.StringTypes + (RedisSet,):
            if type(value) == RedisSet:
                return self.rclient.sinterstore(self.key, (self.key, value.key))
            return self.rclient.sinterstore(self.key, (self.key, value))

        return self.resetValue(self.asSet().intersection(value))

    def difference(self, value):
        if type(value) in types.StringTypes + (RedisSet,):
            if type(value) == RedisSet:
                return self.__unpickledSet__(self.rclient.sdiff((self.key, value.key)))
            return self.__unpickledSet__(self.rclient.sdiff((self.key, value)))

        return self.asSet().difference(value)

    def difference_update(self, value):
        if type(value) in types.StringTypes + (RedisSet,):
            if type(value) == RedisSet:
                return self.rclient.sdiffstore(self.key, (self.key, value.key))
            return self.rclient.sdiffstore(self.key, (self.key, value))

        return self.resetValue(self.asSet().difference(value))

    def union(self, value):
        if type(value) in types.StringTypes + (RedisSet,):
            if type(value) == RedisSet:
                return self.__unpickledSet__(self.rclient.sunion((self.key, value.key)))
            return self.__unpickledSet__(self.rclient.sunion((self.key, value)))

        return self.asSet().union(value)

    def update(self, value):
        if type(value) in types.StringTypes + (RedisSet,):
            if type(value) == RedisSet:
                return self.rclient.sunionstore(self.key, (self.key, value.key))
            return self.rclient.sunionstore(self.key (self.key, value))

        return self.resetValue(self.asSet().union(value))

    def pop(self):
        returnValue = self.__unpickleIfNeeded__(self.rclient.spop(self.key))
        self.handleExpiration()
        return returnValue

class RedisSortedSet(RedisIterable):
    dataType = "zset"

    def __str__(self):
        return str(self.asList())

    def __repr__(self):
        return str(self.asList())

    def all(self):
        return self[0:]

    def value(self):
        return self.asList()

    def asSet(self):
        return self.__unpickledSet__(self.all())

    def asList(self):
        return self.__unpickledList__(self.all())

    def __iter__(self):
        return self.asList().__iter__()

    def clear(self):
        return self.delete()

    def resetValue(self, pythonSet):
        self.rclient.delete(self.key)
        if pythonSet:
            for value in pythonSet:
                self.add(value)
        self.handleExpiration()
        if self.expirationTime:
            self.expire(self.expirationTime)

    def __len__(self):
        return self.rclient.zcard(self.key)

    def score(self, value):
        return self.rclient.zscore(self.key, self.__pickleIfNeeded__(value))

    def add(self, value, score=None):
        if not score:
            score = len(self)
        ret = self.rclient.zadd(self.key, self.__pickleIfNeeded__(value), score)
        self.handleExpiration()
        return ret

    def __getitem__(self, index):
        if type(index) == slice:
            start, stop = (index.start or 0, index.stop)
            if stop == None:
                stop = len(self)
            return self.__unpickledList__(self.rclient.zrange(self.key, start, stop))

        return self.__unpickleIfNeeded__(self.rclient.zrange(self.key, index, index)[0])

    def remove(self, value):
        return self.rclient.zrem(self.key, self.__pickleIfNeeded__(value))

class RedisDict(RedisItem):
    """ Allows you to modify values/keys in redis as if it's a python dictionary using the native redis hash:
           Faster and uses a lot less memory than RedisDict, however doesn't support
           individual key expires.
    """
    dataType = "hash"

    def __getitem__(self, key):
        value = self.rclient.hget(self.key, key)
        if value == None:
            raise KeyError(key)
        return self.__unpickleIfNeeded__(value)

    def __setitem__(self, key, value):
        return self.rclient.hset(self.key, key, self.__pickleIfNeeded__(value))

    def setItem(self, key, value):
        return self.__setitem__(key, value)

    def keys(self):
        return self.rclient.hkeys(self.key)

    def value(self):
        return self.asDict()

    def resetValue(self, valueDictionary=None):
        self.clear()
        if valueDictionary:
            return self.update(valueDictionary)

    def asDict(self):
        dictionary = self.rclient.hgetall(self.key)
        for key, value in dictionary.iteritems():
            dictionary[key] = self.__unpickleIfNeeded__(value)
        return dictionary

    def values(self):
        values = self.rclient.hvals(self.key)
        for index, value in enumerate(values):
            values[index] = self.__unpickleIfNeeded__(value)

        return values

    def move(self, key1, key2):
        if key1 == key2:
            return True
        if self.has_key(key2):
            return False
        self[key2] = self[key1]
        del self[key1]
        return True

    def items(self):
        return self.asDict().items()

    def has_key(self, key):
        return self.rclient.hexists(self.key, key)

    def __contains__(self, key):
        return self.has_key(key)

    def update(self, updateDictionary):
        for key, value in updateDictionary.iteritems():
            self.__setitem__(key, value)

        return True

    def pop(self, key, default=None):
        value = self.get(key, None)
        if not self.__delitem__(key):
            return default
        return value

    def clear(self):
        return self.rclient.delete(self.key)

    def get(self, key, default=None):
        if self.has_key(key):
            return self[key]

        return default

    def __delitem__(self, key):
        return self.rclient.hdel(self.key, key)

    def setdefault(self, key, defaultValue):
        if not self.has_key(key):
            self[key] = defaultValue

        return self[key]

    def __len__(self):
        return self.rclient.hlen(self.key)

class RedisKeyDict(RedisItem):
    """ Allows you to modify values/keys in redis as if it's a python dictionary using nested redis keys"""

    def __init__(self, key, expires=False, rclient=None):
        self.key = self.inKey(key)
        self.__objectCache__ = {}
        self.metaInfo = RedisString(key, defaultValue="<dict:>", expires=expires)
        self.rclient = rclient or redisClient

    def delete(self):
        return self.clear() and self.metaInfo.delete()

    def __getitem__(self, key):
        key = self.inKey(key)
        key = key.replace('.', '%2e')

        if not self.has_key(key):
            self.__objectCache__.pop(key, False)
            raise KeyError(key)

        valueObject = self.__objectCache__.get(key)
        if valueObject:
            return valueObject

        redisObjectType = RedisObject
        redisType = self.rclient.type(self.key + "." + key)
        if redisType == "string":
            redisValue = self.rclient.get(self.key + "." + key)
            if redisValue.isdigit():
                redisObjectType = RedisInteger
            elif redisValue.startswith("<pckl:>"):
                redisObjectType = RedisObject
            elif redisValue.startswith("<vpckl:>"):
                redisObjectType = RedisPickledDict
            elif redisValue == "<dict:>":
                redisObjectType = RedisKeyDict
            elif redisValue in ('True', 'False'):
                redisObjectType = RedisBoolean
            else:
                redisObjectType = RedisString
        elif redisType == "list":
            redisObjectType = RedisList
        elif redisType == "set":
            redisObjectType = RedisSet
        elif redisType == "zset":
            redisObjectType = RedisSortedSet

        self.__objectCache__[key] = redisObjectType(self.key + "." + key, rclient=self.rclient)
        return self.__objectCache__[key]

    def __setitem__(self, key, value):
        key = self.inKey(key)
        key = key.replace('.', '%2e')

        if type(value) in types.StringTypes:
            redisItemClass = RedisString
        elif type(value) == bool:
            redisItemClass = RedisBoolean
        elif type(value) == int:
            redisItemClass = RedisInteger
        elif isinstance(value, list):
            redisItemClass = RedisList
        elif isinstance(value, SortedSet):
            redisItemClass = RedisSortedSet
        elif isinstance(value, set):
            redisItemClass = RedisSet
        elif type(value) == RedisPickledDict:
            redisItemClass = RedisPickledDict
        elif isinstance(value, dict):
            redisItemClass = RedisKeyDict
        else:
            redisItemClass = RedisObject

        valueObject = self.__objectCache__.get(key)
        if valueObject.__class__ == redisItemClass:
            return valueObject.resetValue(value)

        item = redisItemClass(self.key + "." + key, expires=self.ttl(), rclient=self.rclient)
        self.__objectCache__[key] = item
        return item.resetValue(value)

    def setItem(self, key, value, expire=False):
        retVal = self.__setitem__(key, value)
        expire = expire or self.ttl()
        if expire:
            self.rclient.expire(self.key + '.' + self.inKey(key), expire)
        return retVal

    def move(self, key1, key2):
        key2 = self.inKey(key2)

        if key1 == key2:
            return True

        if self.has_key(key2):
            return False

        self[key1].rename(self.key + "." + key2)
        self.__objectCache__[key2] = self.__objectCache__.pop(self.inKey(key1))

        return True

    def keys(self):
        keys = []
        for key in self.rclient.keys(self.key + ".*"):
            outKey = self.outKey(key[len(self.key) + 1:])
            if not "." in outKey:
                keys.append(outKey)

        return keys

    def rename(self, key):
        """ Renames this key """
        for itemKey, value in self.items():
            if isinstance(value, RedisKeyDict):
                value.rename(key + ":" + itemKey)
            else:
                value.rename(key + "." + itemKey)

        self.metaInfo.rename(key)
        self.key = key

    def value(self):
        return self.asDict()

    def resetValue(self, valueDictionary):
        self.clear()
        return self.update(valueDictionary)

    def asDict(self):
        valueDictionary = {}
        for key, value in self.items():
            key = key.replace('%2e', '.')
            valueDictionary[key] = value

        return valueDictionary

    def values(self):
        values = []
        for key in self.keys():
            values.append(self[key])

        return values

    def items(self):
        return zip(self.keys(), self.values())

    def has_key(self, key):
        return self.rclient.exists(self.key + "." + self.inKey(key))

    def __contains__(self, key):
        return self.has_key(key)

    def update(self, updateDictionary):
        for key, value in updateDictionary.iteritems():
            self.__setitem__(key, value)

        return True

    def pop(self, key, default=None):
        value = self.get(key, None)
        if not self.__delitem__(key):
            return default
        return value

    def expire(self, seconds=0):
        """ Sets key expiration in redis """
        self.metaInfo.expire(seconds)
        for value in self.values():
            value.expire(seconds)

    def ttl(self):
        """ Returns the time for the key to live in redis """
        return self.metaInfo.ttl()

    def clear(self):
        success = True
        for key in self.keys():
            if not self.__delitem__(key):
                success = False

        return success

    def get(self, key, default=None):
        if self.has_key(key):
            return self[key]

        return default

    def __delitem__(self, key):
        returnValue = self[key].delete()
        self.__objectCache__.pop(self.inKey(key), None)
        return returnValue

    def setdefault(self, key, defaultValue):
        if not self.has_key(key):
            self[key] = defaultValue

        return self[key]

    def __len__(self):
        return len(self.keys())

class RedisLock(RedisList):
    ''' A locking mechanism implemented in Redis safe and atomic across multiple machines '''
    RESULT_TIMEOUT = "__~~THERE_WAS_A_TIMEOUT~~__9191919192828383" # Return if timeout exceeded
    RESULT_LOSTKEY = "__~~THERE_WAS_A_LISTCLEAR~~__9191912938293892" #List cleared. You should retry.
    MAX_KEYHOLD = 60 # Seconds for the maximum someone can hold the lock
    POLL_INTERVAL = .5 # Seconds to poll

    def __init__(self, key):
        RedisList.__init__(self, key, defaultValue=[])
        self.lastAcquireTime = RedisInteger(key + '.lastAcquireTime', defaultValue=0)

    def updateLastAcquireTime(self):
        ''' Reset last lock acquisition to now '''
        self.lastAcquireTime.resetValue(int(time.time()))

    def checkLastAcquireTime(self):
        ''' This method enforces a max keyhold time'''
        now = int(time.time())
        lastAcquireTimeValue = self.lastAcquireTime.value()
        if lastAcquireTimeValue != 0 and (now - lastAcquireTimeValue) > self.MAX_KEYHOLD:
            # Somebody must have acquired the lock and never released it, so send everyone a RESULT_LOSTKEY because we must clear the list.
            self.updateLastAcquireTime()
            self.resetValue([])
            time.sleep(.5)
            return False
        return True


    def acquire(self, myKey, timeout=10):
        ''' Acquire lock, using a UNIQUE KEY of @myKey. timeout of @timeout, cannot be gretear than MAX_KEYHOLD '''
        self.checkLastAcquireTime()

        self.append(myKey)
        if timeout > self.MAX_KEYHOLD - 2:
            timeout = self.MAX_KEYHOLD - 2
        xrange_time = int(timeout / self.POLL_INTERVAL)


        for i in xrange(xrange_time):
            myValue = self.value()
            if not myValue or len(myValue) == 0 or myKey not in myValue:
                self.remove(myKey)
                return self.RESULT_LOSTKEY
            if myValue and len(myValue) and myValue[0] == myKey:
                self.updateLastAcquireTime()
                return True
            time.sleep(self.POLL_INTERVAL)
        self.remove(myKey)
        return self.RESULT_TIMEOUT

    def release(self, myKey):
        ''' Release lock held by @myKey '''
        myValue = self.value()
        ret = True
        if myValue and len(myValue) and myValue[0] != myKey:
            ret = False
#            raise Exception("You do not hold this lock!")
        try:
            self.remove(myKey)
        except:
            pass
        return ret
