RedisStore
==========

RedisStore is python library that allows interacting with Redis keys as if they where 
normal python objects.

## Installation

    $ sudo pip install redisstore

or alternatively:

    $ sudo easy_install redisstore

From source:

    $ sudo python setup.py install


## Getting Started

    >>> from RedisStore import RedisList
    >>>
    >>> myList = RedisList("myKey")
    >>> myList.append("item1")
    >>> myList.append({'key':'value'})
    >>> for item in myList:
    >>>     print item #loaded live from RedisDB
    
## Supported Redis Data Types
    RedisString
    RedisObject - uses redis string but pickles on storing and unpickles on reading
    RedisList
    RedisSet
    RedisSortedSet
    RedisDict
    
    
## Other Info
    Uses py-redis as a back-end for interaction with Redis.
    Incudes convienence decorators for caching methods to correct data types.

    
