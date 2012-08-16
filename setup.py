#!/usr/bin/env python

from distutils.core import setup

setup(name='RedisStore',
      version='0.1',
      description='Enables interacting with Redis keys from within python as if they where native object types.',
      author='Timothy Crosley',
      author_email='timothy.crosley@gmail.com',
      url='http://www.simpleinnovation.org/',
      download_url='https://github.com/timothycrosley/RedisStore/blob/master/dist/RedisStore-0.1.tar.gz?raw=true',
      license = "GNU GPLv2",
      requires = ['redis',],)
