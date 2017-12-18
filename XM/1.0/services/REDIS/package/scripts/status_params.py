#!/usr/bin/env python
from resource_management import *
from resource_management.libraries.script.script import Script
config = Script.get_config()


redis_pid_file = format("/var/run/redis/redis.pid")
sentinel_pid_file = format("/var/run/redis/sentinel.pid")
