from resource_management import *

from redis_sentinel import redis_sentinel
from resource_management.libraries.script.script import Script
from resource_management.core.resources.system import Directory, File, Execute
from resource_management.libraries.functions.check_process_status import check_process_status

class redis_sentinel_service(Script):
    def install(self, env):
        self.install_packages(env)
        self.configure(env)

    def configure(self, env):
        import params
        env.set_params(params)
        redis_sentinel()

    def stop(self, env):
        import params
        env.set_params(params)
        Execute('service redis-sentinel stop')

    def start(self, env):
        import params
        env.set_params(params)
        Execute('service redis-sentinel start')

    def status(self, env):
        import status_params
        env.set_params(status_params)
        check_process_status(status_params.sentinel_pid_file)


if __name__ == "__main__":
    redis_sentinel_service().execute()
