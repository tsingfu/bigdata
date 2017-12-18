from resource_management import *
from resource_management.libraries.functions.check_process_status import check_process_status
from resource_management.libraries.functions.format import format
from resource_management.libraries.script.script import Script
from resource_management.core.resources.system import Directory, File, Execute
from redis import redis


class redis_service(Script):
    def install(self, env):
        self.install_packages(env)
        self.configure(env)

    def configure(self, env):
        import params
        env.set_params(params)
        redis()

    def stop(self, env):
        import params
        env.set_params(params)
        stop_cmd = format("service redis stop")
        Execute(stop_cmd)

    def start(self, env):
        import params
        env.set_params(params)
        start_cmd = format("service redis start")
        Execute(start_cmd)

    def status(self, env):
        import status_params
        env.set_params(status_params)
        check_process_status(status_params.redis_pid_file)


if __name__ == "__main__":
    redis_service().execute()
