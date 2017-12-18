from resource_management import *
from resource_management.core.resources.system import Directory, Execute, File
from resource_management.core.source import InlineTemplate, Template
from resource_management.libraries.functions.check_process_status import check_process_status


class Tserver(Script):
    def install(self, env):
        self.install_packages(env)
        self.configure(env)

    def configure(self, env):
        import params
        env.set_params(params)

    def stop(self, env):
        self.configure(env)
        Execute('service kudu-master stop')

    def start(self, env):
        self.configure(env)
        Execute('service kudu-master start')

    def status(self, env):
        import status_params
        env.set_params(status_params)
        check_process_status(status_params.tserver_pid)


if __name__ == "__main__":
    Tserver().execute()
