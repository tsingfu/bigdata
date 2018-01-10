from resource_management.core.resources.system import Execute
from resource_management.libraries.script import Script

from resource_management.core.resources.system import Directory
from resource_management.core.resources.system import File
from resource_management.core.source import InlineTemplate
from resource_management.libraries.functions.check_process_status import check_process_status


class Graphite(Script):
    def install(self, env):
        import params
        env.set_params(params)
        self.install_packages(env)

    def configure(self, env, upgrade_type=None, config_dir=None):
        import params
        env.set_params(params)

    def stop(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        Execute("")

    def start(self, env, upgrade_type=None):
        import params
        env.set_params(params)

        self.configure(env)
        Execute("")

    def status(self, env):
        import params


if __name__ == "__main__":
    Graphite().execute()
