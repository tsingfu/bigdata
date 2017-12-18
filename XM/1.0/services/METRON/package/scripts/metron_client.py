
from resource_management.libraries.script.script import Script
from resource_management.core.exceptions import ClientComponentHasNoStatus
from resource_management.core.resources.system import Directory
from metron_security import storm_security_setup
from metron_service import install_metron
class MetronClient(Script):

    def install(self, env):
        import params
        env.set_params(params)
        install_metron()
        self.configure(env)

    def configure(self, env):
        import params
        env.set_params(params)
        storm_security_setup(params)

    def start(self, env, upgrade_type=None):
        import params
        env.set_params(params)

    def stop(self, env, upgrade_type=None):
        import params
        env.set_params(params)

    def status(self, env):
        raise ClientComponentHasNoStatus()

if __name__ == "__main__":
    MetronClient().execute()
