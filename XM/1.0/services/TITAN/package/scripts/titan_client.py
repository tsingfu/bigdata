from resource_management import *
from resource_management.libraries.script.script import Script
import titan
from titan import install_titan

class TitanClient(Script):
    def configure(self, env):
        import params
        env.set_params(params)
        install_titan()
        titan.titan()

    def status(self, env):
        raise ClientComponentHasNoStatus()

    def pre_rolling_restart(self, env):
        import params
        env.set_params(params)
        print 'todo'

    def install(self, env):
        self.install_packages(env)
        install_titan(first=True)
        self.configure(env)


if __name__ == "__main__":
    TitanClient().execute()
