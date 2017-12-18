from resource_management.core.exceptions import ClientComponentHasNoStatus
from resource_management.libraries.script.script import Script

from angel import angel, install_angel


class Angel(Script):
    def configure(self, env, config_dir=None, upgrade_type=None):
        import params
        env.set_params(params)
        angel(config_dir)

    def status(self, env):
        raise ClientComponentHasNoStatus()

    def stack_upgrade_save_new_config(self, env):
        import params
        env.set_params(params)
        print 'todo'

    def pre_upgrade_restart(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        print 'todo'

    def install(self, env):
        import params
        install_angel()
        self.configure(env, config_dir=params.angel_conf_dir)
        

if __name__ == "__main__":
    Angel().execute()
