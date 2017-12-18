from resource_management.libraries.script.script import Script
from pig import pig, install_pig
from resource_management.core.exceptions import ClientComponentHasNoStatus


class PigClient(Script):
    def configure(self, env):
        import params
        env.set_params(params)
        install_pig(first=True)
        pig()

    def status(self, env):
        raise ClientComponentHasNoStatus()

    def get_component_name(self):
        return "pig-client"

    def pre_upgrade_restart(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        install_pig()

    def install(self, env):
        install_pig()
        self.configure(env)


if __name__ == "__main__":
    PigClient().execute()
