from resource_management.libraries.script.hook import Hook
from shared_initialization import setup_config

class AfterInstallHook(Hook):
    def hook(self, env):
        import params
        env.set_params(params)
        #setup_stack_symlinks()
        setup_config()
        #link_configs(self.stroutfile)


if __name__ == "__main__":
    AfterInstallHook().execute()
