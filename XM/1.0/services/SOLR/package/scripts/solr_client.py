from resource_management.core.exceptions import ClientComponentHasNoStatus
from resource_management.libraries.script.script import Script
from setup_solr import setup_solr, install_solr


class SolrClient(Script):
    def install(self, env):
        import params
        env.set_params(params)
        install_solr(first=True)
        self.configure(env)

    def configure(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        setup_solr(name='client')

    def start(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        install_solr()
        self.configure(env)

    def stop(self, env, upgrade_type=None):
        import params
        env.set_params(params)

    def status(self, env):
        raise ClientComponentHasNoStatus()


if __name__ == "__main__":
    SolrClient().execute()
