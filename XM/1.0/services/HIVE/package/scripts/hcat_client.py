#!/usr/bin/env python

from hcat import hcat
from resource_management.core.exceptions import ClientComponentHasNoStatus
from resource_management.libraries.script.script import Script
from hive import install_hive


class HCatClient(Script):
    def install(self, env):
        import params
        install_hive()
        self.configure(env)

    def configure(self, env):
        import params
        env.set_params(params)
        install_hive()
        hcat()

    def status(self, env):
        raise ClientComponentHasNoStatus()

    def get_component_name(self):
        # HCat client doesn't have a first-class entry in <stack-selector-tool>. Since clients always
        # update after daemons, this ensures that the hcat directories are correct on hosts
        # which do not include the WebHCat daemon
        return "hive-webhcat"

    def pre_upgrade_restart(self, env, upgrade_type=None):
        print 'todo'


if __name__ == "__main__":
    HCatClient().execute()
