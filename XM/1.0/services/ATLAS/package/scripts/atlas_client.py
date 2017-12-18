#!/usr/bin/env python

import sys
from resource_management.libraries.script.script import Script
from resource_management.core.exceptions import ClientComponentHasNoStatus

from metadata import metadata, install_atlas


class AtlasClient(Script):
    def get_component_name(self):
        return "atlas-client"

    def pre_upgrade_restart(self, env, upgrade_type=None):
        import params
        env.set_params(params)

    def install(self, env):
        install_atlas(first=True)
        self.configure(env)

    def configure(self, env, upgrade_type=None, config_dir=None):
        import params
        env.set_params(params)
        install_atlas()
        metadata('client')

    def status(self, env):
        raise ClientComponentHasNoStatus()


if __name__ == "__main__":
    AtlasClient().execute()
