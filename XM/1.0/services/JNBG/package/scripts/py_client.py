#!/usr/bin/env python

import os
from resource_management.core.resources.system import Execute, File, Directory
from resource_management.core.source import StaticFile, InlineTemplate, Template
from resource_management.core.resources.system import Execute
from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.format import format
import jnbg_helpers as helpers


class PyClient(Script):
    def install(self, env):
        import py_client_params as params
        from jkg_toree_params import user, group, sh_scripts_dir, sh_scripts, sh_scripts_user

        # Setup bash scripts for execution
        for sh_script in sh_scripts:
            File(sh_scripts_dir + os.sep + sh_script,
                 content=StaticFile(sh_script),
                 mode=0750
                 )
        for sh_script in sh_scripts_user:
            File(sh_scripts_dir + os.sep + sh_script,
                 content=StaticFile(sh_script),
                 mode=0755
                 )

        self.install_packages(env)
        self.configure(env)

        # Run install commands for Python client defined in params
        for command in params.commands: Execute(command, logoutput=True)

    def status(self, env):
        raise ClientComponentHasNoStatus()

    def configure(self, env):
        import py_client_params as params
        env.set_params(params)


if __name__ == "__main__":
    PyClient().execute()
