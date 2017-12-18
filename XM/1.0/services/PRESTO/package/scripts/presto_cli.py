# -*- coding: utf-8 -*-

from resource_management.libraries.script.script import Script
from resource_management.core.resources.system import Execute
from resource_management.core.exceptions import ClientComponentHasNoStatus


class Cli(Script):
    def install(self, env):
        import params
        import os
        if not os.path.exists('/usr/sbin/presto-cli'):
            Execute('mkdir -p /usr/sbin')
            Execute('wget %s -O /usr/sbin/presto-cli' % params.download_url_cli)
            Execute('chmod +x /usr/sbin/presto-cli')

    def status(self, env):
        raise ClientComponentHasNoStatus()

    def configure(self, env):
        import params
        env.set_params(params)

    def start(self, env):
        import params
        env.set_params(params)

    def stop(self, env):
        import params
        env.set_params(params)


if __name__ == '__main__':
    Cli().execute()
