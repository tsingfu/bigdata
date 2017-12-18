#!/usr/bin/env python

from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.format import format
from resource_management.core.resources.system import Execute


class ServiceCheck(Script):
    def service_check(self, env):
        import params
        env.set_params(params)
        for superset_host in params.config['clusterHostInfo']['superset_hosts']:
            Execute(format(
                "curl -s -o /dev/null -w'%{{http_code}}' --negotiate -u: -k {superset_host}:{params.superset_webserver_port}/health | grep 200"),
                tries=10,
                try_sleep=3,
                logoutput=True)


if __name__ == "__main__":
    ServiceCheck().execute()
