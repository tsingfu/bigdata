#!/usr/bin/env python

from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.format import format
from resource_management.core.resources.system import Execute

class JupyterKernelGatewayServiceCheck(Script):
    def service_check(self, env):
        import jkg_toree_params as params
        env.set_params(params)

        if params.security_enabled:
          jnbg_kinit_cmd = format("{kinit_path_local} -kt {jnbg_kerberos_keytab} {jnbg_kerberos_principal}; ")
          Execute(jnbg_kinit_cmd, user=params.user)

        scheme = "https" if params.ui_ssl_enabled else "http"
        Execute(format("curl -s -o /dev/null -w'%{{http_code}}' --negotiate -u: -k {scheme}://{jkg_host}:{jkg_port}/api/kernelspecs | grep 200"),
                tries = 10,
                try_sleep=3,
                logoutput=True)
        Execute(format("curl -s --negotiate -u: -k {scheme}://{jkg_host}:{jkg_port}/api/kernelspecs | grep Scala"),
                tries = 10,
                try_sleep=3,
                logoutput=True)

if __name__ == "__main__":
    JupyterKernelGatewayServiceCheck().execute()
