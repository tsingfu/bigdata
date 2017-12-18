import os
from resource_management import *
from resource_management.core.resources.system import Execute, File
from resource_management.core.source import InlineTemplate, StaticFile
from resource_management.libraries.functions.format import format
from resource_management.libraries.functions.copy_tarball import copy_to_hdfs
from resource_management.libraries.script.script import Script


class TitanServiceCheck(Script):
    def service_check(self, env):
        import params
        env.set_params(params)

        File(format("{tmp_dir}/titanSmoke.groovy"),
             content=StaticFile("titanSmoke.groovy"),
             mode=0755
             )

        if params.security_enabled:
            kinit_cmd = format("{kinit_path_local} -kt {smoke_user_keytab} {smokeuser_principal};")
            Execute(kinit_cmd,
                    user=params.smokeuser
                    )

        secure = ""
        if params.titan_server_ssl == "true":
            secure = "-k"
            if params.titan_server_ssl_key_cert_file:
                secure = "--cacert " + params.titan_server_ssl_key_cert_file.split(":")[1]
        grepresult = """ | grep 99"""
        if len(params.titan_server_simple_authenticator) > 0:
            grepresult = ""
        headers = """ -XPOST -Hcontent-type:application/json -d '{"gremlin":"100-1"}' """
        http = "http://"
        if params.titan_server_ssl == "true":
            http = "https://"
        titan_server_host = http + format("{titan_host}")
        titan_port = format("{titan_server_port}")
        cmd = "curl " + secure + headers + titan_server_host + ":" + titan_port + grepresult

        Execute((cmd),
                tries=40,
                try_sleep=5,
                path=format('{titan_bin_dir}:/usr/sbin:/sbin:/usr/local/bin:/bin:/usr/bin'),
                user=params.smokeuser,
                logoutput=True
                )

        Execute(format("gremlin {tmp_dir}/titanSmoke.groovy"),
                tries=3,
                try_sleep=5,
                path=format('{titan_bin_dir}:/usr/sbin:/sbin:/usr/local/bin:/bin:/usr/bin'),
                user=params.smokeuser,
                logoutput=True
                )


if __name__ == "__main__":
    # print "Track service check status"
    TitanServiceCheck().execute()
