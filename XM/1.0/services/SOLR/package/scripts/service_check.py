from resource_management.core.logger import Logger
from resource_management.core.resources.system import Execute
from resource_management.libraries.functions.format import format
from resource_management.libraries.script.script import Script


class ServiceCheck(Script):
    def service_check(self, env):
        import params
        env.set_params(params)

        Logger.info('Solr Service Check ...')
        if "solr-env" in params.config['configurations'] \
                and params.solr_hosts is not None \
                and len(params.solr_hosts) > 0:
            solr_protocol = "https" if params.solr_ssl_enabled else "http"
            solr_host = params.solr_hosts[0]  # choose the first solr host
            solr_port = params.solr_port
            solr_url = format("{solr_protocol}://{solr_host}:{solr_port}/solr/#/")

            smokeuser_kinit_cmd = format(
                "{kinit_path_local} -kt {smoke_user_keytab} {smokeuser_principal};") if params.security_enabled else ""
            smoke_solr_cmd = format(
                "{smokeuser_kinit_cmd} curl -s -o /dev/null -w'%{{http_code}}' --negotiate -u: -k {solr_url} | grep 200")
            Execute(smoke_solr_cmd,
                    tries=40,
                    try_sleep=3,
                    user=params.smokeuser,
                    logoutput=True)


if __name__ == "__main__":
    ServiceCheck().execute()
