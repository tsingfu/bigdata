from kerberos_common import *
from resource_management.libraries.functions.security_commons import cached_kinit_executor
from resource_management.core.exceptions import ClientComponentHasNoStatus
from resource_management.core.resources.system import Execute
from resource_management.libraries.functions import default

class KerberosClient(KerberosScript):
    def install(self, env):
        install_packages = default(
            '/configurations/kerberos-env/install_packages', "true")
        if install_packages:
            self.install_packages(env)
        else:
            print "Kerberos client packages are not being installed, manual installation is required."

        self.configure(env)

    def kerberos_client_conf(self):
        kerberos_host = default('clusterHostInfo/krb5_master_hosts',[])
        if len(kerberos_host) > 0:
            realm = default('configurations/krb5-config/kdc.realm','example.com')
            Execute('/usr/sbin/authconfig --enablekrb5 --krb5kdc="' + ' '.join(kerberos_host) + '"  --krb5adminserver="' + ' '.join(kerberos_host) + '"  --krb5realm="' + realm + '"  --update')

    def configure(self, env):
        import params
        env.set_params(params)
        #self.kerberos_client_conf()
        if params.manage_krb5_conf:
            self.write_krb5_conf()
        #delete krb cache to prevent using old krb tickets on fresh kerberos setup
        self.clear_tmp_cache()

    def status(self, env):
        raise ClientComponentHasNoStatus()

    def security_status(self, env):
        import status_params
        if status_params.security_enabled:
            if status_params.smoke_user and status_params.smoke_user_keytab:
                try:
                    cached_kinit_executor(status_params.kinit_path_local,
                                          status_params.smoke_user,
                                          status_params.smoke_user_keytab,
                                          status_params.smoke_user_principal,
                                          status_params.hostname,
                                          status_params.tmp_dir)
                    self.put_structured_out(
                        {"securityState": "SECURED_KERBEROS"})
                except Exception as e:
                    self.put_structured_out({"securityState": "ERROR"})
                    self.put_structured_out({"securityStateErrorInfo": str(e)})
            else:
                self.put_structured_out({"securityState": "UNKNOWN"})
                self.put_structured_out({"securityStateErrorInfo":
                                         "Missing smoke user credentials"})
        else:
            self.put_structured_out({"securityState": "UNSECURED"})

    def set_keytab(self, env):
        self.write_keytab_file()

    def remove_keytab(self, env):
        self.delete_keytab_file()


if __name__ == "__main__":
    KerberosClient().execute()
