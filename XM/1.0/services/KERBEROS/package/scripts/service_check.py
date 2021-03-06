from resource_management.libraries import functions
from resource_management.libraries.functions.format import format
from resource_management.core.resources.system import Execute, File
from resource_management.core.logger import Logger

from kerberos_common import *

# hashlib is supplied as of Python 2.5 as the replacement interface for md5
# and other secure hashes.  In 2.6, md5 is deprecated.  Import hashlib if
# available, avoiding a deprecation warning under 2.6.  Import md5 otherwise,
# preserving 2.4 compatibility.
try:
    import hashlib
    _md5 = hashlib.md5
except ImportError:
    import md5
    _md5 = md5.new


class KerberosServiceCheck(KerberosScript):
    def service_check(self, env):
        import params

        # If Ambari IS managing Kerberos identities (kerberos-env/manage_identities = true), it is
        # expected that a (smoke) test principal and its associated keytab file is available for use
        # **  If not available, this service check will fail
        # **  If available, this service check will execute
        #
        # If Ambari IS NOT managing Kerberos identities (kerberos-env/manage_identities = false), the
        # smoke test principal and its associated keytab file may not be available
        # **  If not available, this service check will execute
        # **  If available, this service check will execute

        if ((params.smoke_test_principal is not None) and
            (params.smoke_test_keytab_file is not None) and
                os.path.isfile(params.smoke_test_keytab_file)):
            print "Performing kinit using %s" % params.smoke_test_principal

            ccache_file_name = _md5(
                "{0}|{1}".format(params.smoke_test_principal,
                                 params.smoke_test_keytab_file)).hexdigest()
            ccache_file_path = "{0}{1}kerberos_service_check_cc_{2}".format(
                params.tmp_dir, os.sep, ccache_file_name)

            kinit_path_local = functions.get_kinit_path(
                default('/configurations/kerberos-env/executable_search_paths',
                        None))
            kinit_command = "{0} -c {1} -kt {2} {3}".format(
                kinit_path_local, ccache_file_path,
                params.smoke_test_keytab_file, params.smoke_test_principal)

            try:
                # kinit
                Execute(kinit_command, user=params.smoke_user)
            finally:
                File(ccache_file_path, # Since kinit might fail to write to the cache file for various reasons, an existence check should be done before cleanup
                     action = "delete",
                )
        elif params.manage_identities:
            err_msg = Logger.filter_text(
                "Failed to execute kinit test due to principal or keytab not found or available")
            raise Fail(err_msg)
        else:
            # Ambari is not managing identities so if the smoke user does not exist, indicate why....
            print "Skipping this service check since Ambari is not managing Kerberos identities and the smoke user " \
                  "credentials are not available. To execute this service check, the smoke user principal name " \
                  "and keytab file location must be set in the cluster_env and the smoke user's keytab file must" \
                  "exist in the configured location."


if __name__ == "__main__":
    KerberosServiceCheck().execute()
