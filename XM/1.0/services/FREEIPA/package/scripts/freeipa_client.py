import freeipa
import os
import glob
from resource_management import *


class FreeipaClient(Script):

    packages = [
        'ntp', 'curl', 'wget', 'pdsh', 'openssl', 'ipa-client',
        'ipa-admintools', 'oddjob-mkhomedir'
    ]
    ipa_client_install_lock_file = '/root/ipa_client_install_lock_file'

    def status(self, env):
        raise ClientComponentHasNoStatus()

    def write_resolvconf(self, env):
        """
        Common method to overwrite resolv.conf if required
        sensitive to the value of params.install_with_dns

        NB: If we are installing freeipa with DNS the settings in resolv.conf must
        be overriden. However these new settings will probably not survive a
        network restart. This could cause potential problems.

        This also can cause a lot of issues on failed FreeIPA installations, or while
        the FreeIPA server is switched off, and so we give the user full control over
        the resolv.conf template so that they can modify this approach if needed
        """
        import params
        env.set_params(params)
        if params.install_with_dns:
            File(
                "/etc/resolv.conf",
                content=InlineTemplate(params.resolvconf_template),
                mode=0644)

    def install(self, env):
        import params
        env.set_params(params)

        self.install_jce()
        installed_on_server = (params.ipa_server == params.hostname)

        if installed_on_server:
            print 'The FreeIPA client installation is modified when installed on the freeipa server:',
            print ' %s freeipa_server %s' % (params.ipa_server,
                                             params.hostname)

        if os.path.exists(self.ipa_client_install_lock_file):
            print 'ipa client already installed, nothing to do here.'
            return self.write_resolvconf(env)

        rm = freeipa.RobotAdmin()
        # Native package installation system driven by metainfo.xml intentionally
        # avoided. Both client and server components are very different and don't
        # intersect on any highlevel component.
        if not installed_on_server:
            for package in self.packages:
                Package(package)

            Execute('chkconfig ntpd on')

            # installs ipa-client software
            rm.client_install(params.ipa_server, params.domain, params.realm,
                              params.client_init_wait, params.install_with_dns,
                              params.install_distribution_user)

        # here we remove the robot-admin-password in case we are not running on the server
        # Note the strange construction due to the enter/exit clauses of the get_freeipa method
        # Although it may look like these lines do nothing, do not be fooled
        with rm.get_freeipa(not installed_on_server) as fi:
            pass

        # Only write the resolv.conf if the client installation was successful, otherwise I can get into biiig trouble!
        self.write_resolvconf(env)

        if not os.path.exists(self.ipa_client_install_lock_file):
            with open(self.ipa_client_install_lock_file, 'w') as f:
                f.write('')

    def install_jce(self):
        import params
        # need to think of some protection against recursive softlinks
        for javapath in params.searchpath.split(':'):
            # print "this is javaPath"+javapath
            if not len(javapath):
                continue
            # Does the top directory exist and is it a directory?
            if os.path.isdir(
                    os.path.realpath(
                        os.sep.join(javapath.split(os.sep)[:-1]))):
                for dir in glob.glob(javapath):
                    dir = os.path.realpath(dir)
                    if os.path.isdir(dir):
                        # print os.listdir(dir)
                        for folderpath in params.folderpath.split(':'):
                            if not os.path.isdir(dir + '/' + folderpath):
                                Execute('mkdir -p ' + dir + '/' + folderpath)
                            if '1.7' == self.java_version_installed(dir):
                                Execute('unzip -o -j -q jce_policy-7.zip -d ' +
                                        dir + '/' + folderpath)
                            else:
                                Execute('unzip -o -j -q jce_policy-8.zip -d ' +
                                        dir + '/' + folderpath)

    def java_version_installed(self, dir):
        if '1.7' in dir:
            return '1.7'
        else:
            return '1.8'


if __name__ == "__main__":
    FreeipaClient().execute()
