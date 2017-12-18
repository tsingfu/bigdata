# encoding=utf8

import sys, os, pwd, signal, time
from resource_management import *
from resource_management.core.resources.system import Directory, Execute, File
from resource_management.core.source import InlineTemplate, Template
from resource_management.core.logger import Logger
from resource_management.libraries.script.script import Script


def turn_on_maintenance():
    config = Script.get_config()
    ambari_server_host = config['clusterHostInfo']['ambari_server_host'][0]
    ambari_server_port = config['clusterHostInfo']['ambari_server_port'][0]
    ambari_server_use_ssl = config['clusterHostInfo']['ambari_server_use_ssl'][0] == 'true'
    ambari_server_protocol = 'https' if ambari_server_use_ssl else 'http'
    cluster_name = config['clusterName']
    ambari_server_auth_host_url = format('{ambari_server_protocol}://{ambari_server_host}:{ambari_server_port}')
    turn_on_url = ambari_server_auth_host_url + '/api/v1/clusters/' + cluster_name + '/services/OPENLDAP'
    data = '''{"RequestInfo": {"context": "Turn On Maintenance Mode for OPENLDAP"},"Body": {"ServiceInfo": {"maintenance_state": "ON"}}}'''
    Execute("curl -u 'ambari-qa:example.com' -H 'X-Requested-By:ambari' -i " + turn_on_url + " -X PUT -d '" + data + "'")


class Master(Script):
    def install(self, env):
        # Install packages listed in metainfo.xml
        self.install_packages(env)
        self.configure(env)
        import params

        service_packagedir = os.path.realpath(__file__).split('/scripts')[0]

        # Ensure the shell scripts in the services dir are executable
        Execute('find ' + service_packagedir +
                ' -iname "*.sh" | xargs chmod +x')

        Execute('echo "Running ' + service_packagedir + '/scripts/setup.sh"')

        # run setup script which has simple shell setup
        Execute(service_packagedir + '/scripts/setup.sh ' +
                params.ldap_password + ' ' + params.ldap_adminuser + ' ' +
                params.ldap_domain + ' ' + params.ldap_ldifdir + ' ' +
                params.ldap_ou + ' "' + params.binddn + '" >> ' + params.stack_log)

    def configure(self, env):
        import params
        env.set_params(params)

        content = Template('slapd.j2')
        File(
            format("/etc/openldap/slapd.conf"),
            content=content,
            owner='root',
            group='root',
            mode=0644)
        Execute('chkconfig slapd on')

    def stop(self, env):
        Logger.info(u"LDAP不能stop stop后会导致找不到用户!!!")
        Execute('service slapd stop')

    def start(self, env):
        self.configure(env)
        Execute('service slapd start')
        turn_on_maintenance()

    def restart(self, env):
        self.configure(env)
        self.stop(env)
        self.start(env)

    def status(self, env):
        Execute('service slapd status')

if __name__ == "__main__":
    Master().execute()
