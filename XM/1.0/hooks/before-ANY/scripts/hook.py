# encoding=utf8
from resource_management import *
from shared_initialization import *
from resource_management.core.resources.system import Execute, File
from resource_management.core.logger import Logger
from resource_management.core.resources.system import Execute
from resource_management.libraries.functions import default
from resource_management.core.exceptions import Fail


def ldap_client_conf():
    ldap_url = ''
    basedn = default('openldap-config/ldap.domain', 'dc=example,dc=com')
    ldap_hosts = default('clusterHostInfo/openldap_master_hosts', [])
    ldap_hosts_input = default('configurations/zookeeper-env/ldap_hosts', '')
    if ldap_hosts_input.strip() != '':
        ldap_hosts = ldap_hosts_input.split(' ')
        ldap_url = ['ldap://' + item + '/' for item in ldap_hosts]
        ldap_url = ' '.join(ldap_url)
    elif len(ldap_hosts) > 0:
        ldap_url = ['ldap://' + item + '/' for item in ldap_hosts]
        ldap_url = ' '.join(ldap_url)
    if len(ldap_url) > 0:
        Execute("mkdir -p /etc/openldap/cacerts")
        Execute(
            '/usr/sbin/authconfig --enablekrb5 --enableshadow --useshadow --enablelocauthorize --enableldap --enableldapauth --ldapserver="' + ldap_url + '" --ldapbasedn="' + basedn + '" --update')
    else:
        raise Fail(u'ldap地址为空 请先填写ldap地址 或 安装ldap')


from resource_management.core.resources.system import Directory
from resource_management.core.resources.system import Execute
import time


def backup_keytab():
    backup_dir = '/data/backup/keytab/' + time.strftime('%Y%m%d_%H%M')
    Directory(backup_dir,
              create_parents=True,
              owner='root',
              group='root')
    Execute('cp -rpf /etc/security/keytabs ' + backup_dir)


class BeforeAnyHook(Hook):
    def hook(self, env):
        import params
        env.set_params(params)

        try:
            backup_keytab()
            Execute('service nslcd start')
        except Exception as e:
            print 'backup fail'

        if not os.path.exists('/etc/yum.repos.d/hadoop.repo'):
            Logger.info("get repo from yum.example.com")
            Execute('rm -rf /etc/yum.repos.d/*')
            Execute('wget http://yum.example.com/hadoop/hadoop.repo -O /etc/yum.repos.d/hadoop.repo')
            Execute('yum clean all')

        ldap_client_conf()

        setup_users()
        if params.has_namenode or params.dfs_type == 'HCFS':
            setup_hadoop_env()
            # setup_java()


if __name__ == "__main__":
    BeforeAnyHook().execute()
