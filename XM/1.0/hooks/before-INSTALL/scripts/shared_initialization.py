# encoding=utf8
import os

from resource_management.core.resources.packaging import Package
from resource_management.core.resources.system import Execute
from resource_management.libraries.functions import default
from resource_management.core.exceptions import Fail


def install_mysql_connector_java():
    mysql_connector_jar = 'mysql-connector-java.jar'
    mysql_jar_url = 'http://yum.example.com/hadoop/mysql-connector-java-5.1.40-bin.jar'
    if not os.path.exists('/usr/share/java/%s' % mysql_connector_jar):
        Execute("wget %s -O /usr/share/java/%s" %
                (mysql_jar_url, mysql_connector_jar))
        Execute("echo 'export CLASSPATH=$CLASSPATH:/usr/share/java/%s'>>/etc/profile.d/java.sh"
                % mysql_connector_jar)


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


def kerberos_client_conf():
    kerberos_host = default('clusterHostInfo/krb5_master_hosts', [])
    realm = default('configurations/krb5-config/kdc.realm', 'example.com')
    kdc_hosts = default('configurations/zookeeper-env/kdc_hosts', '')
    if kdc_hosts.strip() != '':
        Execute(
            '/usr/sbin/authconfig --enablekrb5 --krb5kdc="' + kdc_hosts + '"  --krb5adminserver="' + kdc_hosts + '"  --krb5realm="' + realm + '"  --update')
    elif len(kerberos_host) > 0:
        Execute('/usr/sbin/authconfig --enablekrb5 --krb5kdc="' + ' '.join(
            kerberos_host) + '"  --krb5adminserver="' + ' '.join(
            kerberos_host) + '"  --krb5realm="' + realm + '"  --update')
    else:
        raise Fail(u'ldap地址为空 请先填写KDC地址或 安装KDC')


def install_packages():
    import params
    if params.host_sys_prepped:
        return
    packages = ['unzip', 'curl', 'jdk', 'openssl-devel', 'openldap-clients', 'nss-pam-ldapd', 'pam_ldap',
                'pam_krb5', 'authconfig', 'lzo', 'krb5-workstation',
                'krb5-libs', 'libcgroup', 'lz4', 'lz4-devel', 'zstd-devel', 'libisal']  # , 'java-1.8.0-openjdk-devel']
    Package(
        packages,
        retry_on_repo_unavailability=params.
            agent_stack_retry_on_unavailability,
        retry_count=params.agent_stack_retry_count)
    Execute(" chkconfig nslcd on")
    Execute(" chkconfig nscd on")
    if not os.path.exists('/cgroups'):
        try:
            Execute('umount /sys/fs/cgroup/cpu,cpuacct')
            Execute('mkdir -p /cgroups/cpu /cgroups/cpuacct')
            Execute('mount -t cgroup -o rw,nosuid,nodev,noexec,relatime,cpu cgroup /cgroups/cpu')
            Execute('mount -t cgroup -o rw,nosuid,nodev,noexec,relatime,cpuacct cgroup /cgroups/cpuacct')
        except Exception as e:
            print "mount cgroups error"
    if not os.path.exists('/cgroups/cpu'):
        Execute('mkdir -p /cgroups/cpu')
