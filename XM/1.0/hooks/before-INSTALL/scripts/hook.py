import sys, os
from resource_management import *
from shared_initialization import *
from repo_initialization import *
from resource_management.core.resources.jcepolicyinfo import JcePolicyInfo
from resource_management.core.logger import Logger
from resource_management import Directory, File, PropertiesFile, InlineTemplate, format

from resource_management.core.resources.system import Execute
import urllib2
import json
import socket

CONST_RACK_INFO_URL = 'http://cmdb.example.com/open/rack?ip='


def is_gpu_machine():
    return os.path.exists('/dev/nvidiactl')


def install_gpu_stack():
    if is_gpu_machine():
        if not os.path.exists('/opt/intel/daal'):
            Execute(
                "wget http://yum.example.com/hadoop/intel/l_daal_2018.0.128.tgz -O /tmp/l_daal_2018.0.128.tgz && cd /tmp && tar -zxvf /tmp/l_daal_2018.0.128.tgz && cd /tmp/l_daal_2018.0.128 && ./install.sh -s silent.cfg && rm -rf /tmp/l_daal_*")
        if not os.path.exists('/opt/intel/mkl'):
            Execute(
                "wget http://yum.example.com/hadoop/intel/l_mkl_2018.0.128.tgz -O /tmp/l_mkl_2018.0.128.tgz && cd /tmp && tar -zxvf /tmp/l_mkl_2018.0.128.tgz && cd /tmp/l_mkl_2018.0.128 && ./install.sh -s silent.cfg && rm -rf /tmp/l_mkl_*")
        if not os.path.exists('/opt/intel/ipp'):
            Execute(
                "wget http://yum.example.com/hadoop/intel/l_ipp_2018.0.128.tgz -O /tmp/l_ipp_2018.0.128.tgz && cd /tmp && tar -zxvf /tmp/l_ipp_2018.0.128.tgz && cd /tmp/l_ipp_2018.0.128 && ./install.sh -s silent.cfg && rm -rf /tmp/l_ipp*")
        if not os.path.exists('/opt/intel/mkl/wrapper/mkl_wrapper.jar'):
            Execute('mkdir -p /opt/intel/mkl/wrapper && wget http://yum.example.com/hadoop/intel/mkl_wrapper.jar -O /opt/intel/mkl/wrapper/mkl_wrapper.jar')
        if not os.path.exists('/opt/intel/mkl/wrapper/mkl_wrapper.so'):
            Execute('mkdir -p /opt/intel/mkl/wrapper && wget http://yum.example.com/hadoop/intel/mkl_wrapper.so -O /opt/intel/mkl/wrapper/mkl_wrapper.so')
        if not os.path.exists('/opt/intel/intelpython2'):
            Execute(
                "wget http://yum.example.com/hadoop/intel/l_python2_p_2018.0.018.tgz -O /tmp/l_python2_p_2018.0.018.tgz && cd /tmp && tar -zxvf /tmp/l_python2_p_2018.0.018.tgz && cd /tmp/l_python2_p_2018.0.018 && ./install.sh -s silent.cfg && rm -rf /tmp/l_python2_*")


def modify_rack_awareness():
    import params
    hostname = params.config["hostname"].lower()
    ip = socket.gethostbyname(socket.gethostname())
    try:
        response = urllib2.urlopen(CONST_RACK_INFO_URL + ip)
        rack_info = response.read()
        rack_info = json.loads(rack_info)
        if isinstance(rack_info, list):
            topology_item = '/default/default/default'
        else:
            datacenter = rack_info.get(ip).get('datacenter') or 'default'
            switch = rack_info.get(ip).get('switch') or 'default'
            rack = rack_info.get(ip).get('rack') or 'default'
            topology_item = '/' + datacenter.replace('/', '').lower() + '/' + switch.replace('/',
                                                                                             '').lower() + '/' + rack.replace(
                '/', '').lower()

        ambari_server_host = params.config['clusterHostInfo']['ambari_server_host'][0]
        ambari_server_port = params.config['clusterHostInfo']['ambari_server_port'][0]
        ambari_server_use_ssl = params.config['clusterHostInfo']['ambari_server_use_ssl'][0] == 'true'
        ambari_server_protocol = 'https' if ambari_server_use_ssl else 'http'
        cluster_name = params.config['clusterName']
        ambari_server_auth_host_url = format('{ambari_server_protocol}://{ambari_server_host}:{ambari_server_port}')
        rack_update_url = ambari_server_auth_host_url + '/api/v1/clusters/' + cluster_name + '/hosts'

        rack_update = '{"RequestInfo":{"context":"Set Rack","query":"Hosts/host_name.in(' + hostname + ')"},"Body":{"Hosts":{"rack_info":"' + topology_item + '"}}}'
        Execute(
            "curl -u 'ambari-qa:example.com' -H 'X-Requested-By:ambari' -i " + rack_update_url + " -X PUT -d '" + rack_update + "'")
    except urllib2.HTTPError as e:
        print "can not get %s rack_info from cmdb" % ip


def java_version():
    return 1,8
    import params
    import subprocess
    proc = subprocess.Popen([params.java_home + '/bin/java', '-version'], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    java_version = proc.communicate()[1].split('\n')[0]

    version_number = java_version.split()[-1].strip('"')
    major, minor, _ = version_number.split('.')
    Logger.debug("java version is : {0}".format(minor))
    return int(major), int(minor)


def install_jce_policy():
    import params
    jcePolicyInfo = JcePolicyInfo(params.java_home)

    if jcePolicyInfo.is_unlimited_key_jce_policy():
        major, minor = java_version()
        Logger.info("The unlimited key JCE policy is required, and appears to have been installed.")
        java_security_dir = format("{java_home}/jre/lib/security")
        Logger.debug("Removing existing JCE policy JAR files: {0}.".format(java_security_dir))
        File(format("{java_security_dir}/US_export_policy.jar"), action="delete")
        File(format("{java_security_dir}/local_policy.jar"), action="delete")
        src_url = jce_zip_target = ''
        if minor == 7:  # jdk7
            src_url = 'http://yum.example.com/hadoop/UnlimitedJCEPolicyJDK7.zip'
            jce_zip_target = '/tmp/UnlimitedJCEPolicyJDK7.zip'
        elif minor == 8:  # jdk8
            src_url = 'http://yum.example.com/hadoop/jce_policy-8.zip'
            jce_zip_target = '/tmp/jce_policy-8.zip'

        if src_url and jce_zip_target:
            Execute('wget ' + src_url + ' -O ' + jce_zip_target)
            Logger.debug(
                "Unzipping the unlimited key JCE policy files from {0} into {1}.".format(jce_zip_target,
                                                                                         java_security_dir))
            extract_cmd = ("unzip", "-o", "-j", "-q", jce_zip_target, "-d", java_security_dir)
            Execute(extract_cmd,
                    only_if=format("test -e {java_security_dir} && test -f {jce_zip_target}"),
                    path=['/bin/', '/usr/bin'],
                    sudo=True
                    )


def link_libjvm():
    if not os.path.exists('/lib64/libjvm.so'):
        if os.path.exists('/usr/java/default/jre/lib/amd64/server/libjvm.so'):
            Execute('ln -s /usr/java/default/jre/lib/amd64/server/libjvm.so /lib64/libjvm.so')


class BeforeInstallHook(Hook):
    def hook(self, env):
        import params

        self.run_custom_hook('before-ANY')
        env.set_params(params)

        install_mysql_connector_java()

        install_repos()
        install_packages()
        ldap_client_conf()

        try:
            Execute('service nslcd start')
        except Exception as e:
            print 'nslcd restart error'

        link_libjvm()
        install_jce_policy()

        # hadoop rack awareness
        try:
            modify_rack_awareness()
        except Exception as e:
            Logger.debug(str(e))

        try:
            install_gpu_stack()
        except Exception as e:
            Logger.debug(str(e))


if __name__ == "__main__":
    BeforeInstallHook().execute()
