#!/usr/bin/env python

from resource_management.libraries.resources import HdfsResource
from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.format import format
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions import get_kinit_path
from resource_management.libraries.functions.get_not_managed_resources import get_not_managed_resources

# server configurations
config = Script.get_config()
stack_root = '/opt'

install_dir = config['configurations']['slider-env']['install_dir']
download_url = config['configurations']['slider-env']['download_url']
filename = download_url.split('/')[-1]
version_dir = filename.replace('.tar.gz', '').replace('.tgz', '')

slider_home_dir = install_dir

# hadoop params
slider_bin_dir = format('{slider_home_dir}/bin')

slider_conf_dir = '/etc/slider'

slider_lib_dir = format('{slider_home_dir}/lib')
slider_tar_gz = '/tmp/' + filename

user_group = config['configurations']['cluster-env']['user_group']

kinit_path_local = get_kinit_path(default('/configurations/kerberos-env/executable_search_paths', None))
security_enabled = config['configurations']['cluster-env']['security_enabled']
hdfs_user = config['configurations']['hadoop-env']['hdfs_user']
hdfs_principal_name = config['configurations']['hadoop-env']['hdfs_principal_name']
hdfs_user_keytab = config['configurations']['hadoop-env']['hdfs_user_keytab']

hadoop_bin_dir = config['configurations']['hadoop-env']['install_dir'] + '/bin'
hadoop_conf_dir = '/etc/hadoop'

hdfs_site = config['configurations']['hdfs-site']
default_fs = config['configurations']['core-site']['fs.defaultFS']

import functools

HdfsResource = functools.partial(
    HdfsResource,
    user=hdfs_user,
    hdfs_resource_ignore_file="/var/lib/ambari-agent/data/.hdfs_resource_ignore",
    security_enabled=security_enabled,
    keytab=hdfs_user_keytab,
    kinit_path_local=kinit_path_local,
    hadoop_bin_dir=hadoop_bin_dir,
    hadoop_conf_dir=hadoop_conf_dir,
    principal_name=hdfs_principal_name,
    hdfs_site=hdfs_site,
    default_fs=default_fs,
    immutable_paths=get_not_managed_resources()
)

stack_name = default("/hostLevelParams/stack_name", None)

version = default("/commandParams/version", None)

smokeuser = config['configurations']['cluster-env']['smokeuser']
smokeuser_principal = config['configurations']['cluster-env']['smokeuser_principal_name']
security_enabled = config['configurations']['cluster-env']['security_enabled']
smokeuser_keytab = config['configurations']['cluster-env']['smokeuser_keytab']
kinit_path_local = get_kinit_path(default('/configurations/kerberos-env/executable_search_paths', None))
slider_env_sh_template = config['configurations']['slider-env']['content']

java64_home = config['hostLevelParams']['java_home']
log4j_props = config['configurations']['slider-log4j']['content']
slider_cmd = format("{slider_bin_dir}/slider")
