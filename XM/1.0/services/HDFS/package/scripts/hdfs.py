
from resource_management.libraries.script.script import Script
from resource_management.core.resources.system import Directory, File, Link
from resource_management.core.resources import Package
from resource_management.core.source import Template
from resource_management.core.resources.service import ServiceConfig
from resource_management.libraries.resources.xml_config import XmlConfig
import os

from resource_management.core.resources.system import Execute


def copy_to_hdfs(name, user_group, owner, custom_source_file=None, custom_dest_file=None):
    import os
    import params
    dest_dir = os.path.dirname(custom_dest_file)
    params.HdfsResource(dest_dir,
                        type="directory",
                        action="create_on_execute",
                        owner=owner,
                        mode=0555
                        )

    params.HdfsResource(custom_dest_file,
                        type="file",
                        action="create_on_execute",
                        source=custom_source_file,
                        group=user_group,
                        owner=owner,
                        mode=0444,
                        replace_existing_files=False,
                        )
    params.HdfsResource(None, action="execute")

def install_hadoop(first=False,is_master=False):
    import params
    Directory(
        params.hdfs_log_dir,
        owner=params.hdfs_user,
        group=params.user_group,
        create_parents=True,
        mode=0755)

    Directory(
        params.limits_conf_dir,
        create_parents=True,
        owner='root',
        group='root')

    Directory(
        params.hadoop_conf_dir,
        create_parents=True,
        owner='root',
        group='root')

    if not os.path.exists('/opt/' + params.version_dir) or not os.path.exists(params.install_dir):
        Execute('rm -rf %s' %  '/opt/' + params.version_dir)
        Execute('rm -rf %s' % params.install_dir)
        Execute('/bin/rm -f /tmp/' + params.filename)
        Execute(
            'wget ' + params.download_url + ' -O /tmp/' + params.filename,
            user=params.hdfs_user)
        Execute('tar -zxf /tmp/' + params.filename + ' -C /opt')
        Execute('ln -s /opt/' + params.version_dir + ' ' + params.install_dir)
        Execute(' rm -rf ' + params.install_dir + '/etc/hadoop')
        Execute('ln -s ' + params.hadoop_conf_dir + ' ' + params.install_dir +
                '/etc/hadoop')
        Execute('mkdir ' + params.install_dir + '/logs && chmod 777 ' +
                params.install_dir + '/logs')
        Execute("echo 'export PATH=%s/bin:$PATH'>>/etc/profile.d/hadoop.sh" %
                params.install_dir)
        Execute('chown -R %s:%s /opt/%s' %
                (params.hdfs_user, params.user_group, params.version_dir))
        Execute('chown -R %s:%s %s' %
                (params.hdfs_user, params.user_group, params.install_dir))
        Execute('chown root:%s %s/bin/container-executor' %(params.user_group,params.install_dir))
        if is_master:
            copy_to_hdfs("mapreduce", params.user_group, params.hdfs_user,
                                            custom_source_file='/tmp/' + params.filename,
                                            custom_dest_file='/apps/mapreduce/' + params.filename)
            params.HdfsResource(None, action="execute")
        Execute('/bin/rm -f /tmp/' + params.filename)


def hdfs(name=None):
    import params

    # On some OS this folder could be not exists, so we will create it before pushing there files
    Directory(
        params.limits_conf_dir,
        create_parents=True,
        owner='root',
        group='root')

    Directory(
        params.hadoop_conf_dir,
        create_parents=True,
        owner='root',
        group='root')

    File(
        os.path.join(params.limits_conf_dir, 'hdfs.conf'),
        owner='root',
        group='root',
        mode=0644,
        content=Template("hdfs.conf.j2"))

    if params.security_enabled:
        tc_mode = 0644
        tc_owner = "root"
    else:
        tc_mode = None
        tc_owner = params.hdfs_user

    if "hadoop-policy" in params.config['configurations']:
        XmlConfig(
            "hadoop-policy.xml",
            conf_dir=params.hadoop_conf_dir,
            configurations=params.config['configurations']['hadoop-policy'],
            configuration_attributes=params.config['configuration_attributes']
            ['hadoop-policy'], owner=params.hdfs_user, group=params.user_group)

    if "ssl-client" in params.config['configurations']:
        XmlConfig(
            "ssl-client.xml",
            conf_dir=params.hadoop_conf_dir,
            configurations=params.config['configurations']['ssl-client'],
            configuration_attributes=params.config['configuration_attributes'][
                'ssl-client'], owner=params.hdfs_user, group=params.user_group)

        Directory(
            params.hadoop_conf_secure_dir,
            create_parents=True,
            owner='root',
            group=params.user_group,
            cd_access='a', )

        XmlConfig(
            "ssl-client.xml",
            conf_dir=params.hadoop_conf_secure_dir,
            configurations=params.config['configurations']['ssl-client'],
            configuration_attributes=params.config['configuration_attributes'][
                'ssl-client'], owner=params.hdfs_user, group=params.user_group)

    if "ssl-server" in params.config['configurations']:
        XmlConfig(
            "ssl-server.xml",
            conf_dir=params.hadoop_conf_dir,
            configurations=params.config['configurations']['ssl-server'],
            configuration_attributes=params.config['configuration_attributes'][
                'ssl-server'], owner=params.hdfs_user, group=params.user_group)

    XmlConfig(
        "hdfs-site.xml",
        conf_dir=params.hadoop_conf_dir,
        configurations=params.config['configurations']['hdfs-site'],
        configuration_attributes=params.config['configuration_attributes'][
            'hdfs-site'], owner=params.hdfs_user, group=params.user_group)

    XmlConfig(
        "core-site.xml",
        conf_dir=params.hadoop_conf_dir,
        configurations=params.config['configurations']['core-site'],
        configuration_attributes=params.config['configuration_attributes'][
            'core-site'], owner=params.hdfs_user, group=params.user_group,
        mode=0644)

    File(
        os.path.join(params.hadoop_conf_dir, 'slaves'),
        owner=tc_owner,
        content=Template("slaves.j2"))