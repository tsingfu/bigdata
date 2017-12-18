#!/usr/bin/python

import sys
import fileinput
import shutil
import os
from resource_management import *
from resource_management.core.resources.system import Execute
from resource_management import Directory


def install_spark(first=False):
    import params
    Directory(
        [params.spark_conf, params.spark_logs_dir],
        owner=params.spark_user,
        group=params.user_group,
        mode=0775,
        create_parents=True)
    if not os.path.exists('/opt/' + params.version_dir) or not os.path.exists(params.install_dir):
        Execute('rm -rf %s' % '/opt/' + params.version_dir)
        Execute('rm -rf %s' % params.install_dir)
        Execute(
            'wget ' + params.download_url + ' -O /tmp/' + params.filename,
            user=params.spark_user)
        Execute('tar -zxf /tmp/' + params.filename + ' -C /opt')
        Execute('ln -s /opt/' + params.version_dir + ' ' + params.install_dir)
        Execute(' rm -rf ' + params.install_dir + '/conf')
        Execute('ln -s ' + params.spark_conf + ' ' + params.install_dir +
                '/conf')
        Execute("echo 'export PATH=%s/bin:$PATH'>>/etc/profile.d/hadoop.sh" %
                params.install_dir)
        Execute('chown -R %s:%s /opt/%s' %
                (params.spark_user, params.user_group, params.version_dir))
        Execute('chown -R %s:%s %s' %
                (params.spark_user, params.user_group, params.install_dir))
        Execute('/bin/rm -f /tmp/' + params.filename)

    if params.hive_interactive_enabled and params.spark_llap_enabled:
        Execute('wget ' + params.spark_llap_jar_url + ' -O /' + params.install_dir + '/jars/',user=params.spark_user)

def setup_spark(env, type, upgrade_type=None, action=None, config_dir=None):
    """
  :param env: Python environment
  :param type: Spark component type
  :param upgrade_type: If in a stack upgrade, either UPGRADE_TYPE_ROLLING or UPGRADE_TYPE_NON_ROLLING
  :param action: Action to perform, such as generate configs
  :param config_dir: Optional config directory to write configs to.
  """

    import params

    if config_dir is None:
        config_dir = params.spark_conf

    Directory(
        [params.spark_pid_dir, params.spark_log_dir],
        owner=params.spark_user,
        group=params.user_group,
        mode=0775,
        create_parents=True)
    if type == 'server' and action == 'config':
        params.HdfsResource(
            params.spark_history_dir,
            type="directory",
            action="create_on_execute",
            owner=params.spark_user,
            mode=0775)
        params.HdfsResource(
            params.spark_hdfs_user_dir,
            type="directory",
            action="create_on_execute",
            owner=params.spark_user,
            mode=0775)
        params.HdfsResource(None, action="execute")

    PropertiesFile(
        os.path.join(config_dir, "spark-defaults.conf"),
        properties=params.config['configurations']['spark-defaults'],
        key_value_delimiter=" ", owner=params.spark_user,
        group=params.spark_group, mode=0644)

    # create spark-env.sh in etc/conf dir
    File(
        os.path.join(config_dir, 'spark-env.sh'),
        owner=params.spark_user,
        group=params.spark_group,
        content=InlineTemplate(params.spark_env_sh),
        mode=0644, )

    # create log4j.properties in etc/conf dir
    File(
        os.path.join(config_dir, 'log4j.properties'),
        owner=params.spark_user,
        group=params.spark_group,
        content=params.spark_log4j_properties,
        mode=0644, )

    # create metrics.properties in etc/conf dir
    File(
        os.path.join(config_dir, 'metrics.properties'),
        owner=params.spark_user,
        group=params.spark_group,
        content=InlineTemplate(params.spark_metrics_properties),
        mode=0644)

    if params.is_hive_installed:
        XmlConfig(
            "hive-site.xml",
            conf_dir=config_dir,
            configurations=params.spark_hive_properties,
            owner=params.spark_user,
            group=params.spark_group,
            mode=0644)

    if params.has_spark_thriftserver:
        PropertiesFile(
            params.spark_thrift_server_conf_file,
            properties=params.config['configurations'][
                'spark-thrift-sparkconf'], owner=params.hive_user,
            group=params.user_group, key_value_delimiter=" ", mode=0644)

    File(
        os.path.join(params.spark_conf, 'java-opts'),
        owner=params.spark_user,
        group=params.spark_group,
        content=InlineTemplate(params.spark_javaopts_properties),
        mode=0644)
    # File(os.path.join(params.spark_conf, 'java-opts'), action="delete")

    if params.spark_thrift_fairscheduler_content:
        # create spark-thrift-fairscheduler.xml
        File(
            os.path.join(config_dir, "spark-thrift-fairscheduler.xml"),
            owner=params.spark_user,
            group=params.spark_group,
            mode=0755,
            content=InlineTemplate(params.spark_thrift_fairscheduler_content))
