#!/usr/bin/python
"""
Eagle Ambari Command
"""
from resource_management import *
from resource_management.core.resources.system import Directory, Execute, File
import os
from resource_management.core.source import InlineTemplate
from resource_management.libraries import XmlConfig

def install_eagle():
    import params
    if not os.path.exists('/opt/' + params.version_dir) or not os.path.exists(params.install_dir):
        Execute('rm -rf %s' %  '/opt/' + params.version_dir)
        Execute('rm -rf %s' % params.install_dir)
        Execute(
            'wget ' + params.download_url + ' -O /tmp/' + params.filename,
            user=params.eagle_user)
        Execute('tar -zxf /tmp/' + params.filename + ' -C /opt')
        Execute('ln -s /opt/' + params.version_dir + ' ' + params.install_dir)
        Execute(' rm -rf ' + params.install_dir + '/conf')
        Execute(' mkdir -p ' + params.eagle_conf)
        Execute('ln -s ' + params.eagle_conf + ' ' + params.install_dir +
                '/conf')
        Execute("echo 'export PATH=%s/bin:$PATH'>>/etc/profile.d/hadoop.sh" %
                params.install_dir)
        Execute('chown -R %s:%s /opt/%s' %
                (params.eagle_user, params.user_group, params.version_dir))
        Execute('chown -R %s:%s %s' %
                (params.eagle_user, params.user_group, params.install_dir))
        Execute('/bin/rm -f /tmp/' + params.filename)

def config_eagle():
    import params
    Directory(
        [params.eagle_log_dir, params.eagle_conf],
        owner=params.eagle_user,
        group=params.user_group,
        cd_access="a",
        create_parents=True,
        mode=0755)
    File(
        os.path.join(params.install_dir, "bin/eagle-env.sh"),
        mode=0755,
        content=InlineTemplate(params.eagle_env_content),
        owner=params.eagle_user,
        group=params.user_group)
    File(
        os.path.join(params.eagle_conf, "log4j.properties"),
        mode=0644,
        owner=params.eagle_user,
        group=params.user_group,
        content=InlineTemplate(params.log4j_content))
    File(
        os.path.join(params.eagle_conf, "application.conf"),
        mode=0644,
        owner=params.eagle_user,
        group=params.user_group,
        content=InlineTemplate(params.application_content))
    File(
        os.path.join(params.eagle_conf, "eagle-service.conf"),
        mode=0644,
        owner=params.eagle_user,
        group=params.user_group,
        content=InlineTemplate(params.eagle_service_content))
    File(
        os.path.join(params.eagle_conf, "eagle-scheduler.conf"),
        mode=0644,
        owner=params.eagle_user,
        group=params.user_group,
        content=InlineTemplate(params.eagle_scheduler_content))
    File(
        os.path.join(params.eagle_conf, "kafka-server.properties"),
        mode=0644,
        owner=params.eagle_user,
        group=params.user_group,
        content=InlineTemplate(params.kafka_server_content))

def eagle_service_exec(action="start"):
    import params

    eagle_service_init_shell = format("{eagle_bin}/eagle-service-init.sh")
    eagle_service_shell = format("{eagle_bin}/eagle-service.sh")

    if action == "start":
        start_cmd = format("{eagle_service_shell} start")
        Execute(start_cmd, user=params.eagle_user)
    elif action == "stop":
        stop_cmd = format("{eagle_service_shell} stop")
        Execute(stop_cmd, user=params.eagle_user)
    elif action == "status":
        status_cmd = format("{eagle_service_shell} status")
        Execute(status_cmd, user=params.eagle_user)
    elif action == "restart":
        status_cmd = format("{eagle_service_shell} restart")
        Execute(status_cmd, user=params.eagle_user)
    elif action == "init":
        Execute(eagle_service_init_shell, user=params.eagle_user)
    else:
        raise Exception('Unknown eagle service action: ' + action)


def eagle_topology_exec(action="start"):
    import params

    eagle_topology_shell = format("{eagle_bin}/eagle-topology.sh")
    eagle_topology_init = format("{eagle_bin}/eagle-topology-init.sh")

    if action == "start":
        start_cmd = format("{eagle_topology_shell} start")
        Execute(start_cmd, user=params.eagle_user)
    elif action == "stop":
        stop_cmd = format("{eagle_topology_shell} stop")
        Execute(stop_cmd, user=params.eagle_user)
    elif action == "status":
        status_cmd = format("{eagle_topology_shell} --storm-ui status")
        Execute(status_cmd, user=params.eagle_user)
    elif action == "init":
        Execute(eagle_topology_init, user=params.eagle_user)
    else:
        raise Exception('Unknown eagle topology action: ' + action)


def eagle_hive_topology_exec(action="start"):
    import params

    main_class = "org.apache.eagle.security.hive.jobrunning.HiveJobRunningMonitoringMain"
    topology_name = format("{eagle_site}-hiveQueryRunningTopology")
    config_file = format(
        "{eagle_conf}/{eagle_site}-hiveQueryLog-application.conf")
    eagle_topology_shell = format("{eagle_bin}/eagle-topology.sh")
    eagle_topology_init = format("{eagle_bin}/eagle-topology-init.sh")

    cmd = None

    if action == "start":
        cmd = format(
            "{eagle_topology_shell} --main {main_class} --topology {topology_name} --config {config_file} start")
    elif action == "stop":
        cmd = format("{eagle_topology_shell} --topology {topology_name} stop")
    elif action == "status":
        cmd = format(
            "{eagle_topology_shell} --topology {topology_name} --storm-ui status")
    elif action == "init":
        cmd = eagle_topology_init

    if cmd != None:
        Execute(cmd, user=params.eagle_user)
    else:
        raise Exception('Unknown eagle hive topology action: ' + action)


def eagle_hdfs_topology_exec(action="start"):
    import params

    main_class = "org.apache.eagle.security.auditlog.HdfsAuditLogProcessorMain"
    topology_name = format("{eagle_site}-hdfsAuditLog-topology")
    config_file = format(
        "{eagle_conf}/{eagle_site}-hdfsAuditLog-application.conf")

    eagle_topology_shell = format("{eagle_bin}/eagle-topology.sh")
    eagle_topology_init = format("{eagle_bin}/eagle-topology-init.sh")

    cmd = None
    if action == "start":
        cmd = format(
            "{eagle_topology_shell} --main {main_class} --topology {topology_name} --config {config_file} start")
    elif action == "stop":
        cmd = format("{eagle_topology_shell} --topology {topology_name} stop")
    elif action == "status":
        cmd = format(
            "{eagle_topology_shell} --topology {topology_name} --storm-ui status")
    elif action == "init":
        cmd = eagle_topology_init

    if cmd != None:
        Execute(cmd, user=params.eagle_user)
    else:
        raise Exception('Unknow eagle hdfs topology action: ' + action)


def eagle_userprofile_topology_exec(action="start"):
    import params

    main_class = "org.apache.eagle.security.userprofile.UserProfileDetectionMain"
    topology_name = format("{eagle_site}-userprofile-topology")
    config_file = format("{eagle_conf}/{eagle_site}-userprofile-topology.conf")

    eagle_topology_shell = format("{eagle_bin}/eagle-topology.sh")
    eagle_topology_init = format("{eagle_bin}/eagle-topology-init.sh")

    cmd = None
    if action == "start":
        cmd = format(
            "{eagle_topology_shell} --main {main_class} --topology {topology_name} --config {config_file} start")
    elif action == "stop":
        cmd = format("{eagle_topology_shell} --topology {topology_name} stop")
    elif action == "status":
        cmd = format(
            "{eagle_topology_shell} --topology {topology_name} --storm-ui status")
    elif action == "init":
        cmd = eagle_topology_init

    if cmd != None:
        Execute(cmd, user=params.eagle_user)
    else:
        raise Exception('Unknow eagle userprofile topology action: ' + action)


def eagle_userprofile_scheduler_exec(action="start"):
    import params
    userprofile_scheduler_shell = format(
        "{eagle_bin}/eagle-userprofile-scheduler.sh --site {params.eagle_site}")
    if action == "start":
        Execute(
            format("{userprofile_scheduler_shell} --daemon {action}"),
            user=params.eagle_user)
    elif action == "stop" or action == "status":
        Execute(
            format("{userprofile_scheduler_shell} {action}"),
            user=params.eagle_user)
    else:
        raise Exception("known eagle user profile scheduler action: " + action)
