#!/usr/bin/env python

import os
import sys,glob
from urlparse import urlparse

from resource_management.core.resources.system import File, Execute, Directory
from resource_management.core.source import StaticFile, Template, DownloadSource, InlineTemplate
from resource_management.core.shell import as_user
from resource_management.libraries.functions.is_empty import is_empty
from resource_management.libraries.resources.xml_config import XmlConfig
from resource_management.libraries.functions.format import format
from resource_management.core.shell import quote_bash_args
from resource_management.core.logger import Logger
from resource_management.core import utils
from resource_management.libraries.functions.security_commons import update_credential_provider_path
from ambari_commons.constants import SERVICE

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

def install_hive():
    import params
    Directory(
        [params.config_dir, params.hcat_conf_dir],
        owner=params.hive_user,
        group=params.user_group,
        mode=0775,
        create_parents=True)
    if not os.path.exists('/opt/' + params.version_dir) or not os.path.exists(params.install_dir):
        Execute('rm -rf %s' % '/opt/' + params.version_dir)
        Execute('rm -rf %s' % params.install_dir)
        Execute('/bin/rm -f /tmp/' + params.filename)
        Execute(
            'wget ' + params.download_url + ' -O /tmp/' + params.filename,
            user=params.hive_user)
        Execute('tar -zxf /tmp/' + params.filename + ' -C /opt')
        Execute('ln -s /opt/' + params.version_dir + ' ' + params.install_dir)
        Execute(' rm -rf ' + params.install_dir + '/conf')
        Execute('ln -s ' + params.config_dir + ' ' + params.install_dir +
                '/conf')
        Execute("echo 'export PATH=%s/bin:$PATH'>>/etc/profile.d/hadoop.sh" %
                params.install_dir)
        Execute('chown -R %s:%s /opt/%s' %
                (params.hive_user, params.user_group, params.version_dir))
        Execute('chown -R %s:%s %s' %
                (params.hive_user, params.user_group, params.install_dir))

        Execute('chmod 754 ' + params.hive_bin + '/hive')

        copy_to_hdfs("hive", params.user_group, params.hdfs_user,
                                        custom_source_file='/tmp/' + params.filename,
                                        custom_dest_file='/apps/mapreduce/' + params.filename)
        params.HdfsResource(None, action="execute")
        Execute('/bin/rm -f /tmp/' + params.filename)

        Execute('wget http://yum.example.com/hadoop/hdfs/tez.tar.gz -O ' + params.tez_tar_source)
        Execute('wget http://yum.example.com/hadoop/hdfs/pig.tar.gz -O ' + params.pig_tar_source)
        Execute('wget http://yum.example.com/hadoop/hdfs/hive.tar.gz -O ' + params.hive_tar_source)
        copy_to_hdfs("tez", params.user_group, params.hdfs_user,
                                        custom_source_file=params.tez_tar_source,
                                        custom_dest_file=params.tez_tar_dest_file)
        params.HdfsResource(None, action="execute")
        copy_to_hdfs("pig",
                                        params.user_group,
                                        params.hdfs_user,
                                        custom_source_file=params.pig_tar_source,
                                        custom_dest_file=params.pig_tar_dest_file)
        params.HdfsResource(None, action="execute")
        copy_to_hdfs("hive",
                                        params.user_group,
                                        params.hdfs_user,
                                        custom_source_file=params.hive_tar_source,
                                        custom_dest_file=params.hive_tar_dest_file)
        params.HdfsResource(None, action="execute")

        Execute('/bin/rm -f ' + params.tez_tar_source)
        Execute('/bin/rm -f ' + params.pig_tar_source)
        Execute('/bin/rm -f ' + params.hive_tar_source)



def hive(name=None):
    import params

    hive_client_conf_path = '/etc/hive'
    # Permissions 644 for conf dir (client) files, and 600 for conf.server
    mode_identified = 0644 if params.hive_config_dir == hive_client_conf_path else 0600

    Directory(params.hive_etc_dir_prefix,
              mode=0755
              )

    # We should change configurations for client as well as for server.
    # The reason is that stale-configs are service-level, not component.
    Logger.info("Directories to fill with configs: %s" % str(params.hive_conf_dirs_list))
    for conf_dir in params.hive_conf_dirs_list:
        fill_conf_dir(conf_dir)

    params.hive_site_config = update_credential_provider_path(params.hive_site_config,
                                                              'hive-site',
                                                              os.path.join(params.hive_conf_dir, 'hive-site.jceks'),
                                                              params.hive_user,
                                                              params.user_group
                                                              )
    XmlConfig("hive-site.xml",
              conf_dir=params.hive_config_dir,
              configurations=params.hive_site_config,
              configuration_attributes=params.config['configuration_attributes']['hive-site'],
              owner=params.hive_user,
              group=params.user_group,
              mode=mode_identified)

    # Generate atlas-application.properties.xml file
    if params.enable_atlas_hook:
        script_path = os.path.realpath(__file__).split('/services')[0] + '/hooks/before-INSTALL/scripts/atlas'
        sys.path.append(script_path)
        from setup_atlas_hook import has_atlas_in_cluster, setup_atlas_hook, setup_atlas_jar_symlinks
        atlas_hook_filepath = os.path.join(params.hive_config_dir, params.atlas_hook_filename)
        setup_atlas_hook(SERVICE.HIVE, params.hive_atlas_application_properties, atlas_hook_filepath, params.hive_user,
                         params.user_group)
        setup_atlas_jar_symlinks("hive", params.hive_lib)

    File(format("{hive_config_dir}/hive-env.sh"),
         owner=params.hive_user,
         group=params.user_group,
         content=InlineTemplate(params.hive_env_sh_template),
         mode=mode_identified
         )

    # On some OS this folder could be not exists, so we will create it before pushing there files
    Directory(params.limits_conf_dir,
              create_parents=True,
              owner='root',
              group='root'
              )

    File(os.path.join(params.limits_conf_dir, 'hive.conf'),
         owner='root',
         group='root',
         mode=0644,
         content=Template("hive.conf.j2")
         )
    if params.security_enabled:
        File(os.path.join(params.hive_config_dir, 'zkmigrator_jaas.conf'),
             owner=params.hive_user,
             group=params.user_group,
             content=Template("zkmigrator_jaas.conf.j2")
             )

    File(format("/usr/lib/ambari-agent/{check_db_connection_jar_name}"),
         content=DownloadSource(format("{jdk_location}{check_db_connection_jar_name}")),
         mode=0644,
         )

    if name != "client":
        setup_non_client()
    if name == 'hiveserver2':
        setup_hiveserver2()
    if name == 'metastore':
        setup_metastore()


def setup_hiveserver2():
    import params

    File(params.start_hiveserver2_path,
         mode=0755,
         content=Template(format('{start_hiveserver2_script}'))
         )

    File(os.path.join(params.hive_server_conf_dir, "hadoop-metrics2-hiveserver2.properties"),
         owner=params.hive_user,
         group=params.user_group,
         content=Template("hadoop-metrics2-hiveserver2.properties.j2"),
         mode=0600
         )
    XmlConfig("hiveserver2-site.xml",
              conf_dir=params.hive_server_conf_dir,
              configurations=params.config['configurations']['hiveserver2-site'],
              configuration_attributes=params.config['configuration_attributes']['hiveserver2-site'],
              owner=params.hive_user,
              group=params.user_group,
              mode=0600)

    # copy tarball to HDFS feature not supported
    params.HdfsResource(params.webhcat_apps_dir,
                        type="directory",
                        action="create_on_execute",
                        owner=params.webhcat_user,
                        mode=0755
                        )

    # Create webhcat dirs.
    if params.hcat_hdfs_user_dir != params.webhcat_hdfs_user_dir:
        params.HdfsResource(params.hcat_hdfs_user_dir,
                            type="directory",
                            action="create_on_execute",
                            owner=params.webhcat_user,
                            mode=params.hcat_hdfs_user_mode
                            )

    params.HdfsResource(params.webhcat_hdfs_user_dir,
                        type="directory",
                        action="create_on_execute",
                        owner=params.webhcat_user,
                        mode=params.webhcat_hdfs_user_mode
                        )

    # if warehouse directory is in DFS
    if not params.whs_dir_protocol or params.whs_dir_protocol == urlparse(params.default_fs).scheme:
        # Create Hive Metastore Warehouse Dir
        params.HdfsResource(params.hive_apps_whs_dir,
                            type="directory",
                            action="create_on_execute",
                            owner=params.hive_user,
                            mode=0777
                            )
    else:
        Logger.info(format("Not creating warehouse directory '{hive_apps_whs_dir}', as the location is not in DFS."))

    # Create Hive User Dir
    params.HdfsResource(params.hive_hdfs_user_dir,
                        type="directory",
                        action="create_on_execute",
                        owner=params.hive_user,
                        mode=params.hive_hdfs_user_mode
                        )

    if not is_empty(params.hive_exec_scratchdir) and not urlparse(params.hive_exec_scratchdir).path.startswith("/tmp"):
        params.HdfsResource(params.hive_exec_scratchdir,
                            type="directory",
                            action="create_on_execute",
                            owner=params.hive_user,
                            group=params.hdfs_user,
                            mode=0777)  # Hive expects this dir to be writeable by everyone as it is used as a temp dir

    params.HdfsResource(None, action="execute")


def setup_non_client():
    import params

    Directory(params.hive_pid_dir,
              create_parents=True,
              cd_access='a',
              owner=params.hive_user,
              group=params.user_group,
              mode=0755)
    Directory(params.hive_log_dir,
              create_parents=True,
              cd_access='a',
              owner=params.hive_user,
              group=params.user_group,
              mode=0755)
    Directory(params.hive_var_lib,
              create_parents=True,
              cd_access='a',
              owner=params.hive_user,
              group=params.user_group,
              mode=0755)

    if params.hive_jdbc_target is not None and not os.path.exists(params.hive_jdbc_target):
        jdbc_connector(params.hive_jdbc_target, params.hive_previous_jdbc_jar)
    if params.hive2_jdbc_target is not None and not os.path.exists(params.hive2_jdbc_target):
        jdbc_connector(params.hive2_jdbc_target, params.hive2_previous_jdbc_jar)


def setup_metastore():
    import params

    if params.hive_metastore_site_supported:
        hivemetastore_site_config = params.config['configurations']['hivemetastore-site']
        if hivemetastore_site_config:
            XmlConfig("hivemetastore-site.xml",
                      conf_dir=params.hive_server_conf_dir,
                      configurations=params.config['configurations']['hivemetastore-site'],
                      configuration_attributes=params.config['configuration_attributes']['hivemetastore-site'],
                      owner=params.hive_user,
                      group=params.user_group,
                      mode=0600)

    File(os.path.join(params.hive_server_conf_dir, "hadoop-metrics2-hivemetastore.properties"),
         owner=params.hive_user,
         group=params.user_group,
         content=Template("hadoop-metrics2-hivemetastore.properties.j2"),
         mode=0600
         )

    File(params.start_metastore_path,
         mode=0755,
         content=StaticFile('startMetastore.sh')
         )


def create_metastore_schema():
    import params

    create_schema_cmd = format("export HIVE_CONF_DIR={hive_server_conf_dir} ; "
                               "{hive_schematool_bin}/schematool -initSchema "
                               "-dbType {hive_metastore_db_type} "
                               "-userName {hive_metastore_user_name} "
                               "-passWord {hive_metastore_user_passwd!p} -verbose")

    check_schema_created_cmd = as_user(format("export HIVE_CONF_DIR={hive_server_conf_dir} ; "
                                              "{hive_schematool_bin}/schematool -info "
                                              "-dbType {hive_metastore_db_type} "
                                              "-userName {hive_metastore_user_name} "
                                              "-passWord {hive_metastore_user_passwd!p} -verbose"), params.hive_user)
    quoted_hive_metastore_user_passwd = quote_bash_args(quote_bash_args(params.hive_metastore_user_passwd))
    if quoted_hive_metastore_user_passwd[0] == "'" and quoted_hive_metastore_user_passwd[-1] == "'" \
            or quoted_hive_metastore_user_passwd[0] == '"' and quoted_hive_metastore_user_passwd[-1] == '"':
        quoted_hive_metastore_user_passwd = quoted_hive_metastore_user_passwd[1:-1]
    Logger.sensitive_strings[repr(check_schema_created_cmd)] = repr(check_schema_created_cmd.replace(
        format("-passWord {quoted_hive_metastore_user_passwd}"), "-passWord " + utils.PASSWORDS_HIDE_STRING))

    Execute(create_schema_cmd,
            not_if=check_schema_created_cmd,
            user=params.hive_user
            )


"""
Writes configuration files required by Hive.
"""


def fill_conf_dir(component_conf_dir):
    import params
    hive_client_conf_path = '/etc/hive'
    component_conf_dir = os.path.realpath(component_conf_dir)
    mode_identified_for_file = 0644 if component_conf_dir == hive_client_conf_path else 0600
    mode_identified_for_dir = 0755 if component_conf_dir == hive_client_conf_path else 0700
    Directory(component_conf_dir,
              owner=params.hive_user,
              group=params.user_group,
              create_parents=True,
              mode=mode_identified_for_dir
              )

    XmlConfig("mapred-site.xml",
              conf_dir=component_conf_dir,
              configurations=params.config['configurations']['mapred-site'],
              configuration_attributes=params.config['configuration_attributes']['mapred-site'],
              owner=params.hive_user,
              group=params.user_group,
              mode=mode_identified_for_file)

    File(format("{component_conf_dir}/hive-default.xml.template"),
         owner=params.hive_user,
         group=params.user_group,
         mode=mode_identified_for_file
         )

    File(format("{component_conf_dir}/hive-env.sh.template"),
         owner=params.hive_user,
         group=params.user_group,
         mode=mode_identified_for_file
         )

    if params.log4j_version == '1':
        log4j_exec_filename = 'hive-exec-log4j.properties'
        if (params.log4j_exec_props != None):
            File(format("{component_conf_dir}/{log4j_exec_filename}"),
                 mode=mode_identified_for_file,
                 group=params.user_group,
                 owner=params.hive_user,
                 content=InlineTemplate(params.log4j_exec_props)
                 )
        elif (os.path.exists("{component_conf_dir}/{log4j_exec_filename}.template")):
            File(format("{component_conf_dir}/{log4j_exec_filename}"),
                 mode=mode_identified_for_file,
                 group=params.user_group,
                 owner=params.hive_user,
                 content=StaticFile(format("{component_conf_dir}/{log4j_exec_filename}.template"))
                 )

        log4j_filename = 'hive-log4j.properties'
        if (params.log4j_props != None):
            File(format("{component_conf_dir}/{log4j_filename}"),
                 mode=mode_identified_for_file,
                 group=params.user_group,
                 owner=params.hive_user,
                 content=InlineTemplate(params.log4j_props)
                 )
        elif (os.path.exists("{component_conf_dir}/{log4j_filename}.template")):
            File(format("{component_conf_dir}/{log4j_filename}"),
                 mode=mode_identified_for_file,
                 group=params.user_group,
                 owner=params.hive_user,
                 content=StaticFile(format("{component_conf_dir}/{log4j_filename}.template"))
                 )

    mode_identified = 0644
    llap_daemon_log4j_filename = 'llap-daemon-log4j2.properties'
    File(format("{component_conf_dir}/{llap_daemon_log4j_filename}"),
         mode=mode_identified,
         group=params.user_group,
         owner=params.hive_user,
         content=InlineTemplate(params.llap_daemon_log4j))

    llap_cli_log4j2_filename = 'llap-cli-log4j2.properties'
    File(format("{component_conf_dir}/{llap_cli_log4j2_filename}"),
         mode=mode_identified,
         group=params.user_group,
         owner=params.hive_user,
         content=InlineTemplate(params.llap_cli_log4j2))

    hive_log4j2_filename = 'hive-log4j2.properties'
    File(format("{component_conf_dir}/{hive_log4j2_filename}"),
         mode=mode_identified,
         group=params.user_group,
         owner=params.hive_user,
         content=InlineTemplate(params.hive_log4j2))

    hive_exec_log4j2_filename = 'hive-exec-log4j2.properties'
    File(format("{component_conf_dir}/{hive_exec_log4j2_filename}"),
         mode=mode_identified,
         group=params.user_group,
         owner=params.hive_user,
         content=InlineTemplate(params.hive_exec_log4j2))

    beeline_log4j2_filename = 'beeline-log4j2.properties'
    File(format("{component_conf_dir}/{beeline_log4j2_filename}"),
         mode=mode_identified,
         group=params.user_group,
         owner=params.hive_user,
         content=InlineTemplate(params.beeline_log4j2))

    if params.parquet_logging_properties is not None:
        File(format("{component_conf_dir}/parquet-logging.properties"),
             mode=mode_identified_for_file,
             group=params.user_group,
             owner=params.hive_user,
             content=params.parquet_logging_properties)


def jdbc_connector(target, hive_previous_jdbc_jar):
    """
    Shared by Hive Batch, Hive Metastore, and Hive Interactive
    :param target: Target of jdbc jar name, which could be for any of the components above.
    """
    import params

    if not params.jdbc_jar_name:
        return

    if params.hive_jdbc_driver in params.hive_jdbc_drivers_list and params.hive_use_existing_db:
        environment = {
            "no_proxy": format("{ambari_server_hostname}")
        }

        if hive_previous_jdbc_jar and os.path.isfile(hive_previous_jdbc_jar):
            File(hive_previous_jdbc_jar, action='delete')

        File(params.downloaded_custom_connector,
             content=DownloadSource(params.driver_curl_source))

        Execute(('cp', '-r', params.downloaded_custom_connector, target),
                path=["/bin", "/usr/bin/"],
                sudo=True)

    else:
        # for default hive db (Mysql)
        Execute(('cp', '-r', format('/usr/share/java/{jdbc_jar_name}'), target),
                path=["/bin", "/usr/bin/"],
                sudo=True
                )
    pass

    File(target,
         mode=0644,
         )
