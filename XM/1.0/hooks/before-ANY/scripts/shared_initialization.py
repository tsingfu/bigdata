import os
import re
import getpass
import tempfile
from copy import copy
from resource_management import *
import commands
from resource_management.core.resources.system import Execute, File
from resource_management.core.logger import Logger
from resource_management import Directory, File, PropertiesFile, InlineTemplate, format

def setup_users():
    """
  Creates users before cluster installation
  """
    import params

    should_create_users_and_groups = False
    if params.host_sys_prepped:
        should_create_users_and_groups = not params.sysprep_skip_create_users_and_groups
    else:
        should_create_users_and_groups = not params.ignore_groupsusers_create

    if should_create_users_and_groups:
        for group in params.group_list:
            Group(group, )

        for user in params.user_list:
            status,result  = commands.getstatusoutput('id %s' % user)
            try:
                User(
                    user,
                    gid=params.user_to_gid_dict[user],
                    groups=params.user_to_groups_dict[user],
                    fetch_nonlocal_groups=params.fetch_nonlocal_groups)
            except Exception as e:
                Logger.info(str(e))

        if params.override_uid == "true":
            set_uid(params.smoke_user, params.smoke_user_dirs)
        else:
            Logger.info(
                'Skipping setting uid for smoke user as host is sys prepped')
    else:
        Logger.info(
            'Skipping creation of User and Group as host is sys prepped or ignore_groupsusers_create flag is on')
        pass

    for user in params.user_list:
        if not os.path.exists('/home/' + user):
            try:
                Execute('mkdir -p /home/%s && chown -R %s:hadoop /home/%s && \cp /etc/skel/.bash* /home/%s/' % (user,user,user,user))
            except Exception as e:
                Logger.info('change /home fail')

    if params.has_hbase_masters:
        try:
            Directory(
                params.hbase_tmp_dir,
                owner=params.hbase_user,
                mode=0775,
                create_parents=True,
                cd_access="a", )
            if params.override_uid == "true":
                set_uid(params.hbase_user, params.hbase_user_dirs)
            else:
                Logger.info(
                    'Skipping setting uid for hbase user as host is sys prepped')
        except Exception as e:
            Logger.info('Skipping setting uid for hbase user as host')

    if should_create_users_and_groups:
        if params.has_namenode:
            create_dfs_cluster_admins()
    else:
        Logger.info('Skipping setting dfs cluster admin ')


def create_dfs_cluster_admins():
    """
  dfs.cluster.administrators support format <comma-delimited list of usernames><space><comma-delimited list of group names>
  """
    import params

    groups_list = create_users_and_groups(
        params.dfs_cluster_administrators_group)

    User(
        params.hdfs_user,
        groups=params.user_to_groups_dict[params.hdfs_user] + groups_list,
        fetch_nonlocal_groups=params.fetch_nonlocal_groups)


def create_users_and_groups(user_and_groups):

    import params

    parts = re.split('\s+', user_and_groups)
    if len(parts) == 1:
        parts.append("")

    users_list = parts[0].split(",") if parts[0] else []
    groups_list = parts[1].split(",") if parts[1] else []

    # skip creating groups and users if * is provided as value.
    users_list = filter(lambda x: x != '*', users_list)
    groups_list = filter(lambda x: x != '*', groups_list)

    if users_list:
        User(users_list, fetch_nonlocal_groups=params.fetch_nonlocal_groups)

    if groups_list:
        Group(copy(groups_list), )
    return groups_list


def set_uid(user, user_dirs):
    """
  user_dirs - comma separated directories
  """
    import params

    File(
        format("{tmp_dir}/changeUid.sh"),
        content=StaticFile("changeToSecureUid.sh"),
        mode=0555)
    ignore_groupsusers_create_str = str(
        params.ignore_groupsusers_create).lower()
    Execute(
        format("{tmp_dir}/changeUid.sh {user} {user_dirs}"),
        not_if=format(
            "(test $(id -u {user}) -gt 1000) || ({ignore_groupsusers_create_str})"))


def setup_hadoop_env():
    import params
    stackversion = params.stack_version_unformatted
    Logger.info("FS Type: {0}".format(params.dfs_type))
    if params.has_namenode or stackversion.find(
            'Gluster') >= 0 or params.dfs_type == 'HCFS':
        if params.security_enabled:
            tc_owner = "root"
        else:
            tc_owner = params.hdfs_user

        # create /etc/hadoop
        #Directory(params.hadoop_dir, mode=0755)

        # write out hadoop-env.sh, but only if the directory exists
        if os.path.exists(params.hadoop_conf_dir):
            File(
                os.path.join(params.hadoop_conf_dir, 'hadoop-env.sh'),
                owner=tc_owner,
                group=params.user_group,
                content=InlineTemplate(params.hadoop_env_sh_template))

        # Create tmp dir for java.io.tmpdir
        # Handle a situation when /tmp is set to noexec
        try:
            Directory(
                params.hadoop_java_io_tmpdir,
                owner=params.hdfs_user,
                group=params.user_group,
                mode=01777)
        except Exception as e:
            Logger.info('Skipping setting uid for hdfs user as host')

def setup_java():
    """
  Installs jdk using specific params, that comes from ambari-server
  """
    import params

    java_exec = format("{java_home}/bin/java")

    if not os.path.isfile(java_exec):

        jdk_curl_target = format("{tmp_dir}/{jdk_name}")
        java_dir = os.path.dirname(params.java_home)

        Directory(
            params.artifact_dir,
            create_parents=True, )

        File(
            jdk_curl_target,
            content=DownloadSource(format("{jdk_location}/{jdk_name}")),
            not_if=format("test -f {jdk_curl_target}"))

        File(
            jdk_curl_target,
            mode=0755, )

        tmp_java_dir = tempfile.mkdtemp(prefix="jdk_tmp_", dir=params.tmp_dir)

        try:
            if params.jdk_name.endswith(".bin"):
                chmod_cmd = ("chmod", "+x", jdk_curl_target)
                install_cmd = format(
                    "cd {tmp_java_dir} && echo A | {jdk_curl_target} -noregister && {sudo} cp -rp {tmp_java_dir}/* {java_dir}")
            elif params.jdk_name.endswith(".gz"):
                chmod_cmd = ("chmod", "a+x", java_dir)
                install_cmd = format(
                    "cd {tmp_java_dir} && tar -xf {jdk_curl_target} && {sudo} /bin/cp -rp {tmp_java_dir}/* {java_dir}")

            Directory(java_dir)
            Execute(
                chmod_cmd,
                sudo=True, )

            Execute(install_cmd, )
            java_version_dir = os.listdir(tmp_java_dir)[0]
            Execute("ln -s %s %s" %(java_dir + '/' + java_version_dir,params.java_home))
        finally:
            Directory(tmp_java_dir, action="delete")

        File(
            format("{java_home}/bin/java"),
            mode=0755,
            cd_access="a", )
        Execute(
            ('chmod', '-R', '755', params.java_home),
            sudo=True, )
