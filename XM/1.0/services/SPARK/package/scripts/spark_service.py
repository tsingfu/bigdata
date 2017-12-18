#!/usr/bin/env python

import socket

from resource_management.libraries.script.script import Script
from resource_management.libraries.functions import format
from resource_management.core.resources.system import File, Execute
from resource_management.libraries.functions.show_logs import show_logs


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

def spark_service(name, upgrade_type=None, action=None):
    import params

    if action == 'start':

        if name == 'jobhistoryserver':
            # copy spark-assembly.jar to hdfs
            Execute(
                'wget http://yum.example.com/hadoop/hdfs/spark-assembly-1.6.3-hadoop2.6.0.jar -O /tmp/spark-assembly-1.6.3-hadoop2.6.0.jar')
            copy_to_hdfs("spark", params.user_group, params.hdfs_user,
                                            custom_source_file='/tmp/spark-assembly-1.6.3-hadoop2.6.0.jar',
                                            custom_dest_file='/apps/mapreduce/spark-assembly-1.6.3-hadoop2.6.0.jar')
            params.HdfsResource(None, action="execute")

            # create spark history directory
            params.HdfsResource(
                params.spark_history_dir,
                type="directory",
                action="create_on_execute",
                owner=params.spark_user,
                group=params.user_group,
                mode=0777,
                recursive_chmod=True)
            params.HdfsResource(None, action="execute")

        if params.security_enabled:
            spark_kinit_cmd = format(
                "{kinit_path_local} -kt {spark_kerberos_keytab} {spark_principal}; ")
            Execute(spark_kinit_cmd, user=params.spark_user)

        if name == 'jobhistoryserver':
            historyserver_no_op_test = format(
                'ls {spark_history_server_pid_file} >/dev/null 2>&1 && ps -p `cat {spark_history_server_pid_file}` >/dev/null 2>&1')
            try:
                Execute(
                    format(
                        'source {params.spark_conf}/spark-env.sh; {spark_history_server_start}'),
                    user=params.spark_user,
                    environment={'JAVA_HOME': params.java_home},
                    not_if=historyserver_no_op_test)
            except:
                show_logs(params.spark_log_dir, user=params.spark_user)
                raise

        elif name == 'sparkthriftserver':
            if params.security_enabled:
                hive_principal = params.hive_kerberos_principal
                hive_kinit_cmd = format(
                    "{kinit_path_local} -kt {hive_kerberos_keytab} {hive_principal}; ")
                Execute(hive_kinit_cmd, user=params.hive_user)
            Execute("chown -R %s:%s %s " % (params.hive_user, params.spark_group, params.spark_pid_dir))
            Execute("chown -R %s:%s %s " % (params.hive_user, params.spark_group, params.spark_pid_dir))
            thriftserver_no_op_test = format(
                'ls {spark_thrift_server_pid_file} >/dev/null 2>&1 && ps -p `cat {spark_thrift_server_pid_file}` >/dev/null 2>&1')
            try:
                Execute(
                    format(
                        'source {params.spark_conf}/spark-env.sh; {spark_thrift_server_start} --properties-file {spark_thrift_server_conf_file} {spark_thrift_cmd_opts_properties}'),
                    user=params.hive_user,
                    environment={'JAVA_HOME': params.java_home},
                    not_if=thriftserver_no_op_test)
            except:
                show_logs(params.spark_log_dir, user=params.hive_user)
                raise
    elif action == 'stop':
        if name == 'jobhistoryserver':
            try:
                Execute(
                    format(
                        'source {params.spark_conf}/spark-env.sh; {spark_history_server_stop}'),
                    user=params.spark_user,
                    environment={'JAVA_HOME': params.java_home})
            except:
                show_logs(params.spark_log_dir, user=params.spark_user)
                raise
            File(params.spark_history_server_pid_file, action="delete")

        elif name == 'sparkthriftserver':
            try:
                Execute(
                    format('{spark_thrift_server_stop}'),
                    user=params.hive_user,
                    environment={'JAVA_HOME': params.java_home})
            except:
                show_logs(params.spark_log_dir, user=params.hive_user)
                raise
            File(params.spark_thrift_server_pid_file, action="delete")
