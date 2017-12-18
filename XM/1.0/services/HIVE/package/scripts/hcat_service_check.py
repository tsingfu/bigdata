#!/usr/bin/env python

import os
from resource_management import *
from resource_management.libraries.functions import get_unique_id_and_date


def hcat_service_check():
    import params
    unique = get_unique_id_and_date()
    output_file = format("{hive_apps_whs_dir}/hcatsmoke{unique}")
    test_cmd = format("fs -test -e {output_file}")

    if params.security_enabled:
        kinit_cmd = format(
            "{kinit_path_local} -kt {smoke_user_keytab} {smokeuser_principal}; ")
    else:
        kinit_cmd = ""

    File(
        format("{tmp_dir}/hcatSmoke.sh"),
        content=StaticFile("hcatSmoke.sh"),
        mode=0755)

    prepare_cmd = format(
        "{kinit_cmd}env JAVA_HOME={java64_home} {tmp_dir}/hcatSmoke.sh hcatsmoke{unique} prepare {purge_tables}")

    exec_path = params.execute_path
    if params.version and params.stack_root:
        upgrade_hive_bin = format("{params.install_dir}/bin")
        exec_path = os.environ[
            'PATH'] + os.pathsep + params.hadoop_bin_dir + os.pathsep + upgrade_hive_bin

    Execute(
        prepare_cmd,
        tries=3,
        user=params.smokeuser,
        try_sleep=5,
        path=['/usr/sbin', '/usr/local/bin', '/bin', '/usr/bin', exec_path],
        logoutput=True)

    if params.security_enabled:
        Execute(
            format(
                "{kinit_path_local} -kt {hdfs_user_keytab} {hdfs_principal_name}"),
            user=params.hdfs_user, )

    ExecuteHadoop(
        test_cmd,
        user=params.hdfs_user,
        logoutput=True,
        conf_dir=params.hadoop_conf_dir,
        bin_dir=params.execute_path)

    cleanup_cmd = format(
        "{kinit_cmd} {tmp_dir}/hcatSmoke.sh hcatsmoke{unique} cleanup {purge_tables}")

    Execute(
        cleanup_cmd,
        tries=3,
        user=params.smokeuser,
        try_sleep=5,
        path=['/usr/sbin', '/usr/local/bin', '/bin', '/usr/bin', exec_path],
        logoutput=True)
