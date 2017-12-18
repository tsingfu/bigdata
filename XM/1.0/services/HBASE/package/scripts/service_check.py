#!/usr/bin/env python

from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.format import format
from resource_management.core.resources.system import Execute, File
from resource_management.core.source import StaticFile
from resource_management.core.source import Template
import functions


class HbaseServiceCheck(Script):
    def service_check(self, env):
        import params
        env.set_params(params)

        output_file = "/apps/hbase/data/ambarismoketest"
        smokeuser_kinit_cmd = format(
            "{kinit_path_local} -kt {smoke_user_keytab} {smokeuser_principal};") if params.security_enabled else ""
        hbase_servicecheck_file = format("{exec_tmp_dir}/hbase-smoke.sh")
        hbase_servicecheck_cleanup_file = format(
            "{exec_tmp_dir}/hbase-smoke-cleanup.sh")

        File(
            format("{exec_tmp_dir}/hbaseSmokeVerify.sh"),
            content=StaticFile("hbaseSmokeVerify.sh"),
            mode=0755)

        File(
            hbase_servicecheck_cleanup_file,
            content=StaticFile("hbase-smoke-cleanup.sh"),
            mode=0755)

        File(
            hbase_servicecheck_file,
            mode=0755,
            content=Template('hbase-smoke.sh.j2'))

        if params.security_enabled:
            hbase_grant_premissions_file = format(
                "{exec_tmp_dir}/hbase_grant_permissions.sh")
            grantprivelegecmd = format(
                "{kinit_cmd} {hbase_cmd} shell {hbase_grant_premissions_file}")

            File(
                hbase_grant_premissions_file,
                owner=params.hbase_user,
                group=params.user_group,
                mode=0644,
                content=Template('hbase_grant_permissions.j2'))

            Execute(
                grantprivelegecmd,
                user=params.hbase_user, )

        servicecheckcmd = format(
            "{smokeuser_kinit_cmd} {hbase_cmd} --config {hbase_conf_dir} shell {hbase_servicecheck_file}")
        smokeverifycmd = format(
            "{exec_tmp_dir}/hbaseSmokeVerify.sh {hbase_conf_dir} {service_check_data} {hbase_cmd}")
        cleanupCmd = format(
            "{smokeuser_kinit_cmd} {hbase_cmd} --config {hbase_conf_dir} shell {hbase_servicecheck_cleanup_file}")
        Execute(
            format("{servicecheckcmd} && {smokeverifycmd} && {cleanupCmd}"),
            tries=6,
            try_sleep=5,
            user=params.smoke_test_user,
            logoutput=True)


if __name__ == "__main__":
    HbaseServiceCheck().execute()
