#!/usr/bin/env python

import os

from resource_management.core.logger import Logger
from resource_management.core.resources.system import Execute, Directory
from resource_management.libraries.script import Script
from resource_management.libraries.functions.format import format
from resource_management.libraries.functions.security_commons import cached_kinit_executor
from resource_management.core.resources.system import File
from resource_management.libraries.functions import check_process_status
from setup_ranger_hive import setup_ranger_hive_metastore_service

from hive import hive, install_hive, create_metastore_schema
from hive import jdbc_connector
from hive_service import hive_service


class HiveMetastore(Script):
    def install(self, env):
        install_hive()

    def start(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        install_hive()
        self.configure(env)
        if params.init_metastore_schema:
            create_metastore_schema()  # execute without config lock
        hive_service('metastore', action='start', upgrade_type=upgrade_type)
        setup_ranger_hive_metastore_service()

    def stop(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        hive_service('metastore', action='stop', upgrade_type=upgrade_type)

    def configure(self, env):
        import params
        env.set_params(params)
        hive(name='metastore')

    def get_component_name(self):
        return "hive-metastore"

    def status(self, env):
        import status_params

        env.set_params(status_params)
        check_process_status(status_params.hive_metastore_pid)

    def pre_upgrade_restart(self, env, upgrade_type=None):
        print 'todo'

    def upgrade_schema(self, env):
        """
        Executes the schema upgrade binary.  This is its own function because it could
        be called as a standalone task from the upgrade pack, but is safe to run it for each
        metastore instance. The schema upgrade on an already upgraded metastore is a NOOP.

        The metastore schema upgrade requires a database driver library for most
        databases. During an upgrade, it's possible that the library is not present,
        so this will also attempt to copy/download the appropriate driver.

        This function will also ensure that configurations are written out to disk before running
        since the new configs will most likely not yet exist on an upgrade.

        Should not be invoked for a DOWNGRADE; Metastore only supports schema upgrades.
        """
        Logger.info("Upgrading Hive Metastore Schema")
        import status_params
        import params
        env.set_params(params)

        # ensure that configurations are written out before trying to upgrade the schema
        # since the schematool needs configs and doesn't know how to use the hive conf override
        self.configure(env)

        if params.security_enabled:
            cached_kinit_executor(
                status_params.kinit_path_local, status_params.hive_user,
                params.hive_metastore_keytab_path,
                params.hive_metastore_principal, status_params.hostname,
                status_params.tmp_dir)

        # ensure that the JDBC drive is present for the schema tool; if it's not
        # present, then download it first
        if params.hive_jdbc_driver in params.hive_jdbc_drivers_list:
            target_directory = format("{params.install_dir}/lib")

            # download it if it does not exist
            if not os.path.exists(params.source_jdbc_file):
                jdbc_connector(params.hive_jdbc_target,
                               params.hive_previous_jdbc_jar)

            target_directory_and_filename = os.path.join(
                target_directory, os.path.basename(params.source_jdbc_file))

            # copy the JDBC driver from the older metastore location to the new location only
            # if it does not already exist
            if not os.path.exists(target_directory_and_filename):
                Execute(
                    ('cp', params.source_jdbc_file, target_directory),
                    path=["/bin", "/usr/bin/"],
                    sudo=True)

            File(target_directory_and_filename, mode=0644)

        # build the schema tool command
        binary = format("{hive_schematool_ver_bin}/schematool")

        schematool_hive_server_conf_dir = params.hive_server_conf_dir
        env_dict = {'HIVE_CONF_DIR': schematool_hive_server_conf_dir}

        command = format(
            "{binary} -dbType {hive_metastore_db_type} -upgradeSchema")
        Execute(
            command,
            user=params.hive_user,
            tries=1,
            environment=env_dict,
            logoutput=True)

    def get_log_folder(self):
        import params
        return params.hive_log_dir

    def get_user(self):
        import params
        return params.hive_user

    def get_pid_files(self):
        import status_params
        return [status_params.hive_metastore_pid]


if __name__ == "__main__":
    HiveMetastore().execute()
