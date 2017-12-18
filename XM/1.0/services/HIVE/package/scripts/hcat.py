#!/usr/bin/env python

from resource_management import *
from ambari_commons.constants import SERVICE
from resource_management.core.resources.system import Directory, Execute, File
from resource_management.core.source import InlineTemplate, StaticFile
from resource_management.libraries.resources.xml_config import XmlConfig
import sys, os

def hcat():
    import params

    Directory(
        params.hive_conf_dir,
        create_parents=True,
        owner=params.hcat_user,
        group=params.user_group, )

    Directory(
        params.hcat_conf_dir,
        create_parents=True,
        owner=params.hcat_user,
        group=params.user_group, )

    Directory(
        params.hcat_pid_dir, owner=params.webhcat_user, create_parents=True)

    XmlConfig(
        "hive-site.xml",
        conf_dir=params.hive_client_conf_dir,
        configurations=params.config['configurations']['hive-site'],
        configuration_attributes=params.config['configuration_attributes'][
            'hive-site'], owner=params.hive_user, group=params.user_group,
        mode=0644)

    File(
        format("{hcat_conf_dir}/hcat-env.sh"),
        owner=params.hcat_user,
        group=params.user_group,
        content=InlineTemplate(params.hcat_env_sh_template))

    # Generate atlas-application.properties.xml file
    if params.enable_atlas_hook:
        script_path = os.path.realpath(__file__).split('/services')[0] + '/hooks/before-INSTALL/scripts/atlas'
        sys.path.append(script_path)
        from setup_atlas_hook import has_atlas_in_cluster, setup_atlas_hook,setup_atlas_jar_symlinks
        atlas_hook_filepath = os.path.join(params.hive_config_dir, params.atlas_hook_filename)
        setup_atlas_hook(SERVICE.HIVE, params.hive_atlas_application_properties, atlas_hook_filepath, params.hive_user, params.user_group)
        setup_atlas_jar_symlinks("hive", params.hcat_lib)