import os
from resource_management.core.resources.system import Directory, File
from resource_management.libraries.resources.xml_config import XmlConfig
from resource_management.core.source import InlineTemplate
from resource_management.core.resources.system import Execute


def install_tez():
    import params
    Directory(
        [params.config_dir],
        owner=params.tez_user,
        group=params.user_group,
        mode=0775,
        create_parents=True)
    if not os.path.exists('/opt/' + params.version_dir) or not os.path.exists(params.install_dir):
        Execute('rm -rf %s' % '/opt/' + params.version_dir)
        Execute('rm -rf %s' % params.install_dir)
        Execute(
            'wget ' + params.download_url + ' -O /tmp/' + params.filename,
            user=params.tez_user)
        Execute('tar -zxf /tmp/' + params.filename + ' -C /opt')
        Execute('ln -s /opt/' + params.version_dir + ' ' + params.install_dir)
        Execute(' rm -rf ' + params.install_dir + '/conf')
        Execute('ln -s ' + params.config_dir + ' ' + params.install_dir +
                '/conf')
        Execute("echo 'export PATH=%s/bin:$PATH'>>/etc/profile.d/hadoop.sh" %
                params.install_dir)
        Execute('chown -R %s:%s /opt/%s' %
                (params.tez_user, params.user_group, params.version_dir))
        Execute('chown -R %s:%s %s' %
                (params.tez_user, params.user_group, params.install_dir))
        Execute('/bin/rm -f /tmp/' + params.filename)


def tez(config_dir):
    """
    Write out tez-site.xml and tez-env.sh to the config directory.
    :param config_dir: Which config directory to save configs to, which is different during rolling upgrade.
    """
    import params

    Directory(params.tez_etc_dir, mode=0755)

    Directory(config_dir,
              owner=params.tez_user,
              group=params.user_group,
              create_parents=True)

    XmlConfig("tez-site.xml",
              conf_dir=config_dir,
              configurations=params.config['configurations']['tez-site'],
              configuration_attributes=params.config['configuration_attributes']['tez-site'],
              owner=params.tez_user,
              group=params.user_group,
              mode=0664)

    tez_env_file_path = os.path.join(config_dir, "tez-env.sh")
    File(tez_env_file_path,
         owner=params.tez_user,
         content=InlineTemplate(params.tez_env_sh_template),
         mode=0555)
