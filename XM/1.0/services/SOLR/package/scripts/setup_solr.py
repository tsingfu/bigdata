from resource_management.core.exceptions import Fail
from resource_management.core.source import InlineTemplate, Template, StaticFile
from resource_management.libraries.functions.decorator import retry
from resource_management.libraries.functions.format import format
from resource_management.libraries.functions import solr_cloud_util
from resource_management.core.resources.system import Directory, Execute, File, Link
import os


def install_solr():
    import params
    Directory(
        [params.solr_conf],
        owner=params.solr_user,
        group=params.user_group,
        mode=0775,
        create_parents=True)
    if not os.path.exists('/opt/' + params.version_dir) or not os.path.exists(params.install_dir):
        Execute('rm -rf %s' % '/opt/' + params.version_dir)
        Execute('rm -rf %s' % params.install_dir)
        Execute(
            'wget ' + params.download_url + ' -O /tmp/' + params.filename,
            user=params.solr_user)
        Execute('tar -zxf /tmp/' + params.filename + ' -C /opt')
        Execute('ln -s /opt/' + params.version_dir + ' ' + params.install_dir)
        Execute('ln -s ' + params.solr_conf + ' ' + params.install_dir +
                '/conf')
        Execute('chown -R %s:%s /opt/%s' %
                (params.solr_user, params.user_group, params.version_dir))
        Execute('chown -R %s:%s %s' %
                (params.solr_user, params.user_group, params.install_dir))
        Execute('/bin/rm -f /tmp/' + params.filename)


def setup_solr(name=None):
    import params

    if name == 'server':
        params.HdfsResource(params.solr_hdfs_home_dir,
                            type="directory",
                            action="create_on_execute",
                            owner=params.solr_user,
                            mode=0755
                            )

        params.HdfsResource(None, action="execute")

        Directory([params.solr_log_dir, params.solr_piddir,
                   params.solr_datadir, params.solr_data_resources_dir],
                  mode=0755,
                  cd_access='a',
                  create_parents=True,
                  owner=params.solr_user,
                  group=params.user_group
                  )

        Directory([params.solr_dir],
                  mode=0755,
                  cd_access='a',
                  create_parents=True,
                  recursive_ownership=True
                  )

        Directory([params.solr_conf],
                  mode=0755,
                  cd_access='a',
                  owner=params.solr_user,
                  group=params.user_group,
                  create_parents=True,
                  recursive_ownership=True
                  )

        File(params.solr_log,
             mode=0644,
             owner=params.solr_user,
             group=params.user_group,
             content=''
             )

        File(format("{solr_conf}/solr-env.sh"),
             content=InlineTemplate(params.solr_env_content),
             mode=0755,
             owner=params.solr_user,
             group=params.user_group
             )

        if params.solr_xml_content:
            File(format("{solr_datadir}/solr.xml"),
                 content=InlineTemplate(params.solr_xml_content),
                 owner=params.solr_user,
                 group=params.user_group
                 )

        File(format("{solr_conf}/log4j.properties"),
             content=InlineTemplate(params.solr_log4j_content),
             owner=params.solr_user,
             group=params.user_group
             )

        jaas_file = params.solr_jaas_file if params.security_enabled else None
        url_scheme = 'https' if params.solr_ssl_enabled else 'http'

        create_solr_znode()

        if params.security_enabled:
            File(format("{solr_jaas_file}"),
                 content=Template("solr_jaas.conf.j2"),
                 owner=params.solr_user)

        solr_cloud_util.set_cluster_prop(
            zookeeper_quorum=params.zookeeper_quorum,
            solr_znode=params.solr_znode,
            java64_home=params.java64_home,
            prop_name="urlScheme",
            prop_value=url_scheme,
            jaas_file=jaas_file
        )

        solr_cloud_util.setup_kerberos_plugin(
            zookeeper_quorum=params.zookeeper_quorum,
            solr_znode=params.solr_znode,
            jaas_file=jaas_file,
            java64_home=params.java64_home,
            secure=params.security_enabled
        )

    else:
        raise Fail('Neither client, nor server were selected to install.')


@retry(times=30, sleep_time=5, err_class=Fail)
def create_solr_znode():
    import params
    solr_cloud_util.create_znode(
        zookeeper_quorum=params.zookeeper_quorum,
        solr_znode=params.solr_znode,
        java64_home=params.java64_home)
