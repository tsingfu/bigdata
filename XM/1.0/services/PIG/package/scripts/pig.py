
import os
from resource_management.core.source import InlineTemplate
from resource_management.libraries.functions.format import format
from resource_management.core.resources.system import Directory, Execute, File, Link

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


def install_pig():
    import params
    Directory(
        [params.pig_conf_dir],
        owner=params.pig_user,
        group=params.user_group,
        mode=0775,
        create_parents=True)
    if not os.path.exists('/opt/' + params.version_dir) or not os.path.exists(params.install_dir):
        Execute('rm -rf %s' % '/opt/' + params.version_dir)
        Execute('rm -rf %s' % params.install_dir)
        Execute(
            'wget ' + params.download_url + ' -O /tmp/' + params.filename,
            user=params.pig_user)
        Execute('tar -zxf /tmp/' + params.filename + ' -C /opt')
        Execute('ln -s /opt/' + params.version_dir + ' ' + params.install_dir)
        Execute(' mkdir -p ' + params.pig_conf_dir + ' && cp -r ' + params.install_dir + '/conf/* ' + params.pig_conf_dir)
        Execute(' rm -rf ' + params.install_dir + '/conf')
        Execute('ln -s ' + params.pig_conf_dir + ' ' + params.install_dir +
                '/conf')
        Execute('chown -R %s:%s /opt/%s' %
                (params.pig_user, params.user_group, params.version_dir))
        Execute('chown -R %s:%s %s' %
                (params.pig_user, params.user_group, params.install_dir))
        Execute('/bin/rm -f /tmp/' + params.filename)
        Execute('wget http://yum.example.com/hadoop/hdfs/pig.tar.gz -O /tmp/pig.tar.gz')
        copy_to_hdfs("pig",
                     params.user_group,
                     params.hdfs_user,
                     custom_source_file='/tmp/pig.tar.gz',
                     custom_dest_file='/apps/pig/pig.tar.gz')
        params.HdfsResource(None, action="execute")

def pig():
    import params

    Directory(params.pig_conf_dir,
              create_parents=True,
              owner=params.hdfs_user,
              group=params.user_group
              )

    File(format("{pig_conf_dir}/pig-env.sh"),
         owner=params.hdfs_user,
         mode=0755,
         content=InlineTemplate(params.pig_env_sh_template)
         )

    # pig_properties is always set to a default even if it's not in the payload
    File(format("{params.pig_conf_dir}/pig.properties"),
         mode=0644,
         group=params.user_group,
         owner=params.hdfs_user,
         content=params.pig_properties
         )

    if (params.log4j_props != None):
        File(format("{params.pig_conf_dir}/log4j.properties"),
             mode=0644,
             group=params.user_group,
             owner=params.hdfs_user,
             content=params.log4j_props
             )
    elif (os.path.exists(format("{params.pig_conf_dir}/log4j.properties"))):
        File(format("{params.pig_conf_dir}/log4j.properties"),
             mode=0644,
             group=params.user_group,
             owner=params.hdfs_user
             )
