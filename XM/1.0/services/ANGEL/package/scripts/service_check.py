import os
from resource_management.libraries.script import Script
from resource_management.core.resources.system import File, Execute

def copy_to_hdfs(name, user_group, owner,custom_source_file=None, custom_dest_file=None):
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

class ServiceCheck(Script):
    def service_check(self, env):
        import params
        env.set_params(params)
        pass

if __name__ == "__main__":
    ServiceCheck().execute()
