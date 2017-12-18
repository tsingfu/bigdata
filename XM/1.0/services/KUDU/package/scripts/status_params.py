from resource_management.libraries.script.script import Script

config = Script.get_config()

kudu_pid_dir = config['configurations']['kudu-env']['kudu_pid_dir']
master_pid = kudu_pid_dir + '/master.pid'
tserver_pid = kudu_pid_dir + '/tserver.pid'