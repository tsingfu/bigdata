#!/usr/bin/env python

from resource_management import *
from resource_management.core.resources.system import Execute, File


def service(name,action='start'):
  import params
  import status_params

  pid_file = status_params.pid_files[name]
  no_op_test = format("ls {pid_file} >/dev/null 2>&1 && ps `cat {pid_file}` >/dev/null 2>&1")

  if name == "logviewer":
    tries_count = 12
  else:
    tries_count = 6

  if name == 'ui':
    process_grep = "apache.catalina.startup.Bootstrap$"
  else:
    process_grep = format("jstorm.*.{name}")
    
  find_proc = format("{jps_binary} -l  | grep {process_grep}")
  write_pid = format("{find_proc} | awk {{'print $1'}} > {pid_file}")
  crt_pid_cmd = format("{find_proc} && {write_pid}")

  if action == "start":
    if name == "ui":
      cmd = format("{tomcat_dir}/bin/startup.sh > {log_dir}/{name}.out 2>&1")

      Execute(cmd,
             not_if=no_op_test,
             user='root'
      )
      Execute(crt_pid_cmd,
              user=params.jstorm_user,
              logoutput=True,
              tries=tries_count,
              try_sleep=10,
              path=params.jstorm_bin_dir
      )
    else:
      cmd = format("env JAVA_HOME={java64_home} PATH=$PATH:{java64_home}/bin jstorm {name} > {log_dir}/{name}.out 2>&1")

      Execute(cmd,
             not_if=no_op_test,
             user=params.jstorm_user,
             wait_for_finish=False,
             path=params.jstorm_bin_dir
      )
      Execute(crt_pid_cmd,
              user=params.jstorm_user,
              logoutput=True,
              tries=tries_count,
              try_sleep=10,
              path=params.jstorm_bin_dir
      )

  elif action == "stop":
    if name == "ui":
      cmd = format("{tomcat_dir}/bin/shutdown.sh")
      Execute(cmd,
              user='root'
      )
      Execute(format("rm -f {pid_file}"))
    else:
      process_dont_exist = format("! ({no_op_test})")
      pid = format("`cat {pid_file}` >/dev/null 2>&1")
      Execute(format("kill {pid}"),
              not_if=process_dont_exist
      )
      Execute(format("kill -9 {pid}"),
              not_if=format("sleep 2; {process_dont_exist} || sleep 20; {process_dont_exist}"),
              ignore_failures=True
      )
      Execute(format("rm -f {pid_file}"))
