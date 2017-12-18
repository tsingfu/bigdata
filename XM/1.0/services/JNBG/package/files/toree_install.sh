#!/usr/bin/env bash

set -x

PY_EXEC=$1
PY_VENV_PATH_PREFIX=$2
PY_VENV_OWNER=$3
KINIT_CMD=$4
SPARK_HOME=$5
SPARK_OPTS=$6

checkPipInstall()
{
  pip show $1 2>&1 > /dev/null
}

checkSuccess()
{
  if [ $? != 0 ]
  then
    set +x
    echo "Error encountered at line $1 while attempting to: "
    if [ -n "$2" ]
    then
      echo $2
    fi
    echo Exiting.
    exit 1
  fi
  set -x
}

if [ ! -d "${PY_VENV_PATH_PREFIX}/python2.7" ]; then
  easy_install pip
  checkSuccess $LINENO "-  easy_install pip"

  pip install virtualenv --upgrade
  checkPipInstall virtualenv
  checkSuccess $LINENO "-  pip install virtualenv"

  virtualenv -p ${PY_EXEC} ${PY_VENV_PATH_PREFIX}/python2.7
  checkSuccess $LINENO "-  create virtualenv using ${PY_EXEC}"
fi
source ${PY_VENV_PATH_PREFIX}/python2.7/bin/activate
pip -V

if [ "$rhver" -eq 6 ]; then
  if [ "$rhscl" -eq 1 ]; then
    pip -V
    # uninstall older pip version that accompanies virtualenv with SCL
    pip uninstall -y pip
    easy_install pip
    checkPipInstall pip
    checkSuccess $LINENO "- easy_install pip"
  fi
fi

# Use --index-url and not --extra-index-url as we are trying to install
# specific package versions
pip install setuptools --upgrade
checkPipInstall setuptools
checkSuccess $LINENO "- pip install setuptools"

# Using --upgrade enables updating missing dependencies after failed installs
pip install toree http://yum.example.com/hadoop/toree-0.2.0.dev1.tar.gz
checkPipInstall toree
checkSuccess $LINENO "- pip install toree"

# Note the value of --kernel_name and --interpreters from the toree install command determines the kernel directory
# i.e. --kernel_name='Spark 2.1' --interpreters='Scala' --> .../jupyter/kernels/spark_2.1_scala/
kernel_dir=${PY_VENV_PATH_PREFIX}/python2.7/share/jupyter/kernels/spark_2.1_scala
kernel_run_file=$kernel_dir/bin/run.sh

# Include the end-user name for spark-submit application name (KERNEL_USERNAME env var set by nb2kg)
sed -i "s/--name \"'Apache Toree'\"/--name \"'\${KERNEL_USERNAME:-Notebook} Scala'\"/" $kernel_run_file

# Replace log file path in SPARK_OPTS
sed -i "/eval exec/i SPARK_OPTS=\"\${SPARK_OPTS//spark-driver-USER.log/spark-driver-\${KERNEL_USERNAME:-all}.log}\"\n" $kernel_run_file

# For kerberized clusters
if [ -n "${KINIT_CMD}" ]; then
  sed -i "/eval exec/i ${KINIT_CMD}\n" $kernel_run_file
fi

# Set ownership of the created virtualenv if configured via python_virtualenv_restrictive
if [ "${PY_VENV_OWNER}" != "root" ]; then
  echo ====== Virtualenv owner = $PY_VENV_OWNER =========
  chown -R ${PY_VENV_OWNER}: ${PY_VENV_PATH_PREFIX}/python2.7
fi
