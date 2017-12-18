#!/usr/bin/env bash

set -x

NBX_USER=$1
PY_EXEC=$2
PY_VENV_PATH_PREFIX=$3
PY_VENV_OWNER=$4
KINIT_CMD=$5
SPARK_HOME=$6
TOREE_INTERPRETERS=$7
TOREE_OPTS=${8:-""}
SPARK_OPTS=$9

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

PLATFORM=`uname -p`
rhver=7

if [ "$PLATFORM" == "x86_64" ]
then
  if [ -x /usr/bin/lsb_release ]; then
    rhver=$(/usr/bin/lsb_release -rs | cut -f1 -d.)
  fi
fi

pyver=`echo $(${PY_EXEC} -V 2>&1 | awk '{ print $2 }') | sed -e 's/\.//g'`
if [ "$pyver" -lt 270 ]; then
  echo "Configuration failed; Ensure that the specified python_interpreter_path is Python version 2.7."
  exit 1
fi

if [ ! -d "${PY_VENV_PATH_PREFIX}/python2.7" ]; then
  echo "Configuration failed as the virtualenv ${PY_VENV_PATH_PREFIX}/python2.7 was not found; Ensure that the installation was usccessful."
  exit 1
fi
source ${PY_VENV_PATH_PREFIX}/python2.7/bin/activate
pip -V

if [ -z "${TOREE_OPTS}" ]; then
  jupyter toree install --sys-prefix --spark_home=${SPARK_HOME} --kernel_name='Spark 2.1' --interpreters=${TOREE_INTERPRETERS} "--spark_opts=${SPARK_OPTS}"
  checkSuccess $LINENO "-  jupyter toree install"
else
  jupyter toree install --sys-prefix --spark_home=${SPARK_HOME} --kernel_name='Spark 2.1' --interpreters=${TOREE_INTERPRETERS} "--toree_opts=${TOREE_OPTS}" "--spark_opts=${SPARK_OPTS}"
  checkSuccess $LINENO "-  jupyter toree install"
fi

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
