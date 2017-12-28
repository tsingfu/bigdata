export XLEARNING_HOME="$(cd "`dirname "$0"`"/..; pwd)"
export JAVA_HOME={{java64_home}}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-"/etc/hadoop"}

export XLEARNING_CONF_DIR=$XLEARNING_HOME/conf/
export XLEARNING_CLASSPATH="$XLEARNING_CONF_DIR:$HADOOP_CONF_DIR"

for f in $XLEARNING_HOME/lib/*.jar; do
    export XLEARNING_CLASSPATH=$XLEARNING_CLASSPATH:$f
done

export XLEARNING_CLIENT_OPTS="-Xmx1024m"
