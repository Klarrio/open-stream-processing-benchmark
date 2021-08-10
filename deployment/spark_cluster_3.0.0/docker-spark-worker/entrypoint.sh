#!/bin/bash
set -e

## Defaults
#
: ${SPARK_HOME:?must be set!}
default_opts="--properties-file /spark-defaults.conf"


# Check if CLI args list containes bind address key.
cli_bind_address() {
  echo "$*" | grep -qE -- "--host\b|-h\b|--ip\b|-i\b"
}

# Set permissions on the scratch volumes
scratch_volumes_permissions() {
  mkdir -p $SPARK_HOME/work && chown $SPARK_USER:hadoop $SPARK_HOME/work
  chmod 1777 /tmp
}


## Configuration sourcing
. $SPARK_HOME/sbin/spark-config.sh
. $SPARK_HOME/bin/load-spark-env.sh





## Entrypoint

scratch_volumes_permissions

instance=worker
CLASS="org.apache.spark.deploy.$instance.${instance^}"

# Handle custom bind address set via ENV or CLI
eval bind_address=$SPARK_MASTER_IP
echo $bind_address
if ( ! cli_bind_address $@ ) && [ ! -z $bind_address ] ; then
default_opts="${default_opts} --host ${bind_address}"
fi

echo "==> spark-class invocation arguments: $CLASS $default_opts $@"
echo "==> spark-class invocation arguments: $CLASS "
echo "==> spark-class invocation arguments: $default_opts"
echo "==> spark-class invocation arguments: $@"

cd /tmp

exec gosu $SPARK_USER:hadoop $SPARK_HOME/bin/spark-class $CLASS --webui-port 7778 --cores $CORES --memory $MEMORY --properties-file $SPARK_HOME/conf/spark-defaults.conf spark://spark-master.marathon.mesos:7077
;;
shell)
shift
echo "==> spark-shell invocation arguments: $default_opts $@"

cd /tmp
exec gosu $SPARK_USER:hadoop $SPARK_HOME/bin/spark-shell $default_opts $@
;;
*)
cmdline="$@"
exec ${cmdline:-/bin/bash}
;;
