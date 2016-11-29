#!/bin/bash

###########################
# storm.zookeeper.servers #
###########################
ZOOKEEPER_SERVERS_ESCAPED=
if ! [ -z "$STORM_ZOOKEEPER_SERVERS" ]; then
    # All ZooKeeper server IPs in an array
    IFS=', ' read -r -a ZOOKEEPER_SERVERS_ARRAY <<< "$STORM_ZOOKEEPER_SERVERS"
    for index in "${!ZOOKEEPER_SERVERS_ARRAY[@]}"
    do
        ZOOKEEPER_SERVERS_ESCAPED=$ZOOKEEPER_SERVERS_ESCAPED,"\\\"${ZOOKEEPER_SERVERS_ARRAY[index]}\\\""
    done
    ZOOKEEPER_SERVERS_ESCAPED=[${ZOOKEEPER_SERVERS_ESCAPED:1}]
    ZOOKEEPER_SERVERS_ESCAPED=" -c storm.zookeeper.servers=\"$ZOOKEEPER_SERVERS_ESCAPED\""
fi

########################
# storm.local.hostname #
########################
HOST=" -c storm.local.hostname=$(hostname -i | awk '{print $1;}')"
# For the nimbus, apply "nimbus" as default hostname
for arg in "$@"
do
    if [[ $arg == "nimbus" ]] ; then
        HOST=" -c storm.local.hostname=\"nimbus\""
    fi
done

##########################
# supervisor.slots.ports #
##########################
SUPERVISOR_SLOTS=
# For a supervisor, set worker slots
for arg in "$@"
do
    if [[ $arg == "supervisor" ]] ; then
        SUPERVISOR_SLOTS=" -c supervisor.slots.ports=\"[6700,6701,6702,6703]\""
    fi
done

################
# nimbus.seeds #
################
NIMBUS_SEEDS=" -c nimbus.seeds=\"[\\\"nimbus\\\"]\""

###################################################
# Make sure provided arguments are not overridden #
###################################################
for arg in "$@"
do
    if [[ $arg == *"storm.zookeeper.servers"* ]] ; then
        ZOOKEEPER_SERVERS_ESCAPED=
    fi
    if [[ $arg == *"storm.local.hostname"* ]] ; then
        HOST=
    fi
    if [[ $arg == *"supervisor.slots.ports"* ]] ; then
        SUPERVISOR_SLOTS=
    fi
    if [[ $arg == *"nimbus.seeds"* ]] ; then
        NIMBUS_SEEDS=
    fi
    if [[ $arg == *"nimbus.host"* ]] ; then
        NIMBUS_SEEDS=
    fi
done

CMD="exec bin/storm $@$NIMBUS_SEEDS$SUPERVISOR_SLOTS$HOST$ZOOKEEPER_SERVERS_ESCAPED & /usr/sbin/sshd -D"

echo "$CMD"
eval "$CMD"
# start the server:
#/bin/bash bin/zkServer.sh start-foreground & /usr/sbin/sshd -D
