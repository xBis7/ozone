#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#suite:HA-unsecure

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

export SECURITY_ENABLED=false
export OZONE_REPLICATION_FACTOR=3
export SCM=scm1
export OM_SERVICE_ID=omservice

writeKeys() {
  writes=$1
  threads=$2
  prefix=$3

  random=$((1 + $RANDOM % 100))

  # Run the commands in the background
  docker exec ozone-ha-datanode-1 ozone freon omkg -t "$threads" -n "$writes" -v vol1 -b bucket1 -p "$prefix$random" &
  pid_dn1=$!

  echo "$prefix$random" >> ./key_prefixes.txt

  random=$(($random+1))

  docker exec ozone-ha-datanode-2 ozone freon omkg -t "$threads" -n "$writes" -v vol1 -b bucket1 -p "$prefix$random" &
  pid_dn2=$!

  echo "$prefix$random" >> ./key_prefixes.txt

	random=$(($random+1))

  docker exec ozone-ha-datanode-3 ozone freon omkg -t "$threads" -n "$writes" -v vol1 -b bucket1 -p "$prefix$random" &
  pid_dn3=$!

  echo "$prefix$random" >> ./key_prefixes.txt
  echo "--" >> ./key_prefixes.txt
  # Wait for all commands to finish
  wait $pid_dn1 $pid_dn2 $pid_dn3
}

NUM_KEYS=$1
NUM_KEYS_PER_SNAPSHOT=$2
NUM_SNAPSHOTS=$3

> key_prefixes.txt

docker-compose up --scale datanode=3 -d

leader_res=$(docker-compose exec om1 ozone admin om roles -id=omservice | grep 'LEADER')

while [[ $leader_res != *"LEADER"* ]]
do
  echo "waiting for OM leader..."
  leader_res=$(docker-compose exec om1 ozone admin om roles -id=omservice | grep 'LEADER')
done

# make sure that the leader is always, om1
if [[ $leader_res != *"om1"*  ]]
then
  echo "Current leader is not om1, transferring leadership..."
  docker-compose exec om1 ozone admin om transfer -id=omservice -n=om1
fi



# Initial key writes
while [[  $(expr $NUM_KEYS % 3) != 0 ]]
do
	echo "NUM_KEYS=$NUM_KEYS isn't an odd num, decr..."
	NUM_KEYS=$(($NUM_KEYS-1))
done

keys_per_client=$(($NUM_KEYS/3))

writeKeys "$keys_per_client" 100 "sn0"

# Keys per snapshot
while [[  $(expr $NUM_KEYS_PER_SNAPSHOT % 3) != 0 ]]
do
	echo "NUM_KEYS_PER_SNAPSHOT=$NUM_KEYS_PER_SNAPSHOT isn't an odd num, decr..."
	NUM_KEYS_PER_SNAPSHOT=$(($NUM_KEYS_PER_SNAPSHOT-1))
done

snap_keys_per_client=$(($NUM_KEYS_PER_SNAPSHOT/3))

# clear the file
> ./snaps.txt

snap_inc=$((1 + $RANDOM % 10))

counter=0
while [[ $counter -lt $NUM_SNAPSHOTS ]]
do
  writeKeys "$snap_keys_per_client" 5 "sn$snap_inc"
  docker-compose exec -T om1 ozone sh snapshot create /vol1/bucket1 "snap$snap_inc"

  echo "snap$snap_inc" >> ./snaps.txt

	# Prefix number and snapshot number
	snap_inc=$(($snap_inc+1))

  counter=$(($counter+1))
  echo "finished $counter iteration"
done

docker-compose exec -T om2 /opt/hadoop/bin/ozone om --init
docker-compose exec -T om2 /opt/hadoop/bin/ozone om


