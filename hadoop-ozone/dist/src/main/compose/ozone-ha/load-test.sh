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
  keys=$1

  writes=$(($keys/4))
  threads=$(($writes-10))

  # Run the commands in the background
  docker-compose exec -T om1 ozone freon omkg -t "$threads" -n "$writes" -v vol1 -b bucket1 &
  pid_om1=$!

  docker-compose exec -T om2 ozone freon omkg -t "$threads" -n "$writes" -v vol1 -b bucket1 &
  pid_om2=$!

  docker-compose exec -T scm1 ozone freon omkg -t "$threads" -n "$writes" -v vol1 -b bucket1 &
  pid_scm1=$!

  docker-compose exec -T scm2 ozone freon omkg -t "$threads" -n "$writes" -v vol1 -b bucket1 &
  pid_scm2=$!

  # Wait for all commands to finish
  wait $pid_om1 $pid_om2 $pid_scm1 $pid_scm2
}

NUM_KEYS=$1
NUM_SNAPSHOTS=$2

docker-compose up --scale datanode=3 -d

leader_res=$(docker-compose exec om1 ozone admin om roles -id=omservice | grep 'LEADER')

while [[ $leader_res != *"LEADER"* ]]
do
  echo "waiting for OM leader..."
  leader_res=$(docker-compose exec om1 ozone admin om roles -id=omservice | grep 'LEADER')
done

writeKeys $NUM_KEYS

i=0
while [[ $i -lt $NUM_SNAPSHOTS ]]
do
  writeKeys 100000
  docker-compose exec -T om1 ozone sh snapshot create /vol1/bucket1 "snap$i"
  i=$(($i+1))
  echo "finished $i iteration"
done

docker-compose exec -T om3 /opt/hadoop/bin/ozone om --init
docker-compose exec -T om3 /opt/hadoop/bin/ozone om


