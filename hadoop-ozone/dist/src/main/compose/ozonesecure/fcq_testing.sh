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

#suite:secure

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

export SECURITY_ENABLED=true

: ${OZONE_BUCKET_KEY_NAME:=key1}

docker-compose up -d --scale datanode=3

writeKeys() {
  threads=$1
  keys=$2

  # This will be "om", "dn", "scm".
  node=$3
  iterations=$4

  container="$node"
  keytab_name="$node"

  if [[ $node == "dn" ]]; then
    container="datanode"
  fi

  counter=0

  while [[ $counter -lt $iterations ]]
  do
    # Run the commands in the background
    docker exec ozonesecure-"$container"-1 kinit -kt /etc/security/keytabs/testuser.keytab testuser/"$keytab_name"@EXAMPLE.COM
    docker exec ozonesecure-"$container"-1 ozone freon omkg -t "$threads" -n "$keys" -v vol1 -b bucket1

    sleep 1

    counter=$(($counter+1))

    echo "Finished iteration '$counter' on node '$node'"
  done
}

#writeKeysS3G() {
#
#}

# 1 user on the OM writing 1000 keys, sleeping 1 sec and then repeating.
writeKeys 100 1000 om 1000 &
pid_om=$!

# 1 user on the datanode writing 1.000.000 keys, sleeping 1 sec and then repeating.
writeKeys 100 1000000 dn 1000 &
pid_dn=$!

# 1 user on the S3G writing 100.000 keys, sleeping 1 sec and then repeating.
# writeKeysS3G 100 100000 1000 &
# pid_s3g=$!

wait $pid_om $pid_dn
