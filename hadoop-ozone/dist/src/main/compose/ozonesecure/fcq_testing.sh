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

writeKeysOM() {
  threads=$1
  keys=$2
  iterations=$3
  sleep_secs=$4

  docker exec ozonesecure-om-1 kinit -kt /etc/security/keytabs/om.keytab om/om@EXAMPLE.COM

  counter=0

  while [[ $counter -lt $iterations ]]
  do
    docker exec ozonesecure-datanode-1 ozone freon omkg -t "$threads" -n "$keys" -v vol1 -b bucket1

    sleep "$sleep_secs"

    counter=$(($counter+1))

    echo "Finished iteration '$counter' on node '$node'"
  done
}

writeKeysDn() {
  threads=$1
  keys=$2
  iterations=$3
  sleep_secs=$4

  docker exec ozonesecure-datanode-1 kinit -kt /etc/security/keytabs/testuser.keytab testuser/dn@EXAMPLE.COM

  counter=0

  while [[ $counter -lt $iterations ]]
  do
    docker exec ozonesecure-datanode-1 ozone freon omkg -t "$threads" -n "$keys" -v vol1 -b bucket1

    sleep "$sleep_secs"

    counter=$(($counter+1))

    echo "Finished iteration '$counter' on node '$node'"
  done
}

configureS3Cred() {
  getsecret=$(docker-compose exec -T s3g ozone s3 getsecret)

  awsAccessKey=$($getsecret | grep 'awsAccessKey' | awk -F '[=]' '{ print $2 }')
  awsSecret=$($getsecret | grep 'awsSecret' | awk -F '[=]' '{ print $2 }')

  docker-compose exec -T s3g export AWS_ACCESS_KEY="$awsAccessKey" AWS_SECRET_KEY="$awsSecret"

  docker-compose exec -T s3g aws configure set default.s3.signature_version s3v4
  docker-compose exec -T s3g aws configure set aws_access_key_id "$awsAccessKey"
  docker-compose exec -T s3g aws configure set aws_secret_access_key "$awsSecret"
  docker-compose exec -T s3g aws configure set region us-west-1
}

writeKeysS3G() {
  threads=$1
  keys=$2
  iterations=$3
  sleep_secs=$4

  docker-compose exec -T s3g kinit -kt /etc/security/keytabs/s3g.keytab s3g/s3g@EXAMPLE.COM
  getsecret=$(docker-compose exec -T s3g ozone s3 getsecret)

  awsAccessKey=$($getsecret | grep 'awsAccessKey' | awk -F '[=]' '{ print $2 }')
  awsSecret=$($getsecret | grep 'awsSecret' | awk -F '[=]' '{ print $2 }')

  counter=0

  while [[ $counter -lt $iterations ]]
  do
    docker-compose exec -e AWS_ACCESS_KEY="$awsAccessKey" -e AWS_SECRET_KEY="$awsSecret" s3g ozone freon s3bg -t "$threads" -n "$keys"
    sleep "$sleep_secs"
    counter=$(($counter+1))

    echo "Finished iteration '$counter' on 's3g'"
  done
}

sleep 30

# threads, keys, iterations, sleep
writeKeysOM 100 1000 1000 1 &
pid_om=$!

# threads, keys, iterations, sleep
writeKeysDn 100 100000 1000 5 &
pid_dn=$!

# threads, keys, iterations, sleep
#writeKeysS3G 100 10000 1000 3 &
#pid_s3g=$!

#wait $pid_om $pid_dn pid_s3g
wait $pid_om $pid_dn
