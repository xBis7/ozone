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

#suite:HA-secure

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

export SECURITY_ENABLED=true
export OM_SERVICE_ID="omservice"
export SCM=scm1.org
export COMPOSE_FILE=docker-compose.yaml:om-bootstrap-load.yaml

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

start_docker_env

# Robot test for data creation

# bootstrap new om4
docker-compose up -d om4

echo "xbis: docker logs"
res=$(docker logs ozonesecure-ha-om4-1)

while [[ "$res" == *"Waiting for"* ]]
do
  echo "om4 not ready, waiting for scm3"
  sleep 10
  res=$(docker logs ozonesecure-ha-om4-1)
done

echo "xbis: docker logs"
docker logs ozonesecure-ha-om4-1

echo "xbis: kinit"
docker-compose exec -T om1 kinit -kt /etc/security/keytabs/testuser.keytab testuser/scm@EXAMPLE.COM

echo "xbis: freon"
docker-compose exec -T om1 ozone freon omkg -t 100 -n 1000000

# Avoid leaving it orphaned
stop_containers om4

# Robot test for checking data is installed on om4,
# transferring leadership to om4 and validating data

