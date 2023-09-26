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

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

iterations=$1

execute_command_in_container om1 ozone sh bucket create /s3v/bucket1 || true

counter=0
while [[ $counter -lt $iterations ]]
do
  execute_command_in_container om1 bash -c 'export AWS_ACCESS_KEY=mid400keysPer5s/s3oz3 AWS_SECRET_KEY=pass && ozone freon s3kg -t 10 -n 400 -e http://s3g:9878'
  sleep 5

  counter=$(($counter+1))

  echo "Finished iteration '$counter'."
done
