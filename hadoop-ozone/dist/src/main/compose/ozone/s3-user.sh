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

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

username=$1
threads=$2
keys=$3
sleep_secs=$4
iterations=$5

execute_command_in_container om ozone sh bucket create /s3v/bucket1 || true

counter=0
while [[ $counter -lt $iterations ]]
do
  docker exec -it ozone-datanode-1 bash -c "export AWS_ACCESS_KEY=$username AWS_SECRET_KEY=pass && ozone freon s3kg -t $threads -n $keys -e http://s3g:9878"
  sleep "$sleep_secs"

  counter=$(($counter+1))

  echo "Finished iteration '$counter'."
done
