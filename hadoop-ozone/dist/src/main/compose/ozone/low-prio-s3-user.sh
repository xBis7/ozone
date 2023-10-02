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

execute_command_in_container om ozone sh bucket create /s3v/bucket1 || true

# 10 keys per min
for i in {0..2}
do
  docker exec -it ozone-datanode-2 bash -c "export AWS_ACCESS_KEY=low/oz2 AWS_SECRET_KEY=pass && ozone freon s3kg -t 3 -n 10 -e http://s3g:9878"
  sleep 60

  echo "Finished iteration '$i' / 10 keys"
done

# 100 keys per min
for i in {0..2}
do
  docker exec -it ozone-datanode-2 bash -c "export AWS_ACCESS_KEY=low/oz2 AWS_SECRET_KEY=pass && ozone freon s3kg -t 3 -n 100 -e http://s3g:9878"
  sleep 60

  echo "Finished iteration '$i' / 100 keys"
done

# 1000 keys per min
for i in {0..2}
do
  docker exec -it ozone-datanode-2 bash -c "export AWS_ACCESS_KEY=low/oz2 AWS_SECRET_KEY=pass && ozone freon s3kg -t 10 -n 1000 -e http://s3g:9878"
  sleep 60

  echo "Finished iteration '$i' / 1000 keys"
done

# 10000 keys per min
for i in {0..2}
do
  docker exec -it ozone-datanode-2 bash -c "export AWS_ACCESS_KEY=low/oz2 AWS_SECRET_KEY=pass && ozone freon s3kg -t 10 -n 10000 -e http://s3g:9878"
  sleep 60

  echo "Finished iteration '$i' / 10000 keys"
done

