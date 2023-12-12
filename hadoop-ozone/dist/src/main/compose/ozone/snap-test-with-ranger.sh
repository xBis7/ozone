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
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either exp_resress or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

export COMPOSE_FILE=docker-compose.yaml:ranger.yaml

docker-compose up --scale datanode=3 -d

echo "sleep 15"
sleep 15

# Create 'tmp' volume and 'tmp' bucket
echo ""
echo "hadoop | volume create /vol1"
docker-compose exec -T om ozone sh volume create /vol1

echo ""
echo "hadoop | bucket create /vol1/bucket1"
docker-compose exec -T om ozone sh bucket create /vol1/bucket1

echo ""
echo "hadoop | key put /vol1/bucket1/key1"
docker-compose exec -T om ozone sh key put /vol1/bucket1/key1 README.md

echo ""
echo "testuser | snapshot create /vol1/bucket1 snap1"
docker-compose exec -T -u testuser om ozone sh snapshot create /vol1/bucket1 snap1



