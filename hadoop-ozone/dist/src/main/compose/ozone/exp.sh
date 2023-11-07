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

export OZONE_REPLICATION_FACTOR=3

docker-compose up -d --scale datanode=6

docker-compose exec -T scm ozone sh volume create /vol1
docker-compose exec -T scm ozone sh bucket create /vol1/bucket1
docker-compose exec -T scm ozone sh key put /vol1/bucket1/key1 /etc/hosts

docker-compose exec -T scm ozone admin scm roles
docker-compose exec -T scm ozone admin datanode list
docker-compose exec -T scm ozone admin container info 1

echo ""
echo "ozone admin datanode decommission -id=scmservice --scm=172.20.0.6:9894 172.20.0.8/ozone-datanode-2.ozone_default"

docker compose exec scm bash

#docker-compose stop datanode_1
#docker-compose stop datanode_2
#docker-compose stop datanode_3
#
#docker-compose exec -T om ozone admin container report

