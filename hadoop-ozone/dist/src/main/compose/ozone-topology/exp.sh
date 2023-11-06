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

docker-compose up -d

docker-compose exec -T scm ozone sh volume create /vol1
docker-compose exec -T scm ozone sh bucket create /vol1/bucket1
docker-compose exec -T scm ozone sh key put /vol1/bucket1/key1 /etc/hosts

docker-compose exec -T scm ozone admin scm roles
docker-compose exec -T scm ozone admin datanode list
docker-compose exec -T scm ozone admin container info 1

echo "ozone admin datanode decommission -id=scmservice --scm=10.5.0.71:9894 10.5.0.5/ozone-topology-datanode_2-1.ozone-topology_net"

docker compose exec scm bash

#docker-compose stop datanode_1
#docker-compose stop datanode_2
#docker-compose stop datanode_3
#
#docker-compose exec -T om ozone admin container report

