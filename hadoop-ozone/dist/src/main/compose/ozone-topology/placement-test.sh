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

docker-compose up -d

echo 'sleep 15'
sleep 15

echo 'volume create'
docker-compose exec datanode_1 ozone sh volume create /vol1

echo 'bucket create'
docker-compose exec datanode_1 ozone sh bucket create /vol1/bucket1

echo 'key put RATIS 3'
docker-compose exec datanode_1 ozone sh key put /vol1/bucket1/key1 /etc/hosts -t=RATIS -r=THREE

echo '--'
echo ''
echo 'docker-compose exec scm ozone admin container info 1'
docker-compose exec scm ozone admin container info 1

echo ''
echo '--'

echo '--'
echo ''
echo 'docker-compose exec scm ozone admin scm roles'
docker-compose exec scm ozone admin scm roles

echo ''
echo '--'

echo '--'
echo ''
echo 'docker-compose exec scm ozone admin datanode list'
docker-compose exec scm ozone admin datanode list

echo ''
echo '--'

echo "docker-compose exec scm ozone admin datanode decommission -id=scmservice --scm=10.5.0.71:9894 "

echo ''
echo '--'

