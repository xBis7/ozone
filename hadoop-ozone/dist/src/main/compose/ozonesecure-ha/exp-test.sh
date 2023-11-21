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

echo "$> sleep"
sleep 30

echo "$> kinit"
docker-compose exec -T datanode1 kinit -kt /etc/security/keytabs/testuser.keytab testuser/dn@EXAMPLE.COM
echo "$> volume create"
docker-compose exec -T datanode1 ozone sh volume create /vol1
echo "$> bucket create"
docker-compose exec -T datanode1 ozone sh bucket create /vol1/bucket1
echo "$> key put"
docker-compose exec -T datanode1 ozone sh key put /vol1/bucket1/key1 /etc/hosts

echo "--o3fs--"
echo "--------"
docker-compose exec -T datanode1 ozone dtutil get o3fs://bucket1.vol1.omservice/key1 tokf
echo "$> ozone dtutil print tokf"
docker-compose exec -T datanode1 ozone dtutil print tokf
echo "$> strings tokf"
docker-compose exec -T datanode1 strings tokf

echo ""
echo "--ofs--"
echo "-------"
docker-compose exec -T datanode1 ozone dtutil get ofs://omservice/vol1/bucket1/key1 tokf1
echo "$> ozone dtutil print tokf1"
docker-compose exec -T datanode1 ozone dtutil print tokf1
echo "$> strings tokf1"
docker-compose exec -T datanode1 strings tokf1

