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

docker-compose up --scale datanode=3 -d

# Create the volume and the buckets
echo ""
echo "hadoop | create /vol1 | exp_res: empty"
docker-compose exec -T om ozone sh volume create /vol1
echo ""
echo "hadoop | create /vol1/fsobucket | exp_res: empty"
docker-compose exec -T om ozone sh bucket create /vol1/fsobucket -l FILE_SYSTEM_OPTIMIZED
echo ""
echo "hadoop | create /vol1/legbucket | exp_res: empty"
docker-compose exec -T om ozone sh bucket create /vol1/legbucket -l LEGACY

# Set ACLs
echo ""
echo "hadoop | set /vol1 ACLs | exp_res: ACLs set successfully"
docker-compose exec -T om ozone sh volume setacl -a user:testuser2:rw,user:testuser:a,group:testuser2:rw,group:testuser:a vol1
echo ""
echo "hadoop | set /vol1/fsobucket ACLs | exp_res: ACLs set successfully"
docker-compose exec -T om ozone sh bucket setacl -a user:testuser2:rwlc,user:testuser:a,group:testuser2:rwlc,group:testuser:a vol1/fsobucket
echo ""
echo "hadoop | set /vol1/legbucket ACLs | exp_res: ACLs set successfully"
docker-compose exec -T om ozone sh bucket setacl -a user:testuser2:rwlc,user:testuser:a,group:testuser2:rwlc,group:testuser:a vol1/legbucket

# Create files and dirs as testuser
echo ""
echo "testuser | fs -mkdir /vol1/fsobucket/dir1 | exp_res: empty"
docker-compose exec -T -u testuser om ozone fs -mkdir /vol1/fsobucket/dir1
echo ""
echo "testuser | fs -put /vol1/fsobucket/dir1/readme | exp_res: empty"
docker-compose exec -T -u testuser om ozone fs -put ./README.md /vol1/fsobucket/dir1/readme
echo ""
echo "testuser2 | fs -put /vol1/fsobucket/dir1/license | exp_res: empty"
docker-compose exec -T -u testuser2 om ozone fs -put ./LICENSE.txt /vol1/fsobucket/dir1/license

echo ""
echo "testuser | fs -mkdir /vol1/legbucket/dir1 | exp_res: empty"
docker-compose exec -T -u testuser om ozone fs -mkdir /vol1/legbucket/dir1
echo ""
echo "testuser | fs -put /vol1/legbucket/dir1/readme | exp_res: empty"
docker-compose exec -T -u testuser om ozone fs -put ./README.md /vol1/legbucket/dir1/readme
echo ""
echo "testuser2 | fs -put /vol1/legbucket/dir1/license | exp_res: empty"
docker-compose exec -T -u testuser2 om ozone fs -put ./LICENSE.txt /vol1/legbucket/dir1/license

# Delete under FSO bucket
echo ""
echo "testuser | fs -ls /vol1/fsobucket/dir1 | exp_res: 2 keys"
docker-compose exec -T -u testuser om ozone fs -ls /vol1/fsobucket/dir1
echo ""
echo "testuser2 | fs -rm -r -skipTrash /vol1/fsobucket/dir1 | exp_res: Input/output error"
docker-compose exec -T -u testuser2 om ozone fs -rm -r -skipTrash /vol1/fsobucket/dir1 || true
echo ""
echo "testuser | fs -ls /vol1/fsobucket/dir1 | exp_res: 1 key"
docker-compose exec -T -u testuser om ozone fs -ls /vol1/fsobucket/dir1

# Delete under Legacy bucket
echo ""
echo "testuser | fs -ls /vol1/legbucket/dir1 | exp_res: 2 keys"
docker-compose exec -T -u testuser om ozone fs -ls /vol1/legbucket/dir1
echo ""
echo "testuser2 | fs -rm -r -skipTrash /vol1/legbucket/dir1 | exp_res: Deleted"
docker-compose exec -T -u testuser2 om ozone fs -rm -r -skipTrash /vol1/legbucket/dir1 || true
echo ""
echo "testuser | fs -ls /vol1/legbucket/dir1 | exp_res: 1 key"
docker-compose exec -T -u testuser om ozone fs -ls /vol1/legbucket/dir1

docker-compose down
