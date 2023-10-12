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

bucket_layout=$1

if [[ "$bucket_layout" == "" ]]
then
  bucket_layout="FILE_SYSTEM_OPTIMIZED"
fi

export COMPOSE_FILE=docker-compose.yaml:ranger.yaml

docker-compose up --scale datanode=3 -d

# Create tmp volume and tmp bucket
echo ""
echo "hadoop | create /tmp vol | exp_res: empty"
docker-compose exec -T om ozone sh volume create /tmp

echo ""
echo "hadoop | create /tmp/tmp bucket | exp_res: empty"
docker-compose exec -T om ozone sh bucket create /tmp/tmp -l "$bucket_layout"

# Create files and dirs as testuser
echo ""
echo "testuser | fs -put ./README.md ofs://om/tmp | exp_res: empty"
docker-compose exec -T -u testuser om ozone fs -put ./README.md ofs://om/tmp

echo ""
echo "testuser | fs -mkdir ofs://om/tmp/dir1 | exp_res: empty"
docker-compose exec -T -u testuser om ozone fs -mkdir ofs://om/tmp/dir1

echo ""
echo "testuser | fs -mkdir ofs://om/tmp/dir1/dir2 | exp_res: empty"
docker-compose exec -T -u testuser om ozone fs -mkdir ofs://om/tmp/dir1/dir2

echo ""
echo "testuser | fs -put ./README.md ofs://om/tmp/dir1/key1 | exp_res: empty"
docker-compose exec -T -u testuser om ozone fs -put ./README.md ofs://om/tmp/dir1/key1

echo ""
echo "testuser | fs -put ./README.md ofs://om/tmp/dir1/dir2/key2 | exp_res: empty"
docker-compose exec -T -u testuser om ozone fs -put ./README.md ofs://om/tmp/dir1/dir2/key2

# Create a file as testuser2
echo ""
echo "testuser2 | fs -put ./LICENSE.txt ofs://om/tmp | exp_res: empty"
docker-compose exec -T -u testuser2 om ozone fs -put ./LICENSE.txt ofs://om/tmp

# Try to delete files and dirs owned by testuser as testuser2

# If testuser2 tries to recursively delete a directory and under that directory,
# there are files owned by him, then these files will be deleted. Delete for everything else will fail.
echo ""
echo "testuser2 | fs -rm -skipTrash ofs://om/tmp/dir1/dir2/key2 | exp_res: 'Input/output error'"
docker-compose exec -T -u testuser2 om ozone fs -rm -skipTrash ofs://om/tmp/dir1/dir2/key2 || true

echo ""
echo "testuser2 | fs -rm -r -skipTrash ofs://om/tmp/dir1/dir2 | exp_res: 'Input/output error'"
docker-compose exec -T -u testuser2 om ozone fs -rm -r -skipTrash ofs://om/tmp/dir1/dir2 || true

echo ""
echo "testuser2 | fs -rm -r -skipTrash ofs://om/tmp/dir1 | exp_res: 'Input/output error'"
docker-compose exec -T -u testuser2 om ozone fs -rm -r -skipTrash ofs://om/tmp/dir1 || true

echo ""
echo "testuser2 | fs -rm -skipTrash ofs://om/tmp/dir1/key1 | exp_res: 'Input/output error'"
docker-compose exec -T -u testuser2 om ozone fs -rm -skipTrash ofs://om/tmp/dir1/key1 || true

echo ""
echo "testuser2 | fs -rm -skipTrash ofs://om/tmp/README.md | exp_res: 'Input/output error'"
docker-compose exec -T -u testuser2 om ozone fs -rm -skipTrash ofs://om/tmp/README.md || true

# 'testuser2' can delete his own file
echo ""
echo "testuser2 | fs -rm -skipTrash ofs://om/tmp/LICENSE.txt | exp_res: 'Deleted'"
docker-compose exec -T -u testuser2 om ozone fs -rm -skipTrash ofs://om/tmp/LICENSE.txt

# Repeat above deletes as testuser
echo ""
echo "testuser | fs -rm -skipTrash ofs://om/tmp/dir1/dir2/key2 | exp_res: 'Deleted'"
docker-compose exec -T -u testuser om ozone fs -rm -skipTrash ofs://om/tmp/dir1/dir2/key2

echo ""
echo "testuser | fs -rm -skipTrash ofs://om/tmp/dir1/key1 | exp_res: 'Deleted'"
docker-compose exec -T -u testuser om ozone fs -rm -skipTrash ofs://om/tmp/dir1/key1

echo ""
echo "testuser | fs -rm -r -skipTrash ofs://om/tmp/dir1/dir2 | exp_res: 'Deleted'"
docker-compose exec -T -u testuser om ozone fs -rm -r -skipTrash ofs://om/tmp/dir1/dir2

echo ""
echo "testuser | fs -rm -r -skipTrash ofs://om/tmp/dir1 | exp_res: 'Deleted'"
docker-compose exec -T -u testuser om ozone fs -rm -r -skipTrash ofs://om/tmp/dir1

echo ""
echo "testuser | fs -rm -skipTrash ofs://om/tmp/README.md | exp_res: 'Deleted'"
docker-compose exec -T -u testuser om ozone fs -rm -skipTrash ofs://om/tmp/README.md

echo ""
echo "testuser | fs -rm -r -skipTrash ofs://om/tmp | exp_res: 'Deleted'"
docker-compose exec -T -u testuser om ozone fs -rm -r -skipTrash ofs://om/tmp


