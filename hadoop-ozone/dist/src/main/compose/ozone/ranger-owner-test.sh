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

# Create 'vol1' volume and 'bucket1' bucket
echo ""
echo "hadoop | volume create /vol1 | exp_res: empty"
docker-compose exec -T om ozone sh volume create /vol1

echo ""
echo "hadoop | bucket create /vol1/bucket1 | exp_res: empty"
docker-compose exec -T om ozone sh bucket create /vol1/bucket1 -l "$bucket_layout"

# Create files and dirs as testuser
echo ""
echo "testuser | key put /vol1/bucket1/rootkey1 | exp_res: empty"
docker-compose exec -T -u testuser om ozone sh key put /vol1/bucket1/rootkey1 README.md

echo ""
echo "testuser | key put /vol1/bucket1/dir1/key1 | exp_res: empty"
docker-compose exec -T -u testuser om ozone sh key put /vol1/bucket1/dir1/key1 README.md

echo ""
echo "testuser | key put /vol1/bucket1/dir1/dir2/key2 | exp_res: empty"
docker-compose exec -T -u testuser om ozone sh key put /vol1/bucket1/dir1/dir2/key2 README.md

# Create a file as testuser2
echo ""
echo "testuser2 | key put /vol1/bucket1/rootkey2 | exp_res: empty"
docker-compose exec -T -u testuser2 om ozone sh key put /vol1/bucket1/rootkey2 LICENSE.txt

if [[ "$bucket_layout" == "OBJECT_STORE" ]]
then
  echo ""
  echo "testuser2 | sh key delete /vol1/bucket1/dir1/dir2/key2 | exp_res: 'PERMISSION_DENIED'"
  docker-compose exec -T -u testuser2 om ozone sh key delete /vol1/bucket1/dir1/dir2/key2 || true

  echo ""
  echo "testuser2 | sh key delete /vol1/bucket1/dir1/key1 | exp_res: 'PERMISSION_DENIED'"
  docker-compose exec -T -u testuser2 om ozone sh key delete /vol1/bucket1/dir1/key1 || true

  echo ""
  echo "testuser2 | sh key delete /vol1/bucket1/rootkey1 | exp_res: 'PERMISSION_DENIED'"
  docker-compose exec -T -u testuser2 om ozone sh key delete /vol1/bucket1/rootkey1 || true

  echo ""
  echo "testuser2 | sh bucket delete /vol1/bucket1 | exp_res: 'PERMISSION_DENIED'"
  docker-compose exec -T -u testuser2 om ozone sh bucket delete /vol1/bucket1 || true

  # 'testuser2' can delete his own file
  echo ""
  echo "testuser2 | sh key delete /vol1/bucket1/rootkey2 | exp_res: empty"
  docker-compose exec -T -u testuser2 om ozone sh key delete /vol1/bucket1/rootkey2

  # Repeat above deletes as testuser
  echo ""
  echo "testuser | sh key delete /vol1/bucket1/dir1/dir2/key2 | exp_res: empty"
  docker-compose exec -T -u testuser om ozone sh key delete /vol1/bucket1/dir1/dir2/key2

  echo ""
  echo "testuser | sh key delete /vol1/bucket1/dir1/key1 | exp_res: empty"
  docker-compose exec -T -u testuser om ozone sh key delete /vol1/bucket1/dir1/key1

  echo ""
  echo "testuser | sh key delete /vol1/bucket1/rootkey1 | exp_res: empty"
  docker-compose exec -T -u testuser om ozone sh key delete /vol1/bucket1/rootkey1

  echo ""
  echo "testuser | sh bucket delete /vol1/bucket1 | exp_res: 'is deleted'"
  docker-compose exec -T -u testuser om ozone sh bucket delete /vol1/bucket1
else
  echo ""
  echo "testuser2 | fs -rm -skipTrash /vol1/bucket1/dir1/dir2/key2 | exp_res: 'Input/output error'"
  docker-compose exec -T -u testuser2 om ozone fs -rm -skipTrash /vol1/bucket1/dir1/dir2/key2 || true

  echo ""
  echo "testuser2 | fs -rm -r -skipTrash /vol1/bucket1/dir1/dir2 | exp_res: 'Input/output error'"
  docker-compose exec -T -u testuser2 om ozone fs -rm -r -skipTrash /vol1/bucket1/dir1/dir2 || true

  echo ""
  echo "testuser2 | fs -rm -r -skipTrash /vol1/bucket1/dir1 | exp_res: 'Input/output error'"
  docker-compose exec -T -u testuser2 om ozone fs -rm -r -skipTrash /vol1/bucket1/dir1 || true

  echo ""
  echo "testuser2 | fs -rm -skipTrash /vol1/bucket1/dir1/key1 | exp_res: 'Input/output error'"
  docker-compose exec -T -u testuser2 om ozone fs -rm -skipTrash /vol1/bucket1/dir1/key1 || true

  echo ""
  echo "testuser2 | fs -rm -skipTrash /vol1/bucket1/rootkey1 | exp_res: 'Input/output error'"
  docker-compose exec -T -u testuser2 om ozone fs -rm -skipTrash /vol1/bucket1/rootkey1 || true

  # 'testuser2' can delete his own file
  echo ""
  echo "testuser2 | fs -rm -skipTrash /vol1/bucket1/rootkey2 | exp_res: 'Deleted'"
  docker-compose exec -T -u testuser2 om ozone fs -rm -skipTrash /vol1/bucket1/rootkey2

  # Repeat above deletes as testuser
  echo ""
  echo "testuser | fs -rm -skipTrash /vol1/bucket1/dir1/dir2/key2 | exp_res: 'Deleted'"
  docker-compose exec -T -u testuser om ozone fs -rm -skipTrash /vol1/bucket1/dir1/dir2/key2

  echo ""
  echo "testuser | fs -rm -skipTrash /vol1/bucket1/dir1/key1 | exp_res: 'Deleted'"
  docker-compose exec -T -u testuser om ozone fs -rm -skipTrash /vol1/bucket1/dir1/key1

  echo ""
  echo "testuser | fs -rm -r -skipTrash /vol1/bucket1/dir1/dir2 | exp_res: 'Deleted'"
  docker-compose exec -T -u testuser om ozone fs -rm -r -skipTrash /vol1/bucket1/dir1/dir2

  echo ""
  echo "testuser | fs -rm -r -skipTrash /vol1/bucket1/dir1 | exp_res: 'Deleted'"
  docker-compose exec -T -u testuser om ozone fs -rm -r -skipTrash /vol1/bucket1/dir1

  echo ""
  echo "testuser | fs -rm -skipTrash /vol1/bucket1/rootkey1 | exp_res: 'Deleted'"
  docker-compose exec -T -u testuser om ozone fs -rm -skipTrash /vol1/bucket1/rootkey1

  echo ""
  echo "testuser | fs -rm -r -skipTrash /vol1/bucket1 | exp_res: 'Deleted'"
  docker-compose exec -T -u testuser om ozone fs -rm -r -skipTrash /vol1/bucket1
fi


