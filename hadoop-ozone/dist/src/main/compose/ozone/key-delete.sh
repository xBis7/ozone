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


docker-compose up --scale datanode=3 -d

docker-compose exec -T om ozone sh volume create /vol1

docker-compose exec -T om ozone sh bucket create /vol1/bucket1

docker-compose exec -T om ozone sh key put /vol1/bucket1/key1 README.md

docker exec -it ozone-om-1 bash

#num=$(docker-compose exec -T om ozone admin container list | grep 'numberOfKeys' | awk -F ':' '{print $2}' | awk -F ',' '{print $1}'| awk -F ' ' '{print $0}')
#
#while [[ $num == *"0"* ]]
#do
#  num=$(docker-compose exec -T om ozone admin container list | grep 'numberOfKeys' | awk -F ':' '{print $2}' | awk -F ',' '{print $1}'| awk -F ' ' '{print $0}')
#done
#
#docker-compose exec -T om ozone admin container list
#
#docker-compose exec -T om ozone fs -rm -skipTrash /vol1/bucket1/key1
#
#newnum=$(docker-compose exec -T om ozone admin container list | grep 'numberOfKeys' | awk -F ':' '{print $2}' | awk -F ',' '{print $1}'| awk -F ' ' '{print $0}')
#
#while [[ $newnum != *"0"* ]]
#do
#  num=$(docker-compose exec -T om ozone admin container list | grep 'numberOfKeys' | awk -F ':' '{print $2}' | awk -F ',' '{print $1}'| awk -F ' ' '{print $0}')
#done
#
#docker-compose exec -T om ozone admin container list

