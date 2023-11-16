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

sleep 30

docker-compose exec -T scm ozone freon ockg -t 10 -n 10
echo ""

docker-compose exec -T scm ozone admin datanode list
echo ""

docker-compose exec -T scm ozone admin scm roles
echo ""

echo "ozone admin container report"
echo ""
echo "ozone admin datanode decommission -id=scmservice --scm=10.5.0.71:9894"

docker-compose exec scm bash
