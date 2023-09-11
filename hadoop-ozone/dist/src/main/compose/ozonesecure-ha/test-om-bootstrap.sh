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

#suite:HA-secure

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

export SECURITY_ENABLED=true
export OM_SERVICE_ID="omservice"
export SCM=scm1.org
export COMPOSE_FILE=docker-compose.yaml:om-bootstrap.yaml

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

start_docker_env

volume="vol1"
bucket="bucket1"
snap1="snap1"
snap2="snap2"
keyPrefix="sn"
key1="key1"
key2="key2"
bootstrap_om="om3"

execute_robot_test om1 kinit.robot

# Data creation
execute_robot_test om1 -v VOLUME:${volume} -v BUCKET:${bucket} -v SNAP_1:${snap1} -v SNAP_2:${snap2} -v KEY_PREFIX:${keyPrefix} -v KEY_1:${key1} -v KEY_2:${key2} omha/data-creation-before-om-bootstrap.robot

# Init om3 and start the om daemon in the background
execute_command_in_container om3 ozone om --init
execute_command_in_container -d om3 ozone om

execute_robot_test om3 kinit.robot

# Transferring leadership to the provided OM and validating data
execute_robot_test om3 -v BOOTSTRAPPED_OM:${bootstrap_om} -v VOLUME:${volume} -v BUCKET:${bucket} -v SNAP_1:${snap1} -v SNAP_2:${snap2} -v KEY_PREFIX:${keyPrefix} -v KEY_1:${key1} -v KEY_2:${key2} omha/data-validation-after-om-bootstrap.robot
