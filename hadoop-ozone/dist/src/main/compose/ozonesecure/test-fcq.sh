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

#suite:secure

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

export SECURITY_ENABLED=true

# Append the FairCallQueue configs to docker-config
# `ozone.om.transport.class` shouldn't already be defined in docker-config
# If `OZONE_OM_PORT_DEFAULT = 9862` changes,
# then the port used below should change as well
tee -a ./docker-config << END

# FairCallQueue configs
CORE-SITE.XML_ipc.9862.callqueue.impl=org.apache.hadoop.ipc.FairCallQueue
CORE-SITE.XML_ipc.9862.scheduler.impl=org.apache.hadoop.ipc.DecayRpcScheduler
CORE-SITE.XML_ipc.9862.identity-provider.impl=org.apache.hadoop.ozone.om.helpers.OzoneIdentityProvider
CORE-SITE.XML_ipc.9862.scheduler.priority.levels=2
CORE-SITE.XML_ipc.9862.backoff.enable=true
CORE-SITE.XML_ipc.9862.faircallqueue.multiplexer.weights=2,1
CORE-SITE.XML_ipc.9862.decay-scheduler.thresholds=50

OZONE-SITE.XML_ozone.om.transport.class=org.apache.hadoop.ozone.om.protocolPB.Hadoop3OmTransportFactory
END

start_docker_env

execute_robot_test s3g fcq/s3_om_fcq.robot

stop_docker_env

# Remove the FairCallQueue configs from docker-config
head -n -10 ./docker-config > tmp.txt && mv tmp.txt docker-config

generate_report
