# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

*** Settings ***
Documentation       Smoke test for validating data and snapshots after om bootstrap.
Resource            ../commonlib.robot
Test Timeout        10 minutes
Test Setup          Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit test user     testuser     testuser.keytab

*** Variables ***
${BOOTSTRAPPED_OM}      om4
${VOLUME}               vol1
${BUCKET}               bucket1
${SNAP_1}               snap1
${SNAP_2}               snap2
${KEY_PREFIX}           sn
${KEY_1}                key1
${KEY_2}                key2

*** Keywords ***
Number of checkpoints is higher than 0
    ${checkpoints} =    Execute                 ls -lah /data/om/db.snapshots/checkpointState | grep 'om.db-' | wc -l
                        Should be true          ${checkpoints} > 0

Transfer leadership to OM
    [arguments]         ${new_leader}
    ${result} =         Execute                 ozone admin om transfer --service-id=omservice -n ${new_leader}
                        Should Contain          ${result}               Transfer leadership successfully

Check snapshots on OM
    [arguments]         ${volume}           ${bucket}       ${snap_1}       ${snap_2}
    ${snap1_res} =      Execute             ozone sh snapshot list /${volume}/${bucket} | grep ${snap_1}
                        Should contain      ${snap1_res}        ${snap_1}
    ${snap2_res} =      Execute             ozone sh snapshot list /${volume}/${bucket} | grep ${snap_2}
                        Should contain      ${snap2_res}        ${snap_2}

Run snap diff and validate results
    [arguments]         ${volume}           ${bucket}       ${snap_1}       ${snap_2}       ${key_1}        ${key_2}
    ${diff_res}         Execute             ozone sh snapshot diff /${volume}/${bucket} ${snap_1} ${snap_2}
    ${key_num}          Execute             ${diff_res} | grep 'key' | wc -l
                        Should be true      ${key_num} == 2
    ${diff_key1}        Execute             ${diff_res} | grep ${key_1} | wc -l
                        Should be true      ${diff_key1} == 1
    ${diff_key2}        Execute             ${diff_res} | grep ${key_2} | wc -l
                        Should be true      ${diff_key2} == 1

Validate keys under snapshot
    [arguments]         ${volume}           ${bucket}       ${snap}         ${key_prefix}       ${key_1}        ${key_2}
    ${key1_res}         Execute             ozone sh key cat /${volume}/${bucket}/.snapshot/${snap}/${key_prefix}/${key_1}
                        Should contain      ${key1_res}     ${key_prefix}/${key_1}
    ${key2_res}         Execute             ozone sh key cat /${volume}/${bucket}/.snapshot/${snap}/${key_prefix}/${key_2}
                        Should contain      ${key2_res}     ${key_prefix}/${key_2}

*** Test Cases ***
Check number of checkpoints made
    Wait Until Keyword Succeeds     3min        10sec            Number of checkpoints is higher than 0

Transfer leadership to '${BOOTSTRAPPED_OM}'
    Transfer leadership to OM       ${BOOTSTRAPPED_OM}

Snapshots exist on '${BOOTSTRAPPED_OM}'
    Check snapshots on OM           ${VOLUME}       ${BUCKET}       ${SNAP_1}       ${SNAP_2}

Run snap diff on '${BOOTSTRAPPED_OM}' and check diff keys
    Run snap diff and validate results              ${VOLUME}       ${BUCKET}       ${SNAP_1}       ${SNAP_2}           ${KEY_1}        ${KEY_2}

Cat snapshot keys and validate contents
    Validate keys under snapshot                    ${VOLUME}       ${BUCKET}       ${SNAP_2}       ${KEY_PREFIX}       ${KEY_1}        ${KEY_2}
