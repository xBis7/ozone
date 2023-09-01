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
Documentation       Smoke test for creating data needed for om bootstrap load test.
Resource            ../commonlib.robot
Test Timeout        5 minutes
Test Setup          Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit test user     testuser     testuser.keytab

*** Variables ***
${VOLUME}           vol1
${BUCKET}           bucket1
${TMP_FILE}         tmp.txt

*** Keywords ***
Create volume and bucket
    [arguments]         ${volume}           ${bucket}
    ${vol_res} =        Execute             ozone sh volume create /${volume}
                        Should Be Empty     ${vol_res}
    ${bucket_res} =     Execute             ozone sh bucket create /${volume}/${bucket}
                        Should Be Empty     ${bucket_res}

Create tmp file
    [arguments]         ${file_name}
    ${create_res} =     Execute             touch ${file_name}
                        Should Be Empty     ${create_res}
    ${ls_grep_res} =    Execute             ls -lah | grep '${file_name}'
                        Should contain      ${ls_grep_res}      ${file_name}

Delete tmp file
    [arguments]         ${file_name}
    ${delete_res} =     Execute             rm ${file_name}
                        Should Be Empty     ${delete_res}
    ${ls_grep_res} =    Execute             ls -lah | grep '${file_name}'
                        Should Be Empty     ${ls_grep_res}

Create a key and set contents same as the keyName
    [arguments]         ${key_name}        ${key_prefix}       ${volume}       ${bucket}        ${file_name}
    Execute             > tmp.txt
    Execute             echo '${key_prefix}/${key_name}' >> tmp.txt
    ${key_res} =        Execute             ozone sh key put /${volume}/${bucket}/${key_prefix}/${key_name} ${file_name}
                        Should Be Empty     ${key_res}
    ${key_cat_res} =    Execute             ozone sh key cat /${volume}/${bucket}/${key_prefix}/${key_name}
                        Should contain      ${key_cat_res}      ${key_prefix}/${key_name}

Create actual keys
    [arguments]         ${key_num}          ${key_prefix}       ${volume}       ${bucket}       ${file_name}
    FOR         ${counter}      IN RANGE    ${key_num}
        Create a key and set contents same as the keyName       key${counter}       ${key_prefix}       ${volume}       ${bucket}       ${file_name}
    END

Create key metadata
    [arguments]         ${threads}          ${key_num}          ${volume}       ${bucket}
    ${freon_res} =      Execute             ozone freon omkg -t ${threads} -n ${key_num} -v ${volume} -b ${bucket}
                        Should contain      ${freon_res}        Successful executions: ${key_num}

Create snapshot
    [arguments]         ${volume}           ${bucket}       ${snapshot}
    ${snap_res} =        Execute            ozone sh snapshot create /${volume}/${bucket} ${snapshot}
                        Should Be Empty     ${snap_res}

*** Test Cases ***
Volume-bucket init
    Create volume and bucket        ${VOLUME}       ${BUCKET}

Create 100 key metadata under /${VOLUME}/${BUCKET}
    Create key metadata     10      100     ${VOLUME}       ${BUCKET}

Create snapshot 'snap0'
    Create snapshot         ${VOLUME}       ${BUCKET}       snap0

Create tmp file to be used for key creation
    Create tmp file         ${TMP_FILE}

Create 2 actual keys with prefix 'sn1', key contents the same as the key name
    Create actual keys      2               sn1             ${VOLUME}       ${BUCKET}       ${TMP_FILE}

Create snapshot 'snap1'
    Create snapshot         ${VOLUME}       ${BUCKET}       snap1

Cleanup tmp file
    Delete tmp file         ${TMP_FILE}
