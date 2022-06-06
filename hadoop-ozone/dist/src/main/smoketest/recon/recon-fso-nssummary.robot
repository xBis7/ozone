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
Documentation       Smoke test for Recon Namespace Summary Endpoint for FSO buckets.
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        5 minutes

*** Variables ***
${ENDPOINT_URL}             http://recon:9888
${API_ENDPOINT_URL}         ${ENDPOINT_URL}/api/v1
${ADMIN_NAMESPACE_URL}      ${API_ENDPOINT_URL}/namespace
${SUMMARY_URL}              ${ADMIN_NAMESPACE_URL}/summary
${DISK_USAGE_URL}           ${ADMIN_NAMESPACE_URL}/du
${QUOTA_USAGE_URL}          ${ADMIN_NAMESPACE_URL}/quota
${FILE_SIZE_DIST_URL}       ${ADMIN_NAMESPACE_URL}/dist
${volume}                   volume1
${bucket}                   bucket1

*** Keywords ***
Create volume
    ${result} =     Execute             ozone sh volume create /${volume}
                    Should not contain  ${result}       Failed
                    Sleep               10s

Create bucket
    ${result} =     Execute             ozone sh bucket create -l FILE_SYSTEM_OPTIMIZED /${volume}/${bucket}
                    Should not contain  ${result}       Failed
                    Sleep               30s

Create keys
    ${result} =     Execute             ozone sh key put /${volume}/${bucket}/file1 README.md
                    Should not contain  ${result}       Failed
    ${result} =     Execute             ozone sh key put /${volume}/${bucket}/dir1/dir2/file2 HISTORY.md
                    Should not contain  ${result}       Failed
    ${result} =     Execute             ozone sh key put /${volume}/${bucket}/dir1/dir3/file3 CONTRIBUTING.md
                    Should not contain  ${result}       Failed
    ${result} =     Execute             ozone sh key put /${volume}/${bucket}/dir1/dir4/file4 NOTICE.txt
                    Should not contain  ${result}       Failed
    ${result} =     Execute             ozone sh key put /${volume}/${bucket}/dir1/dir4/file5 LICENSE.txt
                    Should not contain  ${result}       Failed
                    Sleep               60s

Kinit as non admin
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user     scm     scm.keytab

Kinit as ozone admin
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user     testuser     testuser.keytab

Kinit as recon admin
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user     testuser2           testuser2.keytab

Check http return code
    [Arguments]         ${url}          ${expected_code}
    ${result} =         Execute                             curl --negotiate -u : --write-out '\%{http_code}\n' --silent --show-error --output /dev/null ${url}
                        IF  '${SECURITY_ENABLED}' == 'true'
                            Should contain      ${result}       ${expected_code}
                        ELSE
                            # All access should succeed without security.
                            Should contain      ${result}       200
                        END

*** Test Cases ***
Check volume creation
    Create volume

Check bucket creation
    Create bucket

Check keys creation
    Create keys

Check if Recon Web UI is up
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit HTTP user
    ${result} =         Execute                             curl --negotiate -u : -LSs ${ENDPOINT_URL}
                        Should contain      ${result}       Ozone Recon

Check Recon web UI access
    # Unauthenticated user cannot access web UI, but any authenticated user can.
    Execute    kdestroy
    Check http return code      ${ENDPOINT_URL}     401

    kinit as non admin
    Check http return code      ${ENDPOINT_URL}     200

Check Summary api access
    Execute    kdestroy
    Check http return code      ${SUMMARY_URL}?path=/       401

    kinit as non admin
    Check http return code      ${SUMMARY_URL}?path=/       403

    kinit as ozone admin
    Check http return code      ${SUMMARY_URL}?path=/       200

    kinit as recon admin
    Check http return code      ${SUMMARY_URL}?path=/       200

Check Disk Usage api access
    Execute    kdestroy
    Check http return code      ${DISK_USAGE_URL}?path=/       401

    kinit as non admin
    Check http return code      ${DISK_USAGE_URL}?path=/       403

    kinit as ozone admin
    Check http return code      ${DISK_USAGE_URL}?path=/       200

    kinit as recon admin
    Check http return code      ${DISK_USAGE_URL}?path=/       200

Check Quota Usage api access
    Execute    kdestroy
    Check http return code      ${QUOTA_USAGE_URL}?path=/       401

    kinit as non admin
    Check http return code      ${QUOTA_USAGE_URL}?path=/       403

    kinit as ozone admin
    Check http return code      ${QUOTA_USAGE_URL}?path=/       200

    kinit as recon admin
    Check http return code      ${QUOTA_USAGE_URL}?path=/       200

Check File Size Distribution api access
    Execute    kdestroy
    Check http return code      ${FILE_SIZE_DIST_URL}?path=/       401

    kinit as non admin
    Check http return code      ${FILE_SIZE_DIST_URL}?path=/       403

    kinit as ozone admin
    Check http return code      ${FILE_SIZE_DIST_URL}?path=/       200

    kinit as recon admin
    Check http return code      ${FILE_SIZE_DIST_URL}?path=/       200


Check Recon Namespace Summary Root
    ${result} =         Execute                             curl --negotiate -u : -LSs ${SUMMARY_URL}?path=/
                        Should contain      ${result}       OK
                        Should contain      ${result}       ROOT

Check Recon Namespace Summary Volume
    ${result} =         Execute                             curl --negotiate -u : -LSs ${SUMMARY_URL}?path=/${volume}
                        Should contain      ${result}       OK
                        Should contain      ${result}       VOLUME

Check Recon Namespace Summary Bucket
    ${result} =         Execute                             curl --negotiate -u : -LSs ${SUMMARY_URL}?path=/${volume}/${bucket}
                        Should contain      ${result}       OK
                        Should contain      ${result}       BUCKET

Check Recon Namespace Summary Key
    ${result} =         Execute                             curl --negotiate -u : -LSs ${SUMMARY_URL}?path=/${volume}/${bucket}/file1
                        Should contain      ${result}       OK
                        Should contain      ${result}       KEY

Check Recon Namespace Summary Directory
    ${result} =         Execute                             curl --negotiate -u : -LSs ${SUMMARY_URL}?path=/${volume}/${bucket}/dir1/dir2
                        Should contain      ${result}       OK
                        Should contain      ${result}       DIRECTORY

Check Recon Namespace Disk Usage
    ${result} =         Execute                             curl --negotiate -u : -LSs ${DISK_USAGE_URL}?path=/${volume}/${bucket}&files=true&replica=true
                        Should contain      ${result}       OK
                        Should contain      ${result}       \"sizeWithReplica\"
                        Should contain      ${result}       \"subPathCount\"
                        Should contain      ${result}       \"subPaths\"

Check Recon Namespace Volume Quota Usage
    ${result} =         Execute                             curl --negotiate -u : -LSs ${QUOTA_USAGE_URL}?path=/${volume}
                        Should contain      ${result}       OK
                        Should contain      ${result}       \"used\"

Check Recon Namespace Bucket Quota Usage
    ${result} =         Execute                             curl --negotiate -u : -LSs ${QUOTA_USAGE_URL}?path=/${volume}/${bucket}
                        Should contain      ${result}       OK
                        Should contain      ${result}       \"used\"

Check Recon Namespace File Size Distribution Root
    ${result} =         Execute                             curl --negotiate -u : -LSs ${FILE_SIZE_DIST_URL}?path=/
                        Should contain      ${result}       OK
                        Should contain      ${result}       \"dist\"
