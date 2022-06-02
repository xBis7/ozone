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
${ENDPOINT_URL}         http://recon:9888
${NAMESPACE_URL}        ${ENDPOINT_URL}/namespace
${SUMMARY_URL}          ${NAMESPACE_URL}/summary
${DISK_USAGE_URL}       ${NAMESPACE_URL}/du
${QUOTA_USAGE_URL}      ${NAMESPACE_URL}/quota
${FILE_SIZE_DIST_URL}   ${NAMESPACE_URL}/dist
${volume}               volume1
${bucket}               bucket1

*** Keywords ***
Create volume
    ${result} =     Execute             ozone sh volume create /${volume} --user hadoop --space-quota 100TB --namespace-quota 100
                    Should not contain  ${result}       Failed

Create bucket
                    Execute             ozone sh bucket create -l FILE_SYSTEM_OPTIMIZED /${volume}/${bucket}

Create keys
                    Execute             ozone sh key put /${volume}/${bucket}/file1 README.md
                    Execute             ozone sh key put /${volume}/${bucket}/dir1/dir2/file2 HISTORY.md
                    Execute             ozone sh key put /${volume}/${bucket}/dir1/dir3/file3 CONTRIBUTING.md
                    Execute             ozone sh key put /${volume}/${bucket}/dir1/dir4/file4 NOTICE.txt
                    Execute             ozone sh key put /${volume}/${bucket}/dir1/dir4/file5 LICENSE.txt

Init
    Create volume
    Create bucket
    Create keys

*** Test Cases ***
Check system init
    Init

