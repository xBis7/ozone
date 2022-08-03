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
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        5 minutes

*** Variables ***
${SECURITY_ENABLED}  true

*** Keywords ***
Kinit as ozone admin
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user     testuser     testuser.keytab

Get and export cred
    ${result} =         Execute         eval $(ozone s3 getsecret -e)

Freon S3BG
    [arguments]     ${prefix}=s3bg      ${threads}=100      ${n}=5000       ${args}=${EMPTY}
    ${result} =        Execute          ozone freon s3bg -t ${threads} -n ${n} -p ${prefix} ${args}
                       Should contain   ${result}   Successful executions: ${n}

*** Test Cases ***

Kinit user
    Kinit as ozone admin

Check credentials
    Get and export cred

Run Freon s3bg test
    Freon S3BG