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
Documentation       Smoke test for listing om roles.
Resource            ../commonlib.robot
Resource            ./om-leader-transfer.robot
Test Timeout        10 minutes
Test Setup          Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit test user     testuser     testuser.keytab

*** Keywords ***

# Create a docker smoke test that does the same thing as your scripts but with 100 keys and 5 snapshots. Do it cleanly because you will be submitting it as a PR.

*** Test Cases ***
List om roles
    ${output} =         Execute          ozone admin om roles --service-id=omservice
                        Should Match Regexp   ${output}  [om (: LEADER|)]

List om roles as JSON
    ${output} =         Execute          ozone admin om roles --service-id=omservice --json
    ${leader} =         Execute          echo '${output}' | jq -r '.[] | select(.serverRole == "LEADER")'
                        Should Not Be Equal       ${leader}       ${EMPTY}
