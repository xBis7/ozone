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
Documentation       S3 gateway test with aws cli
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ./commonawslib.robot

*** Variables ***
${S3G_URL}                      http://s3g:9878
${OM_URL}                       http://${OM_SERVICE_ID}:9874
${OM_JMX_ENDPOINT}              ${OM_URL}/jmx
${DECAY_RPC_SCHED_METRICS}      DecayRpcSchedulerMetrics2

*** Keywords ***
Setup headers
    Kinit test user    testuser    testuser.keytab
    Setup secure v4 headers

Setup aws credentials
    ${accessKey} =      Execute     aws configure get aws_access_key_id
    ${secret} =         Execute     aws configure get aws_secret_access_key
    Set Environment Variable        AWS_SECRET_ACCESS_KEY  ${secret}
    Set Environment Variable        AWS_ACCESS_KEY_ID  ${accessKey}

Setup bucket1
    Execute AWSS3APICli             create-bucket --bucket bucket1

Freon s3kg
    [arguments]         ${prefix}=s3bg          ${n}=100000         ${threads}=100         ${args}=${EMPTY}
    ${result} =         Execute                  ozone freon s3kg -e ${S3G_URL} -t ${threads} -n ${n} -p ${prefix} ${args}
                        Should contain          ${result}           Successful executions: ${n}

Setup headers, credentials and bucket1
    Setup headers
    Setup aws credentials
    Setup bucket1

*** Test Cases ***
Run freon s3kg
    Setup headers, credentials and bucket1
    Freon s3kg

Get metrics samples
    Execute         echo "-----start-----" >> xbis-logs.txt
    FOR     ${index}     IN RANGE    0    3000
            Wait Until Keyword Succeeds         90sec           3sec        Execute         curl --negotiate -u : -LSs ${OM_JMX_ENDPOINT} | sed -n '/${DECAY_RPC_SCHED_METRICS}/,/}/p' | grep 'Caller(' >> xbis-logs.txt
                                                                            Execute         curl --negotiate -u : -LSs ${OM_JMX_ENDPOINT} | sed -n '/${DECAY_RPC_SCHED_METRICS}/,/}/p' | grep 'Priority.0.AvgResponseTime' >> xbis-logs.txt
                                                                            Execute         curl --negotiate -u : -LSs ${OM_JMX_ENDPOINT} | sed -n '/${DECAY_RPC_SCHED_METRICS}/,/}/p' | grep 'Priority.1.AvgResponseTime' >> xbis-logs.txt
                                                                            Execute         echo "-----" >> xbis-logs.txt
    END
    Execute         echo "-----end-----" >> xbis-logs.txt
