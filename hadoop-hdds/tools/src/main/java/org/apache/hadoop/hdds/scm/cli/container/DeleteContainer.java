/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.cli.container;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMDeleteUnhealthyContainerResponseProto;
import picocli.CommandLine;

import java.io.IOException;

/**
 * This is the handler that processes unhealthy
 * container deletion in SCM command.
 */
@CommandLine.Command(
    name = "delete",
    description = "Delete unhealthy container",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DeleteContainer extends ScmSubcommand {

  @CommandLine.Option(
      description = "Container ID",
      names = {"-cid", "--container-id"},
      required = true
  )
  private long containerID;

  @Override
  protected void execute(ScmClient client) throws IOException {
    SCMDeleteUnhealthyContainerResponseProto response =
        client.deleteUnhealthyContainerInSCM(containerID);
    String msg = response.getSuccess() ?
                     "Operation was successful." :
                     "Operation failed: " + response.getErrorMsg();
    System.out.println(msg);
  }
}
