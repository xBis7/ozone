/**
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
package org.apache.hadoop.ozone.admin.om;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CleanupContainerResponse;
import picocli.CommandLine;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Handler of ozone admin om containerCleanup command.
 */
@CommandLine.Command(
    name = "containerCleanup",
    description = "Command used for cleaning up " +
        "missing containers.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class ContainerCleanupSubCommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Option(
      names = {"-id", "--service-id"},
      description = "Ozone Manager Service ID"
  )
  private String omServiceId;

  @CommandLine.Option(
      names = {"-host", "--service-host"},
      description = "Ozone Manager Host"
  )
  private String omHost;

  @CommandLine.Option(
      names = {"-cid", "--container-id"},
      description = "Missing Container ID",
      required = true
  )
  private long containerId;

  @Override
  public Void call() throws Exception {
    try (OzoneManagerProtocol client =
             parent.createOmClient(omServiceId, omHost, false)) {
      CleanupContainerResponse.StatusType statusType;
      try {
        statusType =
            client.cleanupContainer(containerId);
      } catch (IOException ex) {
        System.out.println(ex.getMessage());
        statusType = CleanupContainerResponse
            .StatusType.CONTAINER_NOT_FOUND_IN_SCM;
      }

      if (statusType.equals(CleanupContainerResponse
          .StatusType.SUCCESS)) {
        System.out.println("Container with ID '" + containerId +
            "' was successfully submitted for cleanup.");
      } else if (statusType.equals(CleanupContainerResponse
          .StatusType.CONTAINER_IS_NOT_MISSING)) {
        System.out.println("Container with ID '" + containerId +
            "', is not missing.");
      } else if (statusType.equals(CleanupContainerResponse
          .StatusType.CONTAINER_NOT_FOUND_IN_SCM)) {
        System.out.println("Container with ID '" + containerId +
            "' doesn't exist.");
      }
    }
    return null;
  }
}
