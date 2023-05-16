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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OmTransportFactory;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.protocol.ClientId;
import picocli.CommandLine;

import java.io.IOException;
import java.util.concurrent.Callable;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;

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
      names = {"-cid", "--container-id"},
      description = "Missing Container ID",
      required = true
  )
  private String containerId;

  @Override
  public Void call() throws Exception {
    OzoneConfiguration conf = parent.getParent().getOzoneConf();
    try (OzoneManagerProtocol client =
             createOmClient(conf, omServiceId)) {
      long id = Long.parseLong(containerId);
      client.cleanupContainer(id);
    }
    return null;
  }

  public OzoneManagerProtocolClientSideTranslatorPB createOmClient(
      OzoneConfiguration conf, String omServiceID) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    RPC.setProtocolEngine(conf, OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);
    String clientId = ClientId.randomId().toString();

    if (omServiceID == null) {

      //if only one serviceId is configured, use that
      final String[] configuredServiceIds =
          conf.getTrimmedStrings(OZONE_OM_SERVICE_IDS_KEY);
      if (configuredServiceIds.length == 1) {
        omServiceID = configuredServiceIds[0];
      }
    }

    OmTransport transport = OmTransportFactory.create(conf, ugi, omServiceID);
    return new OzoneManagerProtocolClientSideTranslatorPB(transport, clientId);
  }
}
