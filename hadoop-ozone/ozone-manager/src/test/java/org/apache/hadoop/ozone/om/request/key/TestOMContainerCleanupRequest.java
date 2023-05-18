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
package org.apache.hadoop.ozone.om.request.key;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ScmClient;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CleanupContainerResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CleanupContainerRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CleanupContainerArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.mockito.Mockito.when;

/**
 * Tests for {@link OMContainerCleanupRequest}.
 */
public class TestOMContainerCleanupRequest {

  @TempDir
  private static File dbDir;

  private static final OzoneConfiguration CONFIG =
      new OzoneConfiguration();
  private static OzoneManager ozoneManager;

  private final OzoneManagerDoubleBufferHelper
      ozoneManagerDoubleBufferHelper =
        ((response, transactionIndex) -> {
          return null;
        });

  @BeforeAll
  public static void init() throws IOException {
    ozoneManager = Mockito.mock(OzoneManager.class);

    CONFIG.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        dbDir.getAbsolutePath());

    OMMetadataManager omMetadataManager =
        new OmMetadataManagerImpl(CONFIG);
    ScmClient scmClient = Mockito.mock(ScmClient.class);

    when(ozoneManager.getConfiguration())
        .thenReturn(CONFIG);
    when(ozoneManager.getMetadataManager())
        .thenReturn(omMetadataManager);
    when(ozoneManager.getScmClient())
        .thenReturn(scmClient);

    try (StorageContainerLocationProtocol containerClient =
        Mockito.mock(StorageContainerLocationProtocol.class)) {
      when(ozoneManager.getScmClient().getContainerClient())
          .thenReturn(containerClient);
    }
  }

  @Test
  public void testPreExecute() throws IOException {
    OMRequest omRequest = createContainerCleanupRequest();

    OMContainerCleanupRequest omContainerCleanupRequest =
        new OMContainerCleanupRequest(omRequest);

    OMRequest modifiedOmRequest = omContainerCleanupRequest
        .preExecute(ozoneManager);

    Assertions.assertEquals(omRequest.getCmdType(),
        modifiedOmRequest.getCmdType());
    Assertions.assertEquals(omRequest.getUserInfo(),
        modifiedOmRequest.getUserInfo());
    Assertions.assertEquals(
        omRequest.getCleanupContainerRequest()
            .getCleanupContainerArgs().getContainerId(),
        modifiedOmRequest.getCleanupContainerRequest()
            .getCleanupContainerArgs().getContainerId());
  }

  @Test
  public void testValidateAndUpdateCacheSuccess()
      throws IOException {
    OMRequest omRequest = doPreExecute();
    long containerId = getContainerIdFromRequest(omRequest);
    long transactionLogIndex = ThreadLocalRandom
        .current().nextLong(100);

    OMContainerCleanupRequest omContainerCleanupRequest =
        new OMContainerCleanupRequest(omRequest);

    when(ozoneManager.getScmClient()
        .getContainerClient()
        .getContainerReplicas(containerId,
            ClientVersion.CURRENT_VERSION))
        .thenReturn(Collections.emptyList());

    ContainerInfo containerInfo = getDummyContainerInfo(containerId);

    when(ozoneManager.getScmClient()
        .getContainerClient()
        .getContainer(containerId))
        .thenReturn(containerInfo);

    OMClientResponse omContainerCleanupResponse =
        omContainerCleanupRequest
            .validateAndUpdateCache(ozoneManager,
                transactionLogIndex, ozoneManagerDoubleBufferHelper);

    Assertions.assertEquals(
        CleanupContainerResponse.StatusType.SUCCESS,
        omContainerCleanupResponse.getOMResponse()
            .getCleanupContainerResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheContainerNotMissing()
      throws IOException {
    OMRequest omRequest = doPreExecute();
    long containerId = getContainerIdFromRequest(omRequest);
    long transactionLogIndex = ThreadLocalRandom
        .current().nextLong(100);

    OMContainerCleanupRequest omContainerCleanupRequest =
        new OMContainerCleanupRequest(omRequest);

    HddsProtos.SCMContainerReplicaProto replicaProto =
        Mockito.mock(HddsProtos.SCMContainerReplicaProto.class);

    when(ozoneManager.getScmClient()
        .getContainerClient()
        .getContainerReplicas(containerId,
            ClientVersion.CURRENT_VERSION))
        .thenReturn(Collections.singletonList(replicaProto));

    OMClientResponse omContainerCleanupResponse =
        omContainerCleanupRequest
            .validateAndUpdateCache(ozoneManager,
                transactionLogIndex, ozoneManagerDoubleBufferHelper);

    Assertions.assertEquals(CleanupContainerResponse
            .StatusType.CONTAINER_IS_NOT_MISSING,
        omContainerCleanupResponse.getOMResponse()
            .getCleanupContainerResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheContainerNotInSCM()
      throws IOException {
    OMRequest omRequest = doPreExecute();
    long containerId = getContainerIdFromRequest(omRequest);
    long transactionLogIndex = ThreadLocalRandom
        .current().nextLong(100);

    OMContainerCleanupRequest omContainerCleanupRequest =
        new OMContainerCleanupRequest(omRequest);

    when(ozoneManager.getScmClient()
        .getContainerClient()
        .getContainerReplicas(containerId,
            ClientVersion.CURRENT_VERSION))
        .thenReturn(Collections.emptyList());

    when(ozoneManager.getScmClient()
        .getContainerClient()
        .getContainer(containerId))
        .thenThrow(ContainerNotFoundException.class);

    OMClientResponse omContainerCleanupResponse =
        omContainerCleanupRequest
            .validateAndUpdateCache(ozoneManager,
                transactionLogIndex, ozoneManagerDoubleBufferHelper);

    Assertions.assertEquals(CleanupContainerResponse
            .StatusType.CONTAINER_NOT_FOUND_IN_SCM,
        omContainerCleanupResponse.getOMResponse()
            .getCleanupContainerResponse().getStatus());
  }

  private long getContainerIdFromRequest(OMRequest omRequest) {
    return omRequest
        .getCleanupContainerRequest()
        .getCleanupContainerArgs()
        .getContainerId();
  }

  private ContainerInfo getDummyContainerInfo(long containerId)
      throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    return new ContainerInfo.Builder()
        .setContainerID(containerId)
        .setOwner(ugi.getShortUserName())
        .setReplicationConfig(ReplicationConfig
            .fromProtoTypeAndFactor(
                HddsProtos.ReplicationType.RATIS,
                HddsProtos.ReplicationFactor.THREE))
        .setState(HddsProtos.LifeCycleState.CLOSING)
        .setPipelineID(PipelineID.randomId())
        .build();
  }

  private OMRequest doPreExecute() throws IOException {
    OMRequest omRequest = createContainerCleanupRequest();

    OMContainerCleanupRequest omContainerCleanupRequest =
        new OMContainerCleanupRequest(omRequest);

    return omContainerCleanupRequest
        .preExecute(ozoneManager);
  }

  private OMRequest createContainerCleanupRequest() {
    long containerId = ThreadLocalRandom.current().nextLong(100);

    CleanupContainerArgs containerArgs =
        CleanupContainerArgs.newBuilder()
            .setContainerId(containerId)
            .build();

    CleanupContainerRequest cleanupContainerRequest =
        CleanupContainerRequest.newBuilder()
            .setCleanupContainerArgs(containerArgs)
            .build();

    return OMRequest.newBuilder()
        .setCmdType(Type.CleanupContainer)
        .setClientId(UUID.randomUUID().toString())
        .setCleanupContainerRequest(cleanupContainerRequest)
        .build();
  }
}
