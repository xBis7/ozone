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
package org.apache.hadoop.ozone.om.response.key;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CleanupContainerResponse;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Tests for {@link OMContainerCleanupResponse}.
 */
public class TestOMContainerCleanupResponse {

  @TempDir
  private static File dbDir;

  private static OMMetadataManager omMetadataManager;
  private static BatchOperation batchOperation;
  private final long containerId =
      ThreadLocalRandom.current().nextLong(100);
  private ContainerInfo containerInfo;
  private OMResponse.Builder omResponse;

  @BeforeAll
  public static void init() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, dbDir.getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(conf);
    batchOperation = omMetadataManager.getStore().initBatchOperation();
  }

  @BeforeEach
  public void setUp() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    containerInfo = new ContainerInfo.Builder()
        .setContainerID(containerId)
        .setOwner(ugi.getShortUserName())
        .setReplicationConfig(ReplicationConfig
            .fromProtoTypeAndFactor(
                HddsProtos.ReplicationType.RATIS,
                HddsProtos.ReplicationFactor.THREE))
        .setState(HddsProtos.LifeCycleState.CLOSING)
        .setPipelineID(PipelineID.randomId())
        .build();

    omResponse = OMResponse.newBuilder()
        .setStatus(Status.OK)
        .setSuccess(true)
        .setCmdType(Type.CleanupContainer);
  }

  @AfterEach
  public void cleanUp() throws IOException {
    // Clean up the table after each test.
    try (TableIterator<Long,
        ? extends Table.KeyValue<Long, ContainerInfo>> iterator =
             omMetadataManager.getMissingContainerTable().iterator()) {
      while (iterator.hasNext()) {
        long id = iterator.next().getKey();
        omMetadataManager.getMissingContainerTable()
            .deleteWithBatch(batchOperation, id);
      }
      omMetadataManager.getStore()
          .commitBatchOperation(batchOperation);
    }

    Assertions.assertTrue(omMetadataManager
        .getMissingContainerTable().isEmpty());
  }

  @Test
  public void testAddToDBBatchSuccess() throws IOException {
    OMContainerCleanupResponse omContainerCleanupResponse =
        new OMContainerCleanupResponse(omResponse.build(),
            CleanupContainerResponse.StatusType.SUCCESS,
            containerId, containerInfo);

    omContainerCleanupResponse.addToDBBatch(omMetadataManager, batchOperation);

    Assertions.assertNull(omMetadataManager
        .getMissingContainerTable().getIfExist(containerId));

    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // Container should exist on the table.
    Assertions.assertNotNull(omMetadataManager
        .getMissingContainerTable().getIfExist(containerId));
  }

  @Test
  public void testAddToDBBatchFailure() throws IOException {
    OMContainerCleanupResponse omContainerCleanupResponse =
        new OMContainerCleanupResponse(omResponse.build(),
            CleanupContainerResponse
                .StatusType.CONTAINER_IS_NOT_MISSING);

    // If StatusType is not success,
    // container is not added to the table.
    omContainerCleanupResponse.addToDBBatch(omMetadataManager, batchOperation);

    Assertions.assertNull(omMetadataManager
        .getMissingContainerTable().getIfExist(containerId));

    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // Container shouldn't exist in the table.
    Assertions.assertNull(omMetadataManager
        .getMissingContainerTable().getIfExist(containerId));
  }
}
