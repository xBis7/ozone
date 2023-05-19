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
package org.apache.hadoop.ozone.om.service;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.mockito.Mockito.when;

/**
 * Tests for {@link MissingContainerCleanupService}.
 */
@Timeout(300)
public class TestMissingContainerCleanupService {

  @TempDir
  private static File dbDir;

  private static final OzoneConfiguration CONFIG =
      new OzoneConfiguration();
  private static final String VOLUME = "vol";
  private static final String BUCKET_FSO = "bucket_fso";
  private static final String BUCKET_LEGACY = "bucket_legacy";
  private static KeyManager keyManager;
  private static OzoneManagerProtocol omWriteClient;
  private static OMMetadataManager omMetadataManager;
  private static StorageContainerLocationProtocol scmContainerClient;
  private static MissingContainerCleanupService containerCleanupService;

  @BeforeAll
  public static void init()
      throws AuthenticationException, IOException {
    CONFIG.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        dbDir.getAbsolutePath());
    CONFIG.set(OzoneConfigKeys.OZONE_METADATA_DIRS,
        dbDir.getAbsolutePath());
    CONFIG.setTimeDuration(OzoneConfigKeys
        .OZONE_CONTAINER_CLEANUP_SERVICE_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    OmTestManagers omTestManagers = new OmTestManagers(CONFIG);
    OzoneManager ozoneManager = omTestManagers.getOzoneManager();
    keyManager = omTestManagers.getKeyManager();
    omWriteClient = omTestManagers.getWriteClient();
    omMetadataManager = omTestManagers.getMetadataManager();
    scmContainerClient = ozoneManager.getScmClient().getContainerClient();
    containerCleanupService = keyManager
        .getMissingContainerCleanupService();

    createVolumeAndBuckets();

    // Start MissingContainerCleanupService
    containerCleanupService.start();
  }

  /**
   * Block and container deletion are not tested in this method.
   */
  @ParameterizedTest
  @ValueSource(strings = {BUCKET_FSO, BUCKET_LEGACY})
  public void testContainerCleanupService(String bucketName)
      throws IOException, InterruptedException, TimeoutException {
    OmKeyInfo omKeyInfo = createKey(bucketName, 4);
    List<Long> containerIds = getContainerIdsForKey(omKeyInfo);

    // Allocating 4 blocks, there should be more than 1 container.
    Assertions.assertTrue(containerIds.size() > 1);

    // The first container is missing.
    long missingId = containerIds.get(0);
    setUpContainerStubs(missingId, true);

    for (int i = 1; i < containerIds.size(); i++) {
      long id = containerIds.get(i);
      setUpContainerStubs(id, false);
    }

    // Assert that getting replicas for the first container,
    // returns an empty list.
    Assertions.assertTrue(scmContainerClient
        .getContainerReplicas(missingId, ClientVersion.CURRENT_VERSION)
        .isEmpty());

    long volumeId = omMetadataManager.getVolumeId(VOLUME);
    long bucketId = omMetadataManager.getBucketId(VOLUME, bucketName);

    // RocksDB key.
    String key = omMetadataManager.getOzonePathKey(volumeId, bucketId,
        omKeyInfo.getParentObjectID(), omKeyInfo.getKeyName());

    BucketLayout bucketLayout =
        getLayoutForBucket(omKeyInfo.getBucketName());

    // The key should exist in RocksDB.
    Assertions.assertNotNull(omMetadataManager
        .getKeyTable(bucketLayout)
        .getIfExist(key));

    ContainerInfo missingContainerInfo =
        scmContainerClient.getContainer(missingId);

    // Put the missing container in MissingContainerTable.
    omMetadataManager.getMissingContainerTable()
        .put(missingId, missingContainerInfo);
    // Missing container should exist in the table.
    Assertions.assertNotNull(omMetadataManager
        .getMissingContainerTable().getIfExist(missingId));

    long currentServiceRunCount =
        containerCleanupService.getServiceRunCount();
    long currentDeletedContainerCount =
        containerCleanupService.getDeletedContainerCount();
    // Wait for the service to run again.
    GenericTestUtils.waitFor(
        () -> containerCleanupService
            .getServiceRunCount() > currentServiceRunCount,
        100, 1000);

    // Wait until the service has deleted the container.
    GenericTestUtils.waitFor(
        () -> containerCleanupService
            .getDeletedContainerCount() > currentDeletedContainerCount,
        100, 1000);

    // Key should no longer exist in RocksDB.
    Assertions.assertNull(omMetadataManager
        .getKeyTable(bucketLayout)
        .getIfExist(key));

    // The container should no longer exist in MissingContainerTable.
    Assertions.assertNull(omMetadataManager
        .getMissingContainerTable().getIfExist(missingId));
  }

  @Test
  public void testContainerNotMissingAnymore()
      throws IOException, InterruptedException, TimeoutException {
    OmKeyInfo omKeyInfo = createKey(BUCKET_FSO, 1);
    List<Long> containerIds = getContainerIdsForKey(omKeyInfo);

    for (long id : containerIds) {
      setUpContainerStubs(id, false);
    }

    long volumeId = omMetadataManager.getVolumeId(VOLUME);
    long bucketId = omMetadataManager.getBucketId(VOLUME, BUCKET_FSO);

    String key = omMetadataManager.getOzonePathKey(volumeId, bucketId,
        omKeyInfo.getParentObjectID(), omKeyInfo.getKeyName());

    // Key should exist in RocksDB.
    Assertions.assertNotNull(omMetadataManager
        .getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .getIfExist(key));

    long notMissingId = containerIds.get(0);
    ContainerInfo info = scmContainerClient
        .getContainer(notMissingId);

    // Put the container in MissingContainerTable.
    omMetadataManager.getMissingContainerTable()
        .put(notMissingId, info);
    // Container should exist in the table.
    Assertions.assertNotNull(omMetadataManager
        .getMissingContainerTable().getIfExist(notMissingId));

    long currentServiceRunCount =
        containerCleanupService.getServiceRunCount();
    long currentDeletedContainerCount =
        containerCleanupService.getDeletedContainerCount();

    // Wait for the service to run again.
    GenericTestUtils.waitFor(
        () -> containerCleanupService
            .getServiceRunCount() > currentServiceRunCount,
        100, 1000);

    // Container is not missing, there shouldn't be any deletes.
    // Count should be the same.
    Assertions.assertEquals(currentDeletedContainerCount,
        containerCleanupService.getDeletedContainerCount());

    // The container should no longer exist in MissingContainerTable.
    Assertions.assertNull(omMetadataManager
        .getMissingContainerTable().getIfExist(notMissingId));
  }

  private static void createVolumeAndBuckets() throws IOException {
    OMRequestTestUtils.addVolumeToOM(omMetadataManager,
        OmVolumeArgs.newBuilder()
            .setOwnerName("o")
            .setAdminName("a")
            .setVolume(VOLUME)
            .build());

    OMRequestTestUtils.addBucketToOM(omMetadataManager,
        OmBucketInfo.newBuilder().setVolumeName(VOLUME)
            .setBucketName(BUCKET_FSO)
            .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
            .setIsVersionEnabled(true)
            .build());

    OMRequestTestUtils.addBucketToOM(omMetadataManager,
        OmBucketInfo.newBuilder().setVolumeName(VOLUME)
            .setBucketName(BUCKET_LEGACY)
            .setBucketLayout(BucketLayout.LEGACY)
            .setIsVersionEnabled(true)
            .build());
  }

  private OmKeyInfo createKey(String bucketName, long numberOfBlocks)
      throws IOException {
    String keyName = String.format("key%s",
        RandomStringUtils.randomAlphanumeric(5));
    OmKeyArgs keyArg =
        new OmKeyArgs.Builder()
            .setVolumeName(VOLUME)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .setAcls(Collections.emptyList())
            .setReplicationConfig(StandaloneReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.ONE))
            .setLocationInfoList(new ArrayList<>())
            .build();
    //Open and Commit the Key in the KeyManager.
    OpenKeySession session = omWriteClient.openKey(keyArg);

    // Add pre-allocated blocks into args and avoid creating excessive block
    OmKeyLocationInfoGroup keyLocations = session.getKeyInfo().
        getLatestVersionLocations();
    Assertions.assertNotNull(keyLocations);
    List<OmKeyLocationInfo> latestBlocks = keyLocations.
        getBlocksLatestVersionOnly();
    int preAllocatedSize = latestBlocks.size();
    for (OmKeyLocationInfo block : latestBlocks) {
      keyArg.addLocationInfo(block);
    }

    // Allocate blocks.
    LinkedList<OmKeyLocationInfo> allocated = new LinkedList<>();
    for (int i = 0; i < numberOfBlocks - preAllocatedSize; i++) {
      allocated.add(omWriteClient.allocateBlock(keyArg, session.getId(),
          new ExcludeList()));
    }

    // Add blocks to be committed to KeyArgs.
    for (OmKeyLocationInfo block: allocated) {
      keyArg.addLocationInfo(block);
    }

    // Commit key.
    omWriteClient.commitKey(keyArg, session.getId());

    long volumeId = omMetadataManager.getVolumeId(VOLUME);
    long bucketId = omMetadataManager.getBucketId(VOLUME, bucketName);
    String key = omMetadataManager.getOzonePathKey(
        volumeId, bucketId, bucketId, keyName);
    OmKeyInfo omKeyInfo = keyManager.getKeyInfo(keyArg, "test");
    BucketLayout bucketLayout = getLayoutForBucket(bucketName);
    // Store key in RocksDB.
    omMetadataManager.getKeyTable(bucketLayout).put(key, omKeyInfo);

    return omKeyInfo;
  }

  private void setUpContainerStubs(long containerId,
                                   boolean containerMissing)
      throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    HddsProtos.LifeCycleState state = containerMissing ?
        HddsProtos.LifeCycleState.CLOSING :
        HddsProtos.LifeCycleState.OPEN;

    ContainerInfo containerInfo =
        new ContainerInfo.Builder()
            .setContainerID(containerId)
            .setOwner(ugi.getShortUserName())
            .setState(state)
            .setReplicationConfig(ReplicationConfig
                .fromProtoTypeAndFactor(
                    HddsProtos.ReplicationType.RATIS,
                    HddsProtos.ReplicationFactor.THREE))
            .build();

    HddsProtos.SCMContainerReplicaProto replicaProto =
        Mockito.mock(HddsProtos.SCMContainerReplicaProto.class);

    List<HddsProtos.SCMContainerReplicaProto> replicaList =
        containerMissing ?
            Collections.emptyList() :
            Collections.singletonList(replicaProto);

    when(scmContainerClient.getContainer(containerId))
        .thenReturn(containerInfo);
    when(scmContainerClient.getContainerReplicas(containerId,
        ClientVersion.CURRENT_VERSION))
        .thenReturn(replicaList);
  }

  private List<Long> getContainerIdsForKey(OmKeyInfo omKeyInfo) {
    return omKeyInfo.getKeyLocationVersions().stream()
        .flatMap(v -> v.getLocationList().stream())
        .map(BlockLocationInfo::getContainerID)
        .collect(Collectors.toList());
  }

  private BucketLayout getLayoutForBucket(String bucket)
      throws IOException {
    String bucketKey = omMetadataManager
        .getBucketKey(VOLUME, bucket);
    OmBucketInfo omBucketInfo = omMetadataManager
        .getBucketTable().getIfExist(bucketKey);
    Assertions.assertNotNull(omBucketInfo);
    return omBucketInfo.getBucketLayout();
  }
}
