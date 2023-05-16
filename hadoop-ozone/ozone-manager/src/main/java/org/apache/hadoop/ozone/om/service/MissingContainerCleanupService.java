/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.service;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.common.DeleteBlockGroupResult;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_CLEANUP_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_CLEANUP_LIMIT_PER_TASK_DEFAULT;

/**
 * Missing container cleanup service. For every container on the
 * MissingContainerTable, delete all keys in the OM and then
 * delete the container in the SCM.
 */
public class MissingContainerCleanupService extends AbstractKeyDeletingService {

  private static final Logger LOG =
      LoggerFactory.getLogger(MissingContainerCleanupService.class);
  private static final int CONTAINER_CLEANUP_CORE_POOL_SIZE = 1;
  private final AtomicBoolean suspended;
  private final OzoneManager ozoneManager;
  private final OMMetadataManager metadataManager;
  private final ScmBlockLocationProtocol scmBlockClient;
  private final StorageContainerLocationProtocol scmContainerClient;
  private final Table<String, OmKeyInfo> keyTable;
  private final Table<String, OmKeyInfo> fileTable;
  private final int containerCleanupLimitPerTask;

  public MissingContainerCleanupService(
      long interval, long serviceTimeout,
      OzoneManager ozoneManager,
      ScmBlockLocationProtocol scmBlockClient,
      StorageContainerLocationProtocol scmClient,
      ConfigurationSource configuration) {
    super(MissingContainerCleanupService.class.getSimpleName(),
        interval, TimeUnit.MILLISECONDS,
        CONTAINER_CLEANUP_CORE_POOL_SIZE,
        serviceTimeout, ozoneManager, scmBlockClient);
    this.ozoneManager = ozoneManager;
    this.scmBlockClient = scmBlockClient;
    this.scmContainerClient = scmClient;
    this.suspended = new AtomicBoolean(false);
    this.metadataManager = ozoneManager.getMetadataManager();
    this.keyTable = metadataManager
        .getKeyTable(BucketLayout.LEGACY);
    this.fileTable = metadataManager
        .getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    this.containerCleanupLimitPerTask = configuration
        .getInt(OZONE_OM_OPEN_KEY_CLEANUP_LIMIT_PER_TASK,
            OZONE_OM_OPEN_KEY_CLEANUP_LIMIT_PER_TASK_DEFAULT);
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new ContainerCleanupTask());
    return queue;
  }

  private boolean shouldRun() {
    return !suspended.get() && ozoneManager.isLeaderReady();
  }

  private class ContainerCleanupTask implements BackgroundTask {

    @Override
    public BackgroundTaskResult call() throws Exception {
      if (!shouldRun()) {
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      }
      OmMetadataManagerImpl omMetadataManager =
          (OmMetadataManagerImpl) metadataManager;
      List<Long> missingContainerIds = omMetadataManager
          .getPendingMissingContainersForCleanup(
              containerCleanupLimitPerTask);

      for (long containerId : missingContainerIds) {
        // When a datanode dies, all replicas are removed.
        // If the container is missing then the container's
        // replica list should be empty.
        List<HddsProtos.SCMContainerReplicaProto> replicas =
            scmContainerClient.getContainerReplicas(containerId,
                ClientVersion.CURRENT_VERSION);

        if (replicas.isEmpty()) {
          // Get container key list
          List<OmKeyInfo> omKeyInfoList = getContainerKeys(containerId);
          // Delete keys in OM
          deleteContainerKeys(omKeyInfoList);

          // Delete container in SCM container map
          scmContainerClient.deleteContainer(containerId);
          LOG.info("Successfully cleaned up missing container " +
              containerId);
        }

        // Remove the container from MissingContainerTable.
        metadataManager.getMissingContainerTable()
            .delete(String.valueOf(containerId));
      }

      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }

    @Override
    public int getPriority() {
      return BackgroundTask.super.getPriority();
    }

    private List<OmKeyInfo> getContainerKeys(long containerId)
        throws IOException {
      List<OmKeyInfo> omKeyInfoList = new LinkedList<>();

      // FileTable
      try (TableIterator<String,
          ? extends Table.KeyValue<String, OmKeyInfo>>
               fileTableIterator = fileTable.iterator()) {
        while (fileTableIterator.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> entry = fileTableIterator.next();

          OmKeyInfo omKeyInfo = entry.getValue();
          List<Long> containerIds = getContainerIDsForKey(omKeyInfo);

          if (containerIds.contains(containerId)) {
            omKeyInfoList.add(omKeyInfo);
          }
        }
      }

      // KeyTable
      try (TableIterator<String,
          ? extends Table.KeyValue<String, OmKeyInfo>>
               keyTableIterator = keyTable.iterator()) {
        while (keyTableIterator.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> entry = keyTableIterator.next();

          OmKeyInfo omKeyInfo = entry.getValue();
          List<Long> containerIds = getContainerIDsForKey(omKeyInfo);

          if (containerIds.contains(containerId)) {
            omKeyInfoList.add(omKeyInfo);
          }
        }
      }

      return omKeyInfoList;
    }

    private void deleteContainerKeys(List<OmKeyInfo> omKeyInfoList)
        throws IOException {
      DBStore dbStore = metadataManager.getStore();
      try (BatchOperation batchOperation = dbStore.initBatchOperation()) {
        for (OmKeyInfo keyInfo : omKeyInfoList) {
          // Get all container Ids for key
          List<Long> containerIds =
              getContainerIDsForKey(keyInfo);
          // Filter container Ids to remove Ids
          // belonging to missing containers.
          List<Long> notMissingContainerIds =
              getNotMissingContainerIDsFromList(containerIds);
          // Get key blocks for not missing containers.
          List<BlockGroup> blockGroupList =
              getAvailableKeyBlocks(notMissingContainerIds, keyInfo);
          if (!blockGroupList.isEmpty()) {
            // Delete blocks.
            List<DeleteBlockGroupResult> results = scmBlockClient
                .deleteKeyBlocks(blockGroupList);
            // Submit purge keys request for the keys
            // that their blocks got deleted.
//            submitPurgeKeysRequest(results, null);
          }
          BucketLayout bucketLayout = checkKeyBucketLayout(keyInfo);
          long volumeId = metadataManager
              .getVolumeId(keyInfo.getVolumeName());
          long bucketId = metadataManager
              .getBucketId(keyInfo.getVolumeName(),
                  keyInfo.getBucketName());

          long parentId = keyInfo.getParentObjectID();

          String fileName = bucketLayout
              .isFileSystemOptimized() ?
              keyInfo.getFileName() :
              keyInfo.getKeyName();

          Table<String, OmKeyInfo> table = bucketLayout
              .isFileSystemOptimized() ?
              fileTable :
              keyTable;

          String key = metadataManager
              .getOzonePathKey(volumeId, bucketId, parentId, fileName);
          table.deleteWithBatch(batchOperation, key);
        }
        dbStore.commitBatchOperation(batchOperation);
      }
    }

    private List<Long> getContainerIDsForKey(OmKeyInfo omKeyInfo) {
      return omKeyInfo.getKeyLocationVersions().stream()
          .flatMap(v -> v.getLocationList().stream())
          .map(BlockLocationInfo::getContainerID)
          .collect(Collectors.toList());
    }

    private List<Long> getNotMissingContainerIDsFromList(
        List<Long> containerIdList) throws IOException {
      List<Long> missingContainers = new ArrayList<>();
      for (long id : containerIdList) {
        if (scmContainerClient.getContainerReplicas(id,
            ClientVersion.CURRENT_VERSION).isEmpty()) {
          missingContainers.add(id);
        }
      }
      containerIdList.removeAll(missingContainers);

      return containerIdList;
    }

    /**
     * Returns a list of blocks that are associated with the
     * provided key and don't belong to missing containers.
     */
    private List<BlockGroup> getAvailableKeyBlocks(
        List<Long> notMissingContainerIdList, OmKeyInfo keyInfo) {
      List<BlockGroup> blockList = new ArrayList<>();
      for (OmKeyLocationInfoGroup keyLocations :
          keyInfo.getKeyLocationVersions()) {
        List<BlockID> blockIDs = keyLocations.getLocationList().stream()
            .map(b -> new BlockID(b.getContainerID(), b.getLocalID()))
            .collect(Collectors.toList());

        List<BlockID> missingContainerBlockIDs = new ArrayList<>();

        for (BlockID id : blockIDs) {
          // If BlockID doesn't exist on the list,
          // then it belongs to missing container.
          if (!notMissingContainerIdList.contains(id.getContainerID())) {
            missingContainerBlockIDs.add(id);
          }
        }

        for (BlockID id : missingContainerBlockIDs) {
          blockIDs.remove(id);
        }

        BlockGroup keyBlocks = BlockGroup.newBuilder()
            .setKeyName(keyInfo.getKeyName())
            .addAllBlockIDs(blockIDs)
            .build();
        blockList.add(keyBlocks);
      }
      return blockList;
    }

    private BucketLayout checkKeyBucketLayout(OmKeyInfo omKeyInfo)
        throws IOException {
      String bucketKey = metadataManager
          .getBucketKey(omKeyInfo.getVolumeName(),
              omKeyInfo.getBucketName());
      return metadataManager.getBucketTable()
          .getIfExist(bucketKey).getBucketLayout();
    }
  }
}
