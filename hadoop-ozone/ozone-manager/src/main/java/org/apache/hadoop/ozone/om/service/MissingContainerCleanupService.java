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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Missing container cleanup service. Delete container from
 * the SCM and then delete all its metadata in the OM.
 */
public class MissingContainerCleanupService extends BackgroundService {

  private static final int CONTAINER_CLEANUP_CORE_POOL_SIZE = 1;
  private final AtomicBoolean suspended;
  private final OzoneManager ozoneManager;
  private final ScmBlockLocationProtocol scmBlockClient;
  private final StorageContainerLocationProtocol scmContainerClient;

  public MissingContainerCleanupService(long interval, TimeUnit unit,
                                        long serviceTimeout,
                                        OzoneManager ozoneManager,
                                        ScmBlockLocationProtocol scmBlockClient,
                                        StorageContainerLocationProtocol scmContainerClient) {
    super(MissingContainerCleanupService.class.getSimpleName(), interval,
        unit, CONTAINER_CLEANUP_CORE_POOL_SIZE, serviceTimeout);
    this.ozoneManager = ozoneManager;
    this.scmBlockClient = scmBlockClient;
    this.scmContainerClient = scmContainerClient;
    this.suspended = new AtomicBoolean(false);
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

      long containerId = 0L;

      // When a datanode dies, all replicas are removed.
      // If the container is missing then the container's
      // replica list should be empty.
      List<HddsProtos.SCMContainerReplicaProto> replicas =
          scmContainerClient.getContainerReplicas(containerId,
              ClientVersion.CURRENT_VERSION);

      if (replicas.isEmpty()) {
        LOG.info("Verified Container " +
            containerId + " is missing.");
        // Delete keys in OM
        List<OmKeyInfo> omKeyInfoList = getContainerKeys(containerId);
        deleteContainerKeys(omKeyInfoList);

        // Delete container in SCM container map
        scmContainerClient.deleteContainer(containerId);
        LOG.info("Successfully cleaned up missing container " +
            containerId);
      } else {
        LOG.error("Provided ID doesn't belong to a missing container");
      }

      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }

    @Override
    public int getPriority() {
      return BackgroundTask.super.getPriority();
    }

    private List<OmKeyInfo> getContainerKeys(long containerId) throws IOException {
      List<OmKeyInfo> omKeyInfoList = new LinkedList<>();

      // FileTable
      try (TableIterator<String,
          ? extends Table.KeyValue<String, OmKeyInfo>>
               fileTableIterator = ozoneManager.getMetadataManager()
              .getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).iterator()) {
        while (fileTableIterator.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> entry = fileTableIterator.next();

          OmKeyInfo omKeyInfo = entry.getValue();
          List<Long> containerIds = omKeyInfo.getKeyLocationVersions().stream()
              .flatMap(v -> v.getLocationList().stream())
              .map(BlockLocationInfo::getContainerID).collect(Collectors.toList());

          if (containerIds.contains(containerId)) {
            omKeyInfoList.add(omKeyInfo);
          }
        }
      }

      // KeyTable
      try (TableIterator<String,
          ? extends Table.KeyValue<String, OmKeyInfo>>
               keyTableIterator = ozoneManager.getMetadataManager()
          .getKeyTable(BucketLayout.LEGACY).iterator()) {
        while (keyTableIterator.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> entry = keyTableIterator.next();

          OmKeyInfo omKeyInfo = entry.getValue();
          List<Long> containerIds = omKeyInfo.getKeyLocationVersions().stream()
              .flatMap(v -> v.getLocationList().stream())
              .map(BlockLocationInfo::getContainerID).collect(Collectors.toList());

          if (containerIds.contains(containerId)) {
            omKeyInfoList.add(omKeyInfo);
          }
        }
      }

      return omKeyInfoList;
    }

    private void deleteContainerKeys(List<OmKeyInfo> omKeyInfoList) {

    }
  }
}
