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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.service.helpers.ContainerKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.server.http.HttpConfig.getHttpPolicy;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_CLEANUP_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_CLEANUP_LIMIT_PER_TASK_DEFAULT;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTPS_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTP_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.http.HttpServer2.HTTPS_SCHEME;
import static org.apache.hadoop.http.HttpServer2.HTTP_SCHEME;

/**
 * Missing container cleanup service. For every container on the
 * MissingContainerTable, delete all keys in the OM and then
 * delete the container in the SCM.
 */
public class MissingContainerCleanupService extends BackgroundService {

  private static final Logger LOG =
      LoggerFactory.getLogger(MissingContainerCleanupService.class);
  private static final int CONTAINER_CLEANUP_CORE_POOL_SIZE = 1;
  private final AtomicBoolean suspended;
  private final OzoneManager ozoneManager;
  private final OMMetadataManager metadataManager;
  private final ScmBlockLocationProtocol scmBlockClient;
  private final StorageContainerLocationProtocol scmContainerClient;
  private final OzoneConfiguration conf;
  private final Table<String, OmKeyInfo> keyTable;
  private final Table<String, OmKeyInfo> fileTable;
  private final int containerCleanupLimitPerTask;
  private final AtomicLong serviceRunCount;
  private final AtomicLong deletedContainerCount;

  public MissingContainerCleanupService(
      long interval, long serviceTimeout,
      OzoneManager ozoneManager,
      ScmBlockLocationProtocol scmBlockClient,
      StorageContainerLocationProtocol scmClient,
      ConfigurationSource configuration) {
    super(MissingContainerCleanupService.class.getSimpleName(),
        interval, TimeUnit.MILLISECONDS,
        CONTAINER_CLEANUP_CORE_POOL_SIZE,
        serviceTimeout);
    this.ozoneManager = ozoneManager;
    this.scmBlockClient = scmBlockClient;
    this.scmContainerClient = scmClient;
    this.suspended = new AtomicBoolean(false);
    this.metadataManager = ozoneManager.getMetadataManager();
    this.conf = ozoneManager.getConfiguration();
    this.keyTable = metadataManager
        .getKeyTable(BucketLayout.LEGACY);
    this.fileTable = metadataManager
        .getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    this.containerCleanupLimitPerTask = configuration
        .getInt(OZONE_OM_OPEN_KEY_CLEANUP_LIMIT_PER_TASK,
            OZONE_OM_OPEN_KEY_CLEANUP_LIMIT_PER_TASK_DEFAULT);
    this.serviceRunCount = new AtomicLong(0);
    this.deletedContainerCount = new AtomicLong(0);
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new ContainerCleanupTask());
    return queue;
  }

  @VisibleForTesting
  public long getServiceRunCount() {
    return serviceRunCount.get();
  }

  @VisibleForTesting
  public long getDeletedContainerCount() {
    return deletedContainerCount.get();
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
      serviceRunCount.incrementAndGet();

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
          // Get container keys from Recon.
          List<ContainerKey> reconContainerKeys =
              getContainerKeysFromRecon(containerId);

          // Get OmKeyInfo list from Recon's key information.
          List<OmKeyInfo> omKeyInfoList =
              getContainerOmKeyInfoList(reconContainerKeys);

          // Delete keys in OM.
          deleteContainerKeys(omKeyInfoList);

          // Delete container in SCM container map.
          scmContainerClient.deleteContainer(containerId);
          LOG.info("Successfully cleaned up missing container " +
              containerId);

          deletedContainerCount.incrementAndGet();
        }

        // The container is cleaned up, remove from the table.
        // If the container isn't cleaned up, it's because it's not missing.
        // In any case, remove from the table.
        metadataManager.getMissingContainerTable()
            .delete(containerId);
      }

      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }

    @Override
    public int getPriority() {
      return BackgroundTask.super.getPriority();
    }

    private void deleteContainerKeys(List<OmKeyInfo> omKeyInfoList)
        throws IOException {
      DBStore dbStore = metadataManager.getStore();
      try (BatchOperation batchOperation = dbStore.initBatchOperation()) {
        for (OmKeyInfo keyInfo : omKeyInfoList) {
          // Get all container Ids for key
          List<Long> containerIds =
              getContainerIDsForKey(keyInfo);
          // Filter container Ids and get only the Ids
          // that don't belong to missing containers.
          List<Long> notMissingContainerIds =
              getNotMissingContainerIDsFromList(containerIds);
          // Get key blocks for not missing containers.
          List<BlockGroup> blockGroupList =
              getAvailableKeyBlocks(notMissingContainerIds, keyInfo);
          if (!blockGroupList.isEmpty()) {
            // Delete blocks.
            scmBlockClient.deleteKeyBlocks(blockGroupList);
          }
          String volumeName = keyInfo.getVolumeName();
          String bucketName = keyInfo.getBucketName();
          BucketLayout bucketLayout =
              getBucketLayoutForKey(volumeName, bucketName);

          Table<String, OmKeyInfo> table =
              bucketLayout.isFileSystemOptimized() ?
                  fileTable : keyTable;

          String key = getTableObjectKey(volumeName, bucketName,
              keyInfo.getKeyName(), bucketLayout);
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

    private BucketLayout getBucketLayoutForKey(
        String volumeName, String bucketName) throws IOException {
      String bucketKey = metadataManager
          .getBucketKey(volumeName, bucketName);
      return metadataManager.getBucketTable()
          .getIfExist(bucketKey).getBucketLayout();
    }

    private List<OmKeyInfo> getContainerOmKeyInfoList(
        List<ContainerKey> containerKeys) throws IOException {
      List<OmKeyInfo> keyInfoList = new ArrayList<>();

      for (ContainerKey containerKey : containerKeys) {
        String volumeName = containerKey.getVolume();
        String bucketName = containerKey.getBucket();
        String keyName = containerKey.getKey();

        BucketLayout bucketLayout =
            getBucketLayoutForKey(volumeName, bucketName);

        String objectKey = getTableObjectKey(volumeName,
            bucketName, keyName, bucketLayout);

        OmKeyInfo omKeyInfo = metadataManager
            .getKeyTable(bucketLayout).getIfExist(objectKey);

        if (Objects.nonNull(omKeyInfo)) {
          keyInfoList.add(omKeyInfo);
        }
      }
      return keyInfoList;
    }

    private String getTableObjectKey(
        String volumeName, String bucketName,
        String keyName, BucketLayout bucketLayout) throws IOException {
      String objectKey;
      if (bucketLayout.isFileSystemOptimized()) {
        Iterator<Path> pathComponents = Paths.get(keyName).iterator();
        long volumeId = metadataManager.getVolumeId(volumeName);
        long bucketId = metadataManager.getBucketId(volumeName,
            bucketName);
        long parentId = OMFileRequest.getParentID(volumeId, bucketId,
            pathComponents, keyName, metadataManager);


        String[] keyComponents = keyName.split(OM_KEY_PREFIX);
        int lastIndex = keyComponents.length - 1;
        String fileName =
            keyComponents.length > 1 ?
                keyComponents[lastIndex] : keyName;

        objectKey = metadataManager
            .getOzonePathKey(volumeId, bucketId, parentId, fileName);
      } else {
        objectKey = metadataManager
            .getOzoneKey(volumeName, bucketName, keyName);
      }
      return objectKey;
    }

    private List<ContainerKey> getContainerKeysFromRecon(long containerId)
        throws JsonProcessingException {
      StringBuilder urlBuilder = new StringBuilder();
      urlBuilder.append(getReconWebAddress())
          .append("/api/v1/containers/")
          .append(containerId)
          .append("/keys");
      String containerKeysResponse = "";
      try {
        containerKeysResponse = makeHttpCall(urlBuilder);
      } catch (Exception e) {
        LOG.error("Error getting container keys response from Recon");
      }

      if (Strings.isNullOrEmpty(containerKeysResponse)) {
        LOG.info("Container keys response from Recon is empty");
        // Return empty list
        return new LinkedList<>();
      }

      // Get the keys JSON array
      String keysJsonArray = containerKeysResponse.substring(
          containerKeysResponse.indexOf("keys\":") + 6,
          containerKeysResponse.length() - 1);

      final ObjectMapper objectMapper = new ObjectMapper();

      return objectMapper.readValue(keysJsonArray,
          new TypeReference<List<ContainerKey>>() { });
    }

    private String makeHttpCall(StringBuilder url)
        throws Exception {

      System.out.println("Connecting to Recon: " + url + " ...");
      final URLConnectionFactory connectionFactory =
          URLConnectionFactory.newDefaultURLConnectionFactory(conf);

      boolean isSpnegoEnabled = isHTTPSEnabled();
      HttpURLConnection httpURLConnection;

      try {
        httpURLConnection = (HttpURLConnection)
            connectionFactory.openConnection(new URL(url.toString()),
                isSpnegoEnabled);
        httpURLConnection.connect();
        int errorCode = httpURLConnection.getResponseCode();
        InputStream inputStream = httpURLConnection.getInputStream();

        if ((errorCode == HTTP_OK) || (errorCode == HTTP_CREATED)) {
          return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }

        if (httpURLConnection.getErrorStream() != null) {
          System.out.println("Recon is being initialized. " +
              "Please wait a moment");
          return null;
        } else {
          System.out.println("Unexpected null in http payload," +
              " while processing request");
        }
        return null;
      } catch (ConnectException ex) {
        System.err.println("Connection Refused. Please make sure the " +
            "Recon Server has been started.");
        return null;
      }
    }

    private String getReconWebAddress() {
      final String protocol;
      final HttpConfig.Policy webPolicy = getHttpPolicy(conf);

      final boolean isHostDefault;
      String host;

      if (webPolicy.isHttpsEnabled()) {
        protocol = HTTPS_SCHEME;
        host = conf.get(OZONE_RECON_HTTPS_ADDRESS_KEY,
            OZONE_RECON_HTTPS_ADDRESS_DEFAULT);
        isHostDefault = getHostOnly(host).equals(
            getHostOnly(OZONE_RECON_HTTPS_ADDRESS_DEFAULT));
      } else {
        protocol = HTTP_SCHEME;
        host = conf.get(OZONE_RECON_HTTP_ADDRESS_KEY,
            OZONE_RECON_HTTP_ADDRESS_DEFAULT);
        isHostDefault = getHostOnly(host).equals(
            getHostOnly(OZONE_RECON_HTTP_ADDRESS_DEFAULT));
      }

      if (isHostDefault) {
        // Fallback to <Recon RPC host name>:<Recon http(s) address port>
        final String rpcHost =
            conf.get(OZONE_RECON_ADDRESS_KEY, OZONE_RECON_ADDRESS_DEFAULT);
        host = getHostOnly(rpcHost) + ":" + getPort(host);
      }

      return protocol + "://" + host;
    }

    private String getHostOnly(String host) {
      return host.split(":", 2)[0];
    }

    private String getPort(String host) {
      return host.split(":", 2)[1];
    }

    private boolean isHTTPSEnabled() {
      return getHttpPolicy(conf) == HttpConfig.Policy.HTTPS_ONLY;
    }
  }
}
