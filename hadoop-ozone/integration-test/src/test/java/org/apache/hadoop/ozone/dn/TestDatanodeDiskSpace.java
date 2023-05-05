/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.hadoop.ozone.dn;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageTypeProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.volume.DbVolume;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;


import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_DATANODE_CONTAINER_DB_DIR;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CONTAINER_CACHE_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DB_NAME;

/**
 * This class tests datanode with a full disk.
 */
@Timeout(300)
public class TestDatanodeDiskSpace {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestDatanodeDiskSpace.class);
  private static final OzoneConfiguration CONF = new OzoneConfiguration();
  private static String DATANODE_UUID;
  private static final String CLUSTER_ID = UUID.randomUUID().toString();
  private static MiniOzoneCluster cluster;
  private static OzoneClient ozClient = null;
  private static ScmClient scmClient;
  private static ObjectStore store = null;
  private static OzoneVolume volume;
  private static OzoneBucket bucket;
  private static HddsDatanodeService datanodeService;

  @BeforeAll
  public static void setup() throws Exception {
    CONF.set(OZONE_SCM_CONTAINER_SIZE, "1GB");
    CONF.setStorageSize(OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN,
        0, StorageUnit.MB);
    CONF.setInt(OZONE_REPLICATION, ReplicationFactor.ONE.getValue());
    // keep the cache size = 1, so we could trigger io exception on
    // reading on-disk db instance
    CONF.setInt(OZONE_CONTAINER_CACHE_SIZE, 1);
    // Enable SchemaV3
    ContainerTestUtils.enableSchemaV3(CONF);
    // set tolerated = 1
    // shorten the gap between successive checks to ease tests
    DatanodeConfiguration dnConf =
        CONF.getObject(DatanodeConfiguration.class);
    dnConf.setFailedDataVolumesTolerated(1);
    dnConf.setDiskCheckMinGap(Duration.ofSeconds(5));
    CONF.setFromObject(dnConf);

    File workDir = GenericTestUtils.getTestDir();
    String dbPathStr = workDir.getAbsolutePath() + "/" +
        MiniOzoneClusterImpl.class.getSimpleName() +
        "-" + CLUSTER_ID + "/datanode-0/data-0";
    CONF.set(HDDS_DATANODE_CONTAINER_DB_DIR, dbPathStr);

    cluster = MiniOzoneCluster.newBuilder(CONF)
        .setClusterId(CLUSTER_ID)
        .setNumDatanodes(1)
        .setNumDataVolumes(1)
        .build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.ONE, 30000);

    List<HddsDatanodeService> datanodes = cluster.getHddsDatanodes();
    // There is only 1 datanode
    Assertions.assertEquals(1, datanodes.size());
    datanodeService = datanodes.get(0);
    DATANODE_UUID = datanodeService.getDatanodeDetails().getUuidString();

    ozClient = OzoneClientFactory.getRpcClient(CONF);
    store = ozClient.getObjectStore();
    scmClient = new ContainerOperationClient(CONF);

    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    volume = store.getVolume(volumeName);

    String bucketName = UUID.randomUUID().toString();
    volume.createBucket(bucketName);
    bucket = volume.getBucket(bucketName);
  }

  @AfterAll
  public static void shutdown() throws IOException {
    IOUtils.closeQuietly(scmClient);
    if (ozClient != null) {
      ozClient.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testHddsVolumeOutOfSpace() throws IOException {
    List<StorageVolume> volumeList = datanodeService.getDatanodeStateMachine()
        .getContainer().getVolumeSet().getVolumesList();
    UUID uuid = datanodeService.getDatanodeDetails().getUuid();

    // There is only 1 volume
    Assertions.assertEquals(1, volumeList.size());
    StorageVolume storageVolume = volumeList.get(0);

    String path = storageVolume.getStorageDir().getAbsolutePath();

    LOG.info("xbis: volume path: " + path);

    long capacity = 0;
    long remaining = 0;
    if (storageVolume.getVolumeInfo().isPresent()) {
      capacity = storageVolume.getVolumeInfo().get().getCapacity();
      storageVolume.getVolumeInfo().get()
          .getUsageForTesting().incrementUsedSpace(capacity);
      remaining = storageVolume.getVolumeInfo().get().getAvailable();
    }
    // Space is maxed out.
    Assertions.assertEquals(0L, remaining);

    LOG.info("xbis: capacity: " + capacity);
    LOG.info("xbis: remaining: " + remaining);

    StorageReportProto storageReportProto =
        HddsTestUtils.createStorageReport(uuid, path, capacity, capacity, remaining,
            StorageTypeProto.SSD);

    MetadataStorageReportProto metadataStorageReportProto =
        HddsTestUtils.createMetadataStorageReport(path, capacity, capacity, remaining,
            StorageTypeProto.SSD);

    List<StorageReportProto> storageReportProtoList =
        new ArrayList<>(Collections.singletonList(storageReportProto));

    List<MetadataStorageReportProto> metadataStorageReportProtoList =
        new ArrayList<>(Collections.singletonList(metadataStorageReportProto));

    SCMNodeManager scmNodeManager = (SCMNodeManager) cluster
        .getStorageContainerManager().getScmNodeManager();

    NodeReportProto nodeReportProto = HddsTestUtils
        .createNodeReport(storageReportProtoList, metadataStorageReportProtoList);
    scmNodeManager.processNodeReport(datanodeService.getDatanodeDetails(), nodeReportProto);

    Assertions.assertEquals(0L, scmNodeManager
        .getNodeStat(datanodeService.getDatanodeDetails()).get().getRemaining().get());


    initDBVolume();

    // write a file
    String keyName = UUID.randomUUID().toString();
    String value = "sample value";
    OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes(UTF_8).length);
    out.write(value.getBytes(UTF_8));
    out.close();
    OzoneKey key = bucket.getKey(keyName);
    Assertions.assertEquals(keyName, key.getName());

    // Create a container and explicitly add it to RocksDB.
  }

  private static void initDBVolume() throws IOException {
    MutableVolumeSet dbVolumeSet = new MutableVolumeSet(DATANODE_UUID,
        CLUSTER_ID, CONF, null, StorageVolume.VolumeType.DB_VOLUME,
        null);
    dbVolumeSet.getVolumesList().get(0).format(CLUSTER_ID);
//    dbVolumeSet.getVolumesList().get(0).createWorkingDir(CLUSTER_ID, null);

    HddsVolume volume = (HddsVolume) datanodeService
        .getDatanodeStateMachine().getContainer()
        .getVolumeSet().getVolumesList().get(0);
    volume.setDbVolume((DbVolume) dbVolumeSet.getVolumesList().get(0));

    File storageIdDir = new File(new File(volume.getDbVolume()
        .getStorageDir(), CLUSTER_ID), volume.getStorageID());
    // Db parent dir should be set to a subdir under the dbVolume.
    Assertions.assertEquals(volume.getDbParentDir(), storageIdDir);

    // The db directory should exist
    File containerDBFile = new File(volume.getDbParentDir(),
        CONTAINER_DB_NAME);
    Assertions.assertTrue(containerDBFile.exists());

    LOG.info("xbis: volume parent: " + volume.getStorageDir().getParent());
    LOG.info("xbis: hdds volume path: " + volume.getStorageDir().getAbsolutePath());
    LOG.info("xbis: db volume path: " + volume.getDbVolume().getStorageDir().getAbsolutePath());
    LOG.info("xbis: containerDB file path: " + containerDBFile.getAbsolutePath());
  }
}
