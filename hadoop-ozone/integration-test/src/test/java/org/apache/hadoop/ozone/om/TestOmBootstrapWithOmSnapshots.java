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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for bootstrapping an OM and installing
 * Ratis snapshots that have OM snapshots.
 */
@Timeout(5000)
public class TestOmBootstrapWithOmSnapshots extends TestOmHARatis {

  private static final long SNAPSHOT_THRESHOLD = 500;

  /**
   * Create a MiniOzoneCluster for testing. The cluster initially has one
   * inactive OM. So at the start of the cluster, there will be 2 active and 1
   * inactive OM.
   *
   * @throws IOException
   */
  @BeforeEach
  public void init() throws Exception {
    conf.setLong(
        OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY,
        SNAPSHOT_THRESHOLD);
    clusterBuilder.setConf(conf);
    cluster = (MiniOzoneHAClusterImpl) clusterBuilder.build();
    cluster.waitForClusterToBeReady();
    client = OzoneClientFactory.getRpcClient(omServiceId, conf);
    objectStore = client.getObjectStore();

    objectStore.createVolume(volumeName, createVolumeArgs);
    OzoneVolume retVolumeinfo = objectStore.getVolume(volumeName);

    retVolumeinfo.createBucket(bucketName,
        BucketArgs.newBuilder().setBucketLayout(TEST_BUCKET_LAYOUT).build());
    ozoneBucket = retVolumeinfo.getBucket(bucketName);
  }

  @ParameterizedTest
  @ValueSource(ints = {100})
  // tried up to 1000 snapshots and this test works, but some of the
  // timeouts have to be increased.
  public void testInstallSnapshot(int numSnapshotsToCreate) throws Exception {
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);

    // Find the inactive OM
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    // Create some snapshots, each with new keys
    int keyIncrement = 10;
    String snapshotNamePrefix = "snapshot";
    String snapshotName = "";
    List<String> keys = new ArrayList<>();
    SnapshotInfo snapshotInfo = null;

    for (int snapshotCount = 0; snapshotCount < numSnapshotsToCreate;
         snapshotCount++) {
      snapshotName = snapshotNamePrefix + snapshotCount;
      keys = writeKeys(keyIncrement);
      snapshotInfo = createOzoneSnapshot(leaderOM, snapshotName);
    }

    // Get the latest db checkpoint from the leader OM.
    TransactionInfo transactionInfo =
        TransactionInfo.readTransactionInfo(leaderOM.getMetadataManager());
    TermIndex leaderOMTermIndex =
        TermIndex.valueOf(transactionInfo.getTerm(),
            transactionInfo.getTransactionIndex());
    long leaderOMSnapshotIndex = leaderOMTermIndex.getIndex();
    long leaderOMSnapshotTermIndex = leaderOMTermIndex.getTerm();

    // Start the inactive OM. Checkpoint installation will happen spontaneously.
    cluster.startInactiveOM(followerNodeId);
    GenericTestUtils.LogCapturer logCapture =
        GenericTestUtils.LogCapturer.captureLogs(OzoneManager.LOG);

    // The recently started OM should be lagging behind the leader OM.
    // Wait & for follower to update transactions to leader snapshot index.
    // Timeout error if follower does not load update within 10s
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex()
          >= leaderOMSnapshotIndex - 1;
    }, 100, 10000);

    long followerOMLastAppliedIndex =
        followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex();
    assertTrue(
        followerOMLastAppliedIndex >= leaderOMSnapshotIndex - 1);

    // After the new checkpoint is installed, the follower OM
    // lastAppliedIndex must >= the snapshot index of the checkpoint. It
    // could be greater than snapshot index if there is any conf entry from ratis.
    followerOMLastAppliedIndex = followerOM.getOmRatisServer()
        .getLastAppliedTermIndex().getIndex();
    assertTrue(followerOMLastAppliedIndex >= leaderOMSnapshotIndex);
    assertTrue(followerOM.getOmRatisServer().getLastAppliedTermIndex()
        .getTerm() >= leaderOMSnapshotTermIndex);

    // Verify checkpoint installation was happened.
    String msg = "Reloaded OM state";
//    assertLogCapture(logCapture, msg);

    // Verify that the follower OM's DB contains the transactions which were
    // made while it was inactive.
    OMMetadataManager followerOMMetaMngr = followerOM.getMetadataManager();
    assertNotNull(followerOMMetaMngr.getVolumeTable().get(
        followerOMMetaMngr.getVolumeKey(volumeName)));
    assertNotNull(followerOMMetaMngr.getBucketTable().get(
        followerOMMetaMngr.getBucketKey(volumeName, bucketName)));
    for (String key : keys) {
      assertNotNull(followerOMMetaMngr.getKeyTable(
              TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMngr.getOzoneKey(volumeName, bucketName, key)));
    }

    // Verify RPC server is running
    GenericTestUtils.waitFor(() -> {
      return followerOM.isOmRpcServerRunning();
    }, 100, 5000);

//    assertLogCapture(logCapture,
//        "Install Checkpoint is finished");

    // Read & Write after snapshot installed.
    List<String> newKeys = writeKeys(1);
    readKeys(newKeys);
    // TODO: Enable this part after RATIS-1481 used
    /*
    Assert.assertNotNull(followerOMMetaMngr.getKeyTable(
        TEST_BUCKET_LAYOUT).get(followerOMMetaMngr.getOzoneKey(
        volumeName, bucketName, newKeys.get(0))));
     */

    checkSnapshot(leaderOM, followerOM, snapshotName, keys, snapshotInfo);
  }
}
