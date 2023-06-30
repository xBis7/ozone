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

import org.apache.hadoop.hdds.utils.DBCheckpointMetrics;
import org.apache.hadoop.hdds.utils.FaultInjector;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.assertj.core.api.Fail;
import org.junit.Ignore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests the Ratis snapshots feature in OM.
 */
@Timeout(5000)
public class TestOMRatisSnapshots extends TestOmHARatis {

  private static final long SNAPSHOT_THRESHOLD = 50;

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

  @Test
  public void testInstallSnapshot() throws Exception {
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);
    OzoneManagerRatisServer leaderRatisServer = leaderOM.getOmRatisServer();

    // Find the inactive OM
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    // Do some transactions so that the log index increases
    List<String> keys = writeKeysToIncreaseLogIndex(leaderRatisServer, 200);

    String snapshotName = "snap1";
    SnapshotInfo snapshotInfo = createOzoneSnapshot(leaderOM, snapshotName);

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
    // could be great than snapshot index if there is any conf entry from ratis.
    followerOMLastAppliedIndex = followerOM.getOmRatisServer()
        .getLastAppliedTermIndex().getIndex();
    assertTrue(followerOMLastAppliedIndex >= leaderOMSnapshotIndex);
    assertTrue(followerOM.getOmRatisServer().getLastAppliedTermIndex()
        .getTerm() >= leaderOMSnapshotTermIndex);

    // Verify checkpoint installation was happened.
    String msg = "Reloaded OM state";
    assertLogCapture(logCapture, msg);

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

    assertLogCapture(logCapture,
        "Install Checkpoint is finished");

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

  @Test
  @Timeout(300)
  public void testInstallIncrementalSnapshot(@TempDir Path tempDir)
      throws Exception {
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);
    OzoneManagerRatisServer leaderRatisServer = leaderOM.getOmRatisServer();

    // Find the inactive OM
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    // Set fault injector to pause before install
    FaultInjector faultInjector = new SnapshotPauseInjector();
    followerOM.getOmSnapshotProvider().setInjector(faultInjector);

    // Do some transactions so that the log index increases
    List<String> firstKeys = writeKeysToIncreaseLogIndex(leaderRatisServer,
        80);

    SnapshotInfo snapshotInfo2 = createOzoneSnapshot(leaderOM, "snap80");

    // Start the inactive OM. Checkpoint installation will happen spontaneously.
    cluster.startInactiveOM(followerNodeId);

    // Wait the follower download the snapshot,but get stuck by injector
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmSnapshotProvider().getNumDownloaded() == 1;
    }, 1000, 10000);

    // Get two incremental tarballs, adding new keys/snapshot for each.
    IncrementData firstIncrement = getNextIncrementalTarball(160, 2, leaderOM,
        leaderRatisServer, faultInjector, followerOM, tempDir);
    IncrementData secondIncrement = getNextIncrementalTarball(240, 3, leaderOM,
        leaderRatisServer, faultInjector, followerOM, tempDir);

    // Resume the follower thread, it would download the incremental snapshot.
    faultInjector.resume();

    // Get the latest db checkpoint from the leader OM.
    TransactionInfo transactionInfo =
        TransactionInfo.readTransactionInfo(leaderOM.getMetadataManager());
    TermIndex leaderOMTermIndex =
        TermIndex.valueOf(transactionInfo.getTerm(),
            transactionInfo.getTransactionIndex());
    long leaderOMSnapshotIndex = leaderOMTermIndex.getIndex();

    // The recently started OM should be lagging behind the leader OM.
    // Wait & for follower to update transactions to leader snapshot index.
    // Timeout error if follower does not load update within 30s
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex()
          >= leaderOMSnapshotIndex - 1;
    }, 1000, 30000);

    assertEquals(3, followerOM.getOmSnapshotProvider().getNumDownloaded());

    // Verify that the follower OM's DB contains the transactions which were
    // made while it was inactive.
    OMMetadataManager followerOMMetaMngr = followerOM.getMetadataManager();
    assertNotNull(followerOMMetaMngr.getVolumeTable().get(
        followerOMMetaMngr.getVolumeKey(volumeName)));
    assertNotNull(followerOMMetaMngr.getBucketTable().get(
        followerOMMetaMngr.getBucketKey(volumeName, bucketName)));

    for (String key : firstKeys) {
      assertNotNull(followerOMMetaMngr.getKeyTable(TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMngr.getOzoneKey(volumeName, bucketName, key)));
    }
    for (String key : firstIncrement.getKeys()) {
      assertNotNull(followerOMMetaMngr.getKeyTable(TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMngr.getOzoneKey(volumeName, bucketName, key)));
    }

    for (String key : secondIncrement.getKeys()) {
      assertNotNull(followerOMMetaMngr.getKeyTable(TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMngr.getOzoneKey(volumeName, bucketName, key)));
    }

    // Verify the metrics recording the incremental checkpoint at leader side
    DBCheckpointMetrics dbMetrics = leaderOM.getMetrics().
        getDBCheckpointMetrics();
    Assertions.assertTrue(
        dbMetrics.getLastCheckpointStreamingNumSSTExcluded() > 0);
    assertEquals(2, dbMetrics.getNumIncrementalCheckpoints());

    // Verify RPC server is running
    GenericTestUtils.waitFor(() -> {
      return followerOM.isOmRpcServerRunning();
    }, 100, 5000);

    // Read & Write after snapshot installed.
    List<String> newKeys = writeKeys(1);
    readKeys(newKeys);
    GenericTestUtils.waitFor(() -> {
      try {
        return followerOMMetaMngr.getKeyTable(
            TEST_BUCKET_LAYOUT).get(followerOMMetaMngr.getOzoneKey(
            volumeName, bucketName, newKeys.get(0))) != null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 100, 10000);

    // Verify follower candidate directory get cleaned
    String[] filesInCandidate = followerOM.getOmSnapshotProvider().
        getCandidateDir().list();
    assertNotNull(filesInCandidate);
    assertEquals(0, filesInCandidate.length);

    checkSnapshot(leaderOM, followerOM, "snap80", firstKeys, snapshotInfo2);
    checkSnapshot(leaderOM, followerOM, "snap160", firstIncrement.getKeys(),
        firstIncrement.getSnapshotInfo());
    checkSnapshot(leaderOM, followerOM, "snap240", secondIncrement.getKeys(),
        secondIncrement.getSnapshotInfo());
    Assertions.assertEquals(
        followerOM.getOmSnapshotProvider().getInitCount(), 2,
        "Only initialized twice");
  }

  @Test
  @Timeout(300)
  public void testInstallIncrementalSnapshotWithFailure() throws Exception {
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);
    OzoneManagerRatisServer leaderRatisServer = leaderOM.getOmRatisServer();

    // Find the inactive OM
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    // Set fault injector to pause before install
    FaultInjector faultInjector = new SnapshotPauseInjector();
    followerOM.getOmSnapshotProvider().setInjector(faultInjector);

    // Do some transactions so that the log index increases
    List<String> firstKeys = writeKeysToIncreaseLogIndex(leaderRatisServer,
        80);

    // Start the inactive OM. Checkpoint installation will happen spontaneously.
    cluster.startInactiveOM(followerNodeId);

    // Wait the follower download the snapshot,but get stuck by injector
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmSnapshotProvider().getNumDownloaded() == 1;
    }, 1000, 10000);

    // Do some transactions, let leader OM take a new snapshot and purge the
    // old logs, so that follower must download the new snapshot again.
    List<String> secondKeys = writeKeysToIncreaseLogIndex(leaderRatisServer,
        160);

    // Resume the follower thread, it would download the incremental snapshot.
    faultInjector.resume();

    // Pause the follower thread again to block the tarball install
    faultInjector.reset();

    // Wait the follower download the incremental snapshot, but get stuck
    // by injector
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmSnapshotProvider().getNumDownloaded() == 2;
    }, 1000, 10000);

    // Corrupt the mixed checkpoint in the candidate DB dir
    File followerCandidateDir = followerOM.getOmSnapshotProvider().
        getCandidateDir();
    List<String> sstList = HAUtils.getExistingSstFiles(followerCandidateDir);
    Assertions.assertTrue(sstList.size() > 0);
    Collections.shuffle(sstList);
    List<String> victimSstList = sstList.subList(0, sstList.size() / 3);
    for (String sst: victimSstList) {
      File victimSst = new File(followerCandidateDir, sst);
      Assertions.assertTrue(victimSst.delete());
    }

    // Resume the follower thread, it would download the full snapshot again
    // as the installation will fail for the corruption detected.
    faultInjector.resume();

    // Get the latest db checkpoint from the leader OM.
    TransactionInfo transactionInfo =
        TransactionInfo.readTransactionInfo(leaderOM.getMetadataManager());
    TermIndex leaderOMTermIndex =
        TermIndex.valueOf(transactionInfo.getTerm(),
            transactionInfo.getTransactionIndex());
    long leaderOMSnapshotIndex = leaderOMTermIndex.getIndex();

    // Wait & for follower to update transactions to leader snapshot index.
    // Timeout error if follower does not load update within 10s
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex()
          >= leaderOMSnapshotIndex - 1;
    }, 1000, 10000);

    // Verify that the follower OM's DB contains the transactions which were
    // made while it was inactive.
    OMMetadataManager followerOMMetaMngr = followerOM.getMetadataManager();
    assertNotNull(followerOMMetaMngr.getVolumeTable().get(
        followerOMMetaMngr.getVolumeKey(volumeName)));
    assertNotNull(followerOMMetaMngr.getBucketTable().get(
        followerOMMetaMngr.getBucketKey(volumeName, bucketName)));

    // Verify that the follower OM's DB contains the transactions which were
    // made while it was inactive.
    for (String key : firstKeys) {
      assertNotNull(followerOMMetaMngr.getKeyTable(TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMngr.getOzoneKey(volumeName, bucketName, key)));
    }
    for (String key : secondKeys) {
      assertNotNull(followerOMMetaMngr.getKeyTable(TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMngr.getOzoneKey(volumeName, bucketName, key)));
    }

    // There is a chance we end up checking the DBCheckpointMetrics before
    // the follower had time to get another checkpoint from the leader.
    // Add this wait check here, to avoid flakiness.
    GenericTestUtils.waitFor(() ->
            leaderOM.getMetrics().getDBCheckpointMetrics().getNumCheckpoints() > 2,
        1000, 30000);

    // Verify the metrics
    DBCheckpointMetrics dbMetrics = leaderOM.getMetrics().
        getDBCheckpointMetrics();
    assertEquals(0, dbMetrics.getLastCheckpointStreamingNumSSTExcluded());
    assertTrue(dbMetrics.getNumIncrementalCheckpoints() >= 1);
    assertTrue(dbMetrics.getNumCheckpoints() >= 3);

    // Verify RPC server is running
    GenericTestUtils.waitFor(() -> {
      return followerOM.isOmRpcServerRunning();
    }, 100, 5000);

    // Read & Write after snapshot installed.
    List<String> newKeys = writeKeys(1);
    readKeys(newKeys);
    GenericTestUtils.waitFor(() -> {
      try {
        return followerOMMetaMngr.getKeyTable(
            TEST_BUCKET_LAYOUT).get(followerOMMetaMngr.getOzoneKey(
                volumeName, bucketName, newKeys.get(0))) != null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 100, 10000);

    // Verify follower candidate directory get cleaned
    String[] filesInCandidate = followerOM.getOmSnapshotProvider().
        getCandidateDir().list();
    assertNotNull(filesInCandidate);
    assertEquals(0, filesInCandidate.length);
  }

  @Ignore("Enable this unit test after RATIS-1481 used")
  public void testInstallSnapshotWithClientWrite() throws Exception {
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);
    OzoneManagerRatisServer leaderRatisServer = leaderOM.getOmRatisServer();

    // Find the inactive OM
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    // Do some transactions so that the log index increases
    List<String> keys = writeKeysToIncreaseLogIndex(leaderRatisServer, 200);

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

    // Continuously create new keys
    ExecutorService executor = Executors.newFixedThreadPool(1);
    Future<List<String>> writeFuture = executor.submit(() -> {
      return writeKeys(200);
    });
    List<String> newKeys = writeFuture.get();

    // Wait checkpoint installation to finish
    Thread.sleep(5000);

    // The recently started OM should be lagging behind the leader OM.
    // Wait & for follower to update transactions to leader snapshot index.
    // Timeout error if follower does not load update within 3s
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex()
          >= leaderOMSnapshotIndex - 1;
    }, 100, 3000);

    // Verify checkpoint installation was happened.
    String msg = "Reloaded OM state";
    assertLogCapture(logCapture, msg);
    assertLogCapture(logCapture, "Install Checkpoint is finished");

    long followerOMLastAppliedIndex =
        followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex();
    assertTrue(
        followerOMLastAppliedIndex >= leaderOMSnapshotIndex - 1);

    // After the new checkpoint is installed, the follower OM
    // lastAppliedIndex must >= the snapshot index of the checkpoint. It
    // could be great than snapshot index if there is any conf entry from ratis.
    followerOMLastAppliedIndex = followerOM.getOmRatisServer()
        .getLastAppliedTermIndex().getIndex();
    assertTrue(followerOMLastAppliedIndex >= leaderOMSnapshotIndex);
    assertTrue(followerOM.getOmRatisServer().getLastAppliedTermIndex()
        .getTerm() >= leaderOMSnapshotTermIndex);

    // Verify that the follower OM's DB contains the transactions which were
    // made while it was inactive.
    OMMetadataManager followerOMMetaMgr = followerOM.getMetadataManager();
    assertNotNull(followerOMMetaMgr.getVolumeTable().get(
        followerOMMetaMgr.getVolumeKey(volumeName)));
    assertNotNull(followerOMMetaMgr.getBucketTable().get(
        followerOMMetaMgr.getBucketKey(volumeName, bucketName)));
    for (String key : keys) {
      assertNotNull(followerOMMetaMgr.getKeyTable(
          TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMgr.getOzoneKey(volumeName, bucketName, key)));
    }
    OMMetadataManager leaderOmMetaMgr = leaderOM.getMetadataManager();
    for (String key : newKeys) {
      assertNotNull(leaderOmMetaMgr.getKeyTable(
          TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMgr.getOzoneKey(volumeName, bucketName, key)));
    }
    Thread.sleep(5000);
    followerOMMetaMgr = followerOM.getMetadataManager();
    for (String key : newKeys) {
      assertNotNull(followerOMMetaMgr.getKeyTable(
          TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMgr.getOzoneKey(volumeName, bucketName, key)));
    }
    // Read newly created keys
    readKeys(newKeys);
    System.out.println("All data are replicated");
  }

  @Test
  public void testInstallSnapshotWithClientRead() throws Exception {
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);
    OzoneManagerRatisServer leaderRatisServer = leaderOM.getOmRatisServer();

    // Find the inactive OM
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    // Do some transactions so that the log index increases
    List<String> keys = writeKeysToIncreaseLogIndex(leaderRatisServer, 200);

    // Get transaction Index
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

    // Continuously read keys
    ExecutorService executor = Executors.newFixedThreadPool(1);
    Future<Void> readFuture = executor.submit(() -> {
      try {
        getKeys(keys, 10);
        readKeys(keys);
      } catch (IOException e) {
        assertTrue(Fail.fail("Read Key failed", e));
      }
      return null;
    });
    readFuture.get();

    // The recently started OM should be lagging behind the leader OM.
    // Wait & for follower to update transactions to leader snapshot index.
    // Timeout error if follower does not load update within 3s
    GenericTestUtils.waitFor(() -> {
      return followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex()
          >= leaderOMSnapshotIndex - 1;
    }, 100, 3000);

    long followerOMLastAppliedIndex =
        followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex();
    assertTrue(
        followerOMLastAppliedIndex >= leaderOMSnapshotIndex - 1);

    // After the new checkpoint is installed, the follower OM
    // lastAppliedIndex must >= the snapshot index of the checkpoint. It
    // could be great than snapshot index if there is any conf entry from ratis.
    followerOMLastAppliedIndex = followerOM.getOmRatisServer()
        .getLastAppliedTermIndex().getIndex();
    assertTrue(followerOMLastAppliedIndex >= leaderOMSnapshotIndex);
    assertTrue(followerOM.getOmRatisServer().getLastAppliedTermIndex()
        .getTerm() >= leaderOMSnapshotTermIndex);

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

    // Wait installation finish
    Thread.sleep(5000);
    // Verify checkpoint installation was happened.
    assertLogCapture(logCapture, "Reloaded OM state");
    assertLogCapture(logCapture, "Install Checkpoint is finished");
  }

  @Test
  public void testInstallOldCheckpointFailure() throws Exception {
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);

    // Find the inactive OM and start it
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    cluster.startInactiveOM(followerNodeId);
    GenericTestUtils.setLogLevel(OzoneManager.LOG, Level.INFO);
    GenericTestUtils.LogCapturer logCapture =
        GenericTestUtils.LogCapturer.captureLogs(OzoneManager.LOG);

    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);
    OzoneManagerRatisServer followerRatisServer = followerOM.getOmRatisServer();

    // Do some transactions so that the log index increases on follower OM
    writeKeysToIncreaseLogIndex(followerRatisServer, 100);

    TermIndex leaderCheckpointTermIndex = leaderOM.getOmRatisServer()
        .getLastAppliedTermIndex();
    DBCheckpoint leaderDbCheckpoint = leaderOM.getMetadataManager().getStore()
        .getCheckpoint(false);

    // Do some more transactions to increase the log index further on
    // follower OM such that it is more than the checkpoint index taken on
    // leader OM.
    writeKeysToIncreaseLogIndex(followerOM.getOmRatisServer(),
        leaderCheckpointTermIndex.getIndex() + 100);

    // Install the old checkpoint on the follower OM. This should fail as the
    // followerOM is already ahead of that transactionLogIndex and the OM
    // state should be reloaded.
    TermIndex followerTermIndex = followerRatisServer.getLastAppliedTermIndex();
    TermIndex newTermIndex = followerOM.installCheckpoint(
        leaderOMNodeId, leaderDbCheckpoint);

    String errorMsg = "Cannot proceed with InstallSnapshot as OM is at " +
        "TermIndex " + followerTermIndex + " and checkpoint has lower " +
        "TermIndex";
    assertLogCapture(logCapture, errorMsg);
    assertNull(newTermIndex,
        "OM installed checkpoint even though checkpoint " +
            "logIndex is less than it's lastAppliedIndex");
    assertEquals(followerTermIndex,
        followerRatisServer.getLastAppliedTermIndex());
    String msg = "OM DB is not stopped. Started services with Term: " +
        followerTermIndex.getTerm() + " and Index: " +
        followerTermIndex.getIndex();
    assertLogCapture(logCapture, msg);
  }

  @Test
  public void testInstallCorruptedCheckpointFailure() throws Exception {
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);
    OzoneManagerRatisServer leaderRatisServer = leaderOM.getOmRatisServer();

    // Find the inactive OM
    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    // Do some transactions so that the log index increases
    writeKeysToIncreaseLogIndex(leaderRatisServer, 100);

    DBCheckpoint leaderDbCheckpoint = leaderOM.getMetadataManager().getStore()
        .getCheckpoint(false);
    Path leaderCheckpointLocation = leaderDbCheckpoint.getCheckpointLocation();
    TransactionInfo leaderCheckpointTrxnInfo = OzoneManagerRatisUtils
        .getTrxnInfoFromCheckpoint(conf, leaderCheckpointLocation);

    // Corrupt the leader checkpoint and install that on the OM. The
    // operation should fail and OM should shutdown.
    boolean delete = true;
    for (File file : leaderCheckpointLocation.toFile()
        .listFiles()) {
      if (file.getName().contains(".sst")) {
        if (delete) {
          file.delete();
          delete = false;
        } else {
          delete = true;
        }
      }
    }

    GenericTestUtils.setLogLevel(OzoneManager.LOG, Level.INFO);
    GenericTestUtils.LogCapturer logCapture =
        GenericTestUtils.LogCapturer.captureLogs(OzoneManager.LOG);
    followerOM.setExitManagerForTesting(new DummyExitManager());
    // Install corrupted checkpoint
    followerOM.installCheckpoint(leaderOMNodeId, leaderCheckpointLocation,
        leaderCheckpointTrxnInfo);

    // Wait checkpoint installation to be finished.
    assertLogCapture(logCapture, "System Exit: " +
        "Failed to reload OM state and instantiate services.");
    String msg = "RPC server is stopped";
    assertLogCapture(logCapture, msg);
  }
}
