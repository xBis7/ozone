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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.ExitManager;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.FaultInjector;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.RDBCheckpointUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.assertj.core.api.Fail;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.OM_HARDLINK_FILE;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPath;
import static org.apache.hadoop.ozone.om.TestOzoneManagerHA.createKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestOmHARatis {

  protected static MiniOzoneCluster.Builder clusterBuilder = null;
  protected static MiniOzoneHAClusterImpl cluster = null;
  protected static ObjectStore objectStore;
  protected static OzoneConfiguration conf;
  protected static String clusterId;
  protected static String scmId;
  protected static String omServiceId;
  protected static int numOfOMs = 3;
  protected static OzoneBucket ozoneBucket;
  protected static String volumeName;
  protected static String bucketName;
  protected static VolumeArgs createVolumeArgs;

  protected static final int LOG_PURGE_GAP = 50;
  // This test depends on direct RocksDB checks that are easier done with OBS
  // buckets.
  protected static final BucketLayout TEST_BUCKET_LAYOUT =
      BucketLayout.OBJECT_STORE;
  protected OzoneClient client;

  /**
   * Create a MiniOzoneCluster for testing. The cluster initially has one
   * inactive OM. So at the start of the cluster, there will be 2 active and 1
   * inactive OM.
   *
   * @throws IOException
   */
  @BeforeAll
  public static void setUp() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omServiceId = "om-service-test1";
    conf.setInt(OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_GAP, LOG_PURGE_GAP);
    conf.setStorageSize(OMConfigKeys.OZONE_OM_RATIS_SEGMENT_SIZE_KEY, 16,
        StorageUnit.KB);
    conf.setStorageSize(OMConfigKeys.
        OZONE_OM_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY, 16, StorageUnit.KB);
    clusterBuilder = MiniOzoneCluster.newOMHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId("om-service-test1")
        .setNumOfOzoneManagers(numOfOMs)
        .setNumOfActiveOMs(2);

    volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    bucketName = "bucket" + RandomStringUtils.randomNumeric(5);

    createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner("user" + RandomStringUtils.randomNumeric(5))
        .setAdmin("admin" + RandomStringUtils.randomNumeric(5))
        .build();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterEach
  public void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  protected void checkSnapshot(OzoneManager leaderOM, OzoneManager followerOM,
                             String snapshotName,
                             List<String> keys, SnapshotInfo snapshotInfo)
      throws IOException {
    // Read back data from snapshot.
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(".snapshot/" + snapshotName + "/" +
            keys.get(keys.size() - 1)).build();
    OmKeyInfo omKeyInfo;
    omKeyInfo = followerOM.lookupKey(omKeyArgs);
    Assertions.assertNotNull(omKeyInfo);
    Assertions.assertEquals(omKeyInfo.getKeyName(), omKeyArgs.getKeyName());

    // Confirm followers snapshot hard links are as expected
    File followerMetaDir = OMStorage.getOmDbDir(followerOM.getConfiguration());
    Path followerActiveDir = Paths.get(followerMetaDir.toString(), OM_DB_NAME);
    Path followerSnapshotDir =
        Paths.get(getSnapshotPath(followerOM.getConfiguration(), snapshotInfo));
    File leaderMetaDir = OMStorage.getOmDbDir(leaderOM.getConfiguration());
    Path leaderActiveDir = Paths.get(leaderMetaDir.toString(), OM_DB_NAME);
    Path leaderSnapshotDir =
        Paths.get(getSnapshotPath(leaderOM.getConfiguration(), snapshotInfo));
    // Get the list of hardlinks from the leader.  Then confirm those links
    //  are on the follower
    int hardLinkCount = 0;
    try (Stream<Path> list = Files.list(leaderSnapshotDir)) {
      for (Path leaderSnapshotSST: list.collect(Collectors.toList())) {
        String fileName = leaderSnapshotSST.getFileName().toString();
        if (fileName.toLowerCase().endsWith(".sst")) {

          Path leaderActiveSST =
              Paths.get(leaderActiveDir.toString(), fileName);
          // Skip if not hard link on the leader
          if (!leaderActiveSST.toFile().exists()) {
            continue;
          }
          // If it is a hard link on the leader, it should be a hard
          // link on the follower
          if (OmSnapshotUtils.getINode(leaderActiveSST)
              .equals(OmSnapshotUtils.getINode(leaderSnapshotSST))) {
            Path followerSnapshotSST =
                Paths.get(followerSnapshotDir.toString(), fileName);
            Path followerActiveSST =
                Paths.get(followerActiveDir.toString(), fileName);
            Assertions.assertEquals(
                OmSnapshotUtils.getINode(followerActiveSST),
                OmSnapshotUtils.getINode(followerSnapshotSST),
                "Snapshot sst file is supposed to be a hard link");
            hardLinkCount++;
          }
        }
      }
    }
    Assertions.assertTrue(hardLinkCount > 0, "No hard links were found");
  }

  protected static class IncrementData {
    private List<String> keys;
    private SnapshotInfo snapshotInfo;
    public List<String> getKeys() {
      return keys;
    }
    public SnapshotInfo getSnapshotInfo() {
      return snapshotInfo;
    }
  }

  protected IncrementData getNextIncrementalTarball(
      int numKeys, int expectedNumDownloads,
      OzoneManager leaderOM, OzoneManagerRatisServer leaderRatisServer,
      FaultInjector faultInjector, OzoneManager followerOM, Path tempDir)
      throws IOException, InterruptedException, TimeoutException {
    IncrementData id = new IncrementData();

    // Get the latest db checkpoint from the leader OM.
    TransactionInfo transactionInfo =
        TransactionInfo.readTransactionInfo(leaderOM.getMetadataManager());
    TermIndex leaderOMTermIndex =
        TermIndex.valueOf(transactionInfo.getTerm(),
            transactionInfo.getTransactionIndex());
    long leaderOMSnapshotIndex = leaderOMTermIndex.getIndex();
    // Do some transactions, let leader OM take a new snapshot and purge the
    // old logs, so that follower must download the new increment.
    id.keys = writeKeysToIncreaseLogIndex(leaderRatisServer,
        numKeys);

    id.snapshotInfo = createOzoneSnapshot(leaderOM, "snap" + numKeys);
    // Resume the follower thread, it would download the incremental snapshot.
    faultInjector.resume();

    // Pause the follower thread again to block the next install
    faultInjector.reset();

    // Wait the follower download the incremental snapshot, but get stuck
    // by injector
    GenericTestUtils.waitFor(() ->
        followerOM.getOmSnapshotProvider().getNumDownloaded() ==
            expectedNumDownloads, 1000, 10000);

    assertTrue(followerOM.getOmRatisServer().
        getLastAppliedTermIndex().getIndex()
        >= leaderOMSnapshotIndex - 1);

    // Now confirm tarball is just incremental and contains no unexpected
    //  files/links.
    Path increment = Paths.get(tempDir.toString(), "increment" + numKeys);
    assertTrue(increment.toFile().mkdirs());
    unTarLatestTarBall(followerOM, increment);
    List<String> sstFiles = HAUtils.getExistingSstFiles(increment.toFile());
    Path followerCandidatePath = followerOM.getOmSnapshotProvider().
        getCandidateDir().toPath();

    // Confirm that none of the files in the tarball match one in the
    // candidate dir.
    assertTrue(sstFiles.size() > 0);
    for (String s: sstFiles) {
      File sstFile = Paths.get(followerCandidatePath.toString(), s).toFile();
      assertFalse(sstFile.exists(),
          sstFile + " should not duplicate existing files");
    }

    // Confirm that none of the links in the tarballs hardLinkFile
    //  match the existing files
    Path hardLinkFile = Paths.get(increment.toString(), OM_HARDLINK_FILE);
    try (Stream<String> lines = Files.lines(hardLinkFile)) {
      int lineCount = 0;
      for (String line: lines.collect(Collectors.toList())) {
        lineCount++;
        String link = line.split("\t")[0];
        File linkFile = Paths.get(
            followerCandidatePath.toString(), link).toFile();
        assertFalse(linkFile.exists(),
            "Incremental checkpoint should not " +
                "duplicate existing links");
      }
      assertTrue(lineCount > 0);
    }
    return id;
  }


  protected SnapshotInfo createOzoneSnapshot(OzoneManager leaderOM, String name)
      throws IOException {
    objectStore.createSnapshot(volumeName, bucketName, name);

    String tableKey = SnapshotInfo.getTableKey(volumeName,
        bucketName,
        name);
    SnapshotInfo snapshotInfo = leaderOM.getMetadataManager()
        .getSnapshotInfoTable()
        .get(tableKey);
    // Allow the snapshot to be written to disk
    String fileName =
        getSnapshotPath(leaderOM.getConfiguration(), snapshotInfo);
    File snapshotDir = new File(fileName);
    if (!RDBCheckpointUtils
        .waitForCheckpointDirectoryExist(snapshotDir)) {
      throw new IOException("snapshot directory doesn't exist");
    }
    return snapshotInfo;
  }

  protected List<String> writeKeysToIncreaseLogIndex(
      OzoneManagerRatisServer omRatisServer, long targetLogIndex)
      throws IOException, InterruptedException {
    List<String> keys = new ArrayList<>();
    long logIndex = omRatisServer.getLastAppliedTermIndex().getIndex();
    while (logIndex < targetLogIndex) {
      keys.add(createKey(ozoneBucket));
      Thread.sleep(100);
      logIndex = omRatisServer.getLastAppliedTermIndex().getIndex();
    }
    return keys;
  }

  protected List<String> writeKeys(long keyCount) throws IOException {
    List<String> keys = new ArrayList<>();
    long index = 0;
    while (index < keyCount) {
      keys.add(createKey(ozoneBucket));
      index++;
    }
    return keys;
  }

  protected void getKeys(List<String> keys, int round) throws IOException {
    while (round > 0) {
      for (String keyName : keys) {
        OzoneKeyDetails key = ozoneBucket.getKey(keyName);
        assertEquals(keyName, key.getName());
      }
      round--;
    }
  }

  protected void readKeys(List<String> keys) throws IOException {
    for (String keyName : keys) {
      OzoneInputStream inputStream = ozoneBucket.readKey(keyName);
      byte[] data = new byte[100];
      inputStream.read(data, 0, 100);
      inputStream.close();
    }
  }

  protected void assertLogCapture(GenericTestUtils.LogCapturer logCapture,
                                String msg)
      throws InterruptedException, TimeoutException {
    GenericTestUtils.waitFor(() -> {
      return logCapture.getOutput().contains(msg);
    }, 100, 5000);
  }

  // Returns temp dir where tarball was untarred.
  protected void unTarLatestTarBall(OzoneManager followerOm, Path tempDir)
      throws IOException {
    File snapshotDir = followerOm.getOmSnapshotProvider().getSnapshotDir();
    // Find the latest tarball.
    String tarBall = Arrays.stream(Objects.requireNonNull(snapshotDir.list())).
        filter(s -> s.toLowerCase().endsWith(".tar")).
        reduce("", (s1, s2) -> s1.compareToIgnoreCase(s2) > 0 ? s1 : s2);
    FileUtil.unTar(new File(snapshotDir, tarBall), tempDir.toFile());
  }

  protected static class DummyExitManager extends ExitManager {
    @Override
    public void exitSystem(int status, String message, Throwable throwable,
                           Logger log) {
      log.error("System Exit: " + message, throwable);
    }
  }

  protected static class SnapshotPauseInjector extends FaultInjector {
    private CountDownLatch ready;
    private CountDownLatch wait;

    SnapshotPauseInjector() {
      init();
    }

    @Override
    public void init() {
      this.ready = new CountDownLatch(1);
      this.wait = new CountDownLatch(1);
    }

    @Override
    public void pause() throws IOException {
      ready.countDown();
      try {
        wait.await();
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void resume() {
      // Make sure injector pauses before resuming.
      try {
        ready.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
        assertTrue(Fail.fail("resume interrupted"));
      }
      wait.countDown();
    }

    @Override
    public void reset() {
      init();
    }
  }
}
