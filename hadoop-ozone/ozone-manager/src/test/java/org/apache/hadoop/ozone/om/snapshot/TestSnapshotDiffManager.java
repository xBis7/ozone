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
package org.apache.hadoop.ozone.om.snapshot;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.service.SnapshotDiffCleanupService;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.DELIMITER;

/**
 * Tests for {@link SnapshotDiffManager}.
 */
public class TestSnapshotDiffManager {

  private static final String VOLUME = "vol";
  private static final String BUCKET = "bucket";

  private static File metaDir;
  private static OzoneManager ozoneManager;
  private static OMMetadataManager omMetadataManager;
  private static SnapshotDiffManager snapshotDiffManager;
  private static PersistentMap<String, SnapshotDiffJob> snapDiffJobTable;
  private static SnapshotDiffCleanupService snapshotDiffCleanupService;

  @BeforeAll
  public static void init() throws AuthenticationException,
      IOException, RocksDBException {
    metaDir = GenericTestUtils.getRandomizedTestDir();
    if (!metaDir.exists()) {
      Assertions.assertTrue(metaDir.mkdirs());
    }
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS,
        metaDir.getAbsolutePath());

    OmTestManagers omTestManagers = new OmTestManagers(conf);
    ozoneManager = omTestManagers.getOzoneManager();
    omMetadataManager = omTestManagers.getMetadataManager();

    snapshotDiffManager = ozoneManager
        .getOmSnapshotManager().getSnapshotDiffManager();
    snapDiffJobTable = snapshotDiffManager.getSnapDiffJobTable();
    snapshotDiffCleanupService = ozoneManager
        .getOmSnapshotManager().getSnapshotDiffCleanupService();

    createVolumeAndBucket();
  }

  @AfterAll
  public static void cleanUp() {
    FileUtil.fullyDelete(metaDir);
  }

  /**
   * Test Snapshot Diff job cancellation.
   * Cancel is ignored unless the job is IN_PROGRESS.
   *
   * Once a job is canceled, it stays in the table until
   * SnapshotDiffCleanupService removes it.
   *
   * Job response until that happens, is CANCELED.
   */
  @Test
  public void testCanceledSnapshotDiffReport()
      throws IOException {
    String fromSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);
    String toSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);
    String fromSnapshotId = UUID.randomUUID().toString();
    String toSnapshotId = UUID.randomUUID().toString();
    String diffJobKey = fromSnapshotId + DELIMITER + toSnapshotId;

    setUpKeysAndSnapshots(fromSnapshotName, toSnapshotName,
        fromSnapshotId, toSnapshotId);

    SnapshotDiffJob diffJob = snapDiffJobTable.get(diffJobKey);
    Assertions.assertNull(diffJob);

    // This is a new job, cancel should fail.
    SnapshotDiffResponse snapshotDiffResponse = ozoneManager
        .snapshotDiff(VOLUME, BUCKET,
            fromSnapshotName, toSnapshotName,
            null, 0, false, true);

    Assertions.assertEquals(
        SnapshotDiffResponse.CancelStatus.NEW_JOB,
        snapshotDiffResponse.getCancelStatus());

    // Response job status should be CANCEL_FAILED.
    Assertions.assertEquals(JobStatus.CANCEL_FAILED,
        snapshotDiffResponse.getJobStatus());

    // Check snapDiffJobTable. Job should exist in the table.
    diffJob = snapDiffJobTable.get(diffJobKey);
    Assertions.assertNull(diffJob);

    // Submit a new job.
    snapshotDiffResponse = ozoneManager
        .snapshotDiff(VOLUME, BUCKET,
            fromSnapshotName, toSnapshotName,
            null, 0, false, false);

    // Response job status should be IN_PROGRESS.
    Assertions.assertEquals(JobStatus.IN_PROGRESS,
        snapshotDiffResponse.getJobStatus());

    // Cancel the job.
    snapshotDiffResponse = ozoneManager
        .snapshotDiff(VOLUME, BUCKET,
            fromSnapshotName, toSnapshotName,
            null, 0, false, true);

    // Response job status should be CANCELED.
    Assertions.assertEquals(JobStatus.CANCELED,
        snapshotDiffResponse.getJobStatus());

    // Check snapDiffJobTable.
    diffJob = snapDiffJobTable.get(diffJobKey);
    Assertions.assertNotNull(diffJob);
    // Status stored in the table should be CANCELED.
    Assertions.assertEquals(JobStatus.CANCELED,
        diffJob.getStatus());

    // Job hasn't been removed from the
    // table yet and response should still be canceled.
    snapshotDiffResponse = snapshotDiffManager
        .getSnapshotDiffReport(VOLUME, BUCKET,
            fromSnapshotName, toSnapshotName,
            0, 0, false);

    // Response should be CANCELED.
    Assertions.assertEquals(JobStatus.CANCELED,
        snapshotDiffResponse.getJobStatus());

    // Check snapDiffJobTable.
    diffJob = snapDiffJobTable.get(diffJobKey);
    Assertions.assertNotNull(diffJob);
    // Status stored in the table should be CANCELED.
    Assertions.assertEquals(JobStatus.CANCELED,
        diffJob.getStatus());

    String jobId = diffJob.getJobId();

    // Job is still in the snapDiffJobTable,
    // so it shouldn't exist in the purged job table.
    Assertions.assertNull(snapshotDiffCleanupService
        .getEntryFromPurgedJobTable(jobId));

    // Run the cleanup service.
    snapshotDiffCleanupService.run();

    // Job should have been removed from the snapDiffJobTable
    // and it should have been moved to the purged job table.
    Assertions.assertNull(snapDiffJobTable.get(diffJobKey));
    Assertions.assertNotNull(snapshotDiffCleanupService
        .getEntryFromPurgedJobTable(jobId));
  }

  private void setUpKeysAndSnapshots(String fromSnapshotName,
                                     String toSnapshotName,
                                     String fromSnapshotId,
                                     String toSnapshotId)
      throws IOException {
    // Get IDs.
    long volumeId = omMetadataManager
        .getVolumeId(VOLUME);
    long bucketId = omMetadataManager
        .getBucketId(VOLUME, BUCKET);

    // Create 5 keys.
    for (int i = 0; i < 5; i++) {
      OmKeyInfo omKeyInfo = createOmKeyInfo(bucketId);
      String tableKey = omMetadataManager.getOzonePathKey(volumeId,
          bucketId, bucketId, omKeyInfo.getFileName());
      omMetadataManager.getFileTable()
          .addCacheEntry(new CacheKey<>(tableKey),
              CacheValue.get(1, omKeyInfo));
      omMetadataManager.getFileTable().put(tableKey, omKeyInfo);
    }

    // Create 1st snapshot and put it in SnapshotTable.
    SnapshotInfo fromSnapshotInfo = SnapshotInfo
        .newInstance(VOLUME, BUCKET,
            fromSnapshotName, fromSnapshotId,
            System.currentTimeMillis());
    fromSnapshotInfo.setSnapshotStatus(SnapshotInfo
        .SnapshotStatus.SNAPSHOT_ACTIVE);

    String fromSnapKey = fromSnapshotInfo.getTableKey();

    OmSnapshot omSnapshotFrom = new OmSnapshot(
        ozoneManager.getKeyManager(), ozoneManager.getPrefixManager(),
        ozoneManager, VOLUME, BUCKET, fromSnapshotName);

    ozoneManager.getOmSnapshotManager().getSnapshotCache()
        .put(fromSnapKey, omSnapshotFrom);

    omMetadataManager.getSnapshotInfoTable()
        .addCacheEntry(new CacheKey<>(fromSnapKey),
            CacheValue.get(1, fromSnapshotInfo));
    omMetadataManager
        .getSnapshotInfoTable().put(fromSnapKey, fromSnapshotInfo);

    // Create 20 keys.
    for (int i = 0; i < 20; i++) {
      OmKeyInfo omKeyInfo = createOmKeyInfo(bucketId);
      String tableKey = omMetadataManager.getOzonePathKey(volumeId,
          bucketId, bucketId, omKeyInfo.getFileName());
      omMetadataManager.getFileTable()
          .addCacheEntry(new CacheKey<>(tableKey),
              CacheValue.get(1, omKeyInfo));
      omMetadataManager.getFileTable().put(tableKey, omKeyInfo);
    }

    // Create 2nd snapshot and put it in SnapshotTable.
    SnapshotInfo toSnapshotInfo = SnapshotInfo
        .newInstance(VOLUME, BUCKET,
            toSnapshotName, toSnapshotId,
            System.currentTimeMillis());
    fromSnapshotInfo.setSnapshotStatus(SnapshotInfo
        .SnapshotStatus.SNAPSHOT_ACTIVE);

    String toSnapKey = toSnapshotInfo.getTableKey();

    OmSnapshot omSnapshotTo = new OmSnapshot(
        ozoneManager.getKeyManager(), ozoneManager.getPrefixManager(),
        ozoneManager, VOLUME, BUCKET, toSnapshotName);

    ozoneManager.getOmSnapshotManager().getSnapshotCache()
        .put(toSnapKey, omSnapshotTo);

    omMetadataManager.getSnapshotInfoTable()
        .addCacheEntry(new CacheKey<>(toSnapKey),
            CacheValue.get(1, toSnapshotInfo));
    omMetadataManager
        .getSnapshotInfoTable().put(toSnapKey, toSnapshotInfo);
  }

  private static void createVolumeAndBucket() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    // Create volume and put it in VolumeTable.
    OmVolumeArgs volumeArgs = OmVolumeArgs.newBuilder()
        .setVolume(VOLUME)
        .setAdminName(ugi.getShortUserName())
        .setOwnerName(ugi.getShortUserName())
        .build();
    String volumeKey = omMetadataManager
        .getVolumeKey(VOLUME);
    omMetadataManager.getVolumeTable()
        .addCacheEntry(new CacheKey<>(volumeKey),
            CacheValue.get(1, volumeArgs));
    omMetadataManager.getVolumeTable()
        .put(volumeKey, volumeArgs);

    // Create bucket and put it in BucketTable.
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(VOLUME)
        .setBucketName(BUCKET)
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .setOwner(ugi.getShortUserName())
        .build();
    String bucketKey = omMetadataManager
        .getBucketKey(VOLUME, BUCKET);

    omMetadataManager.getBucketTable()
        .addCacheEntry(new CacheKey<>(bucketKey),
            CacheValue.get(1, bucketInfo));
    omMetadataManager.getBucketTable()
        .put(bucketKey, bucketInfo);
  }

  private OmKeyInfo createOmKeyInfo(long parentObjectId) {
    String keyName = "key-" + RandomStringUtils.randomNumeric(5);
    long objectId = ThreadLocalRandom.current().nextLong(100);

    return new OmKeyInfo.Builder()
        .setVolumeName(VOLUME)
        .setBucketName(BUCKET)
        .setKeyName(keyName)
        .setFileName(keyName)
        .setReplicationConfig(
            StandaloneReplicationConfig.getInstance(ONE))
        .setObjectID(objectId)
        .setParentObjectID(parentObjectId)
        .setDataSize(500L)
        .setCreationTime(System.currentTimeMillis())
        .build();
  }
}
