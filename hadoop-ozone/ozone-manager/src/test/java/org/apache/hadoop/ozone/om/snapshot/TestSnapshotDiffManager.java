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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.ozone.om.OmSnapshotManager.DELIMITER;

/**
 * Tests for {@link SnapshotDiffManager}.
 */
public class TestSnapshotDiffManager {

  @TempDir
  private static File metaDir;

  private static OzoneManager ozoneManager;
  private static SnapshotDiffManager snapshotDiffManager;
  private static PersistentMap<String, SnapshotDiffJob> snapDiffJobTable;
  private static Map<String, Future<?>> snapDiffFutures;

  @BeforeAll
  public static void init() throws AuthenticationException,
      IOException, RocksDBException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS,
        metaDir.getAbsolutePath());
    OmTestManagers omTestManagers = new OmTestManagers(conf);
    ozoneManager = omTestManagers.getOzoneManager();

    snapshotDiffManager = ozoneManager
        .getOmSnapshotManager().getSnapshotDiffManager();
    snapDiffJobTable = snapshotDiffManager.getSnapDiffJobTable();
    snapDiffFutures = snapshotDiffManager.getSnapDiffFutures();
  }

  @Test
  public void testCanceledSnapshotDiffReport() throws IOException, InterruptedException, TimeoutException {
    String volumeName = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucketName = "buck-" + RandomStringUtils.randomNumeric(5);
    String fromSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);
    String fromSnapshotId = UUID.randomUUID().toString();
    String toSnapshotName = "snap-" + RandomStringUtils.randomNumeric(5);
    String toSnapshotId = UUID.randomUUID().toString();
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    OmVolumeArgs volumeArgs = OmVolumeArgs.newBuilder()
        .setVolume(volumeName)
        .setAdminName(ugi.getShortUserName())
        .setOwnerName(ugi.getShortUserName())
        .build();
    String volumeKey = ozoneManager.getMetadataManager()
        .getVolumeKey(volumeName);
    ozoneManager.getMetadataManager().getVolumeTable()
        .put(volumeKey, volumeArgs);

    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .setOwner(ugi.getShortUserName())
        .build();
    String bucketKey = ozoneManager.getMetadataManager()
        .getBucketKey(volumeName, bucketName);
    ozoneManager.getMetadataManager().getBucketTable().put(bucketKey, bucketInfo);

    SnapshotInfo fromSnapshotInfo = SnapshotInfo
        .newInstance(volumeName, bucketName,
            fromSnapshotName, fromSnapshotId,
            System.currentTimeMillis());

    SnapshotInfo toSnapshotInfo = SnapshotInfo
        .newInstance(volumeName, bucketName,
            toSnapshotName, toSnapshotId,
            System.currentTimeMillis());

    String fromSnapKey = fromSnapshotInfo.getTableKey();
    String toSnapKey = toSnapshotInfo.getTableKey();

    OmSnapshot omSnapshotFrom = new OmSnapshot(ozoneManager.getKeyManager(),
        ozoneManager.getPrefixManager(),
        ozoneManager, volumeName, bucketName, fromSnapshotName);

    OmSnapshot omSnapshotTo = new OmSnapshot(ozoneManager.getKeyManager(),
        ozoneManager.getPrefixManager(),
        ozoneManager, volumeName, bucketName, toSnapshotName);

    ozoneManager.getOmSnapshotManager().getSnapshotCache()
        .put(fromSnapKey, omSnapshotFrom);
    ozoneManager.getOmSnapshotManager().getSnapshotCache()
        .put(toSnapKey, omSnapshotTo);

    ozoneManager.getMetadataManager()
        .getSnapshotInfoTable().put(fromSnapKey, fromSnapshotInfo);

    ozoneManager.getMetadataManager()
        .getSnapshotInfoTable().put(toSnapKey, toSnapshotInfo);

    String diffJobKey = fromSnapshotId + DELIMITER + toSnapshotId;

    SnapshotDiffJob diffJob = snapDiffJobTable.get(diffJobKey);
    Assertions.assertNull(diffJob);

    Assertions.assertFalse(snapDiffFutures.containsKey(diffJobKey));

    SnapshotDiffResponse snapshotDiffResponse = snapshotDiffManager
        .getSnapshotDiffReport(volumeName, bucketName,
            fromSnapshotName, toSnapshotName,
            0, 0, false, true);

    Assertions.assertEquals(JobStatus.IN_PROGRESS,
        snapshotDiffResponse.getJobStatus());

    diffJob = snapDiffJobTable.get(diffJobKey);
    Assertions.assertNotNull(diffJob);
    Assertions.assertTrue(snapDiffFutures.containsKey(diffJobKey));

    snapshotDiffResponse = snapshotDiffManager
        .getSnapshotDiffReport(volumeName, bucketName,
            fromSnapshotName, toSnapshotName,
            0, 0, false, true);

    Assertions.assertEquals(JobStatus.CANCELED,
        snapshotDiffResponse.getJobStatus());

//    java.lang.NullPointerException
//    at org.apache.hadoop.ozone.om.snapshot.SnapshotDiffManager.getBucketLayout(SnapshotDiffManager.java:1261)
//    at org.apache.hadoop.ozone.om.snapshot.SnapshotDiffManager.generateSnapshotDiffReport(SnapshotDiffManager.java:747)

//    ERROR om.SstFilteringService (SstFilteringService.java:call(174)) - Error during Snapshot sst filtering
//    VOLUME_NOT_FOUND org.apache.hadoop.ozone.om.exceptions.OMException: Volume not found vol-91688

    GenericTestUtils.waitFor(() ->
            (!snapDiffFutures.containsKey(diffJobKey)),
        100, 80000);

    diffJob = snapDiffJobTable.get(diffJobKey);
    Assertions.assertNull(diffJob);
    Assertions.assertFalse(snapDiffFutures.containsKey(diffJobKey));
  }
}
