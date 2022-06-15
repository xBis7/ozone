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
package org.apache.hadoop.ozone.recon.tasks;

import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getMockOzoneManagerServiceProvider;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeKeyToOm;

/**
 * Test for OBSNSSummaryTask.
 */
@RunWith(Enclosed.class)
public final class TestOBSNSSummaryTask {

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private static OMMetadataManager omMetadataManager;
  private static ReconOMMetadataManager reconOMMetadataManager;
  private static OBSNSSummaryTask obsNSSummaryTask;

  // Object names
  private static final String VOL = "vol";
  private static final String BUCKET_ONE = "bucket1";
  private static final String BUCKET_TWO = "bucket2";
  private static final String KEY_ONE = "file1";
  private static final String KEY_TWO = "file2";
  private static final String KEY_THREE = "dir1/dir2/file3";
  private static final String KEY_FOUR = "file4";
  private static final String KEY_FIVE = "file5";
  private static final String FILE_ONE = "file1";
  private static final String FILE_TWO = "file2";
  private static final String FILE_THREE = "file3";
  private static final String FILE_FOUR = "file4";
  private static final String FILE_FIVE = "file5";

  private static final String TEST_USER = "TestUser";

  private static final long VOL_OBJECT_ID = 0L;
  private static final long BUCKET_ONE_OBJECT_ID = 1L;
  private static final long BUCKET_TWO_OBJECT_ID = 2L;
  private static final long KEY_ONE_OBJECT_ID = 3L;
  private static final long KEY_TWO_OBJECT_ID = 5L;
  private static final long KEY_FOUR_OBJECT_ID = 6L;
  private static final long KEY_THREE_OBJECT_ID = 8L;
  private static final long KEY_FIVE_OBJECT_ID = 9L;

  private static final long KEY_ONE_SIZE = 500L;
  private static final long KEY_TWO_OLD_SIZE = 1025L;
  private static final long KEY_TWO_UPDATE_SIZE = 1023L;
  private static final long KEY_THREE_SIZE =
      ReconConstants.MAX_FILE_SIZE_UPPER_BOUND - 100L;
  private static final long KEY_FOUR_SIZE = 2050L;
  private static final long KEY_FIVE_SIZE = 100L;

  private TestOBSNSSummaryTask() {
    throw new UnsupportedOperationException(
        "This is a utility test class and cannot be instantiated");
  }

  @BeforeClass
  public static void setUp() throws Exception {
    initializeNewOmMetadataManager(TEMPORARY_FOLDER.newFolder());
    OzoneManagerServiceProviderImpl ozoneManagerServiceProvider =
        getMockOzoneManagerServiceProvider();
    reconOMMetadataManager =
        getTestReconOmMetadataManager(
            omMetadataManager, TEMPORARY_FOLDER.newFolder());

    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(TEMPORARY_FOLDER)
            .withReconOm(reconOMMetadataManager)
            .withOmServiceProvider(ozoneManagerServiceProvider)
            .withReconSqlDb()
            .withContainerDB()
            .build();
    reconNamespaceSummaryManager =
        reconTestInjector.getInstance(ReconNamespaceSummaryManager.class);

    NSSummary nonExistentSummary =
        reconNamespaceSummaryManager.getNSSummary(BUCKET_ONE_OBJECT_ID);
    Assert.assertNull(nonExistentSummary);

    populateOMDB();

    obsNSSummaryTask = new OBSNSSummaryTask(
        reconNamespaceSummaryManager, reconOMMetadataManager);
  }

  /**
   * Nested class for testing OBSNSSummaryTask reprocess.
   */
  public static class TestReprocess {

    private static NSSummary nsSummaryForBucket1;
    private static NSSummary nsSummaryForBucket2;

    @BeforeClass
    public static void setUp() throws IOException {
      // write a NSSummary prior to reprocess
      // verify it got cleaned up after.
      NSSummary staleNSSummary = new NSSummary();
      reconNamespaceSummaryManager.storeNSSummary(-1L, staleNSSummary);

      obsNSSummaryTask.reprocess(reconOMMetadataManager);

      nsSummaryForBucket1 =
          reconNamespaceSummaryManager.getNSSummary(BUCKET_ONE_OBJECT_ID);
      Assert.assertNotNull(nsSummaryForBucket1);
      nsSummaryForBucket2 =
          reconNamespaceSummaryManager.getNSSummary(BUCKET_TWO_OBJECT_ID);
      Assert.assertNotNull(nsSummaryForBucket2);
    }

    @Test
    public void testReprocessNSSummaryNull() throws IOException {
      Assert.assertNull(reconNamespaceSummaryManager.getNSSummary(-1L));
    }

    @Test
    public void testReprocessGetFiles() {
      //dir1/dir2/file3 is also considered a file in OBS
      Assert.assertEquals(2, nsSummaryForBucket1.getNumOfFiles());
      Assert.assertEquals(2, nsSummaryForBucket2.getNumOfFiles());

      Assert.assertEquals(KEY_ONE_SIZE + KEY_THREE_SIZE,
          nsSummaryForBucket1.getSizeOfFiles());
      Assert.assertEquals(KEY_TWO_OLD_SIZE + KEY_FOUR_SIZE,
          nsSummaryForBucket2.getSizeOfFiles());
    }

    /**
     *        bucket1
     *        /    \
     *     file1  dir1
     *            /
     *         dir2
     *          /
     *        file3
     * In a legacy bucket we would have a key and a dir.
     * In an OBS bucket we have two keys(file1, dir1/dir2/file3).
     * FileSize for bucket1 is different
     * than the FileSize we would expect for a legacy bucket.
     */
    @Test
    public void testReprocessFileBucketSize() {
      int[] fileDistBucket1 = nsSummaryForBucket1.getFileSizeBucket();
      int[] fileDistBucket2 = nsSummaryForBucket2.getFileSizeBucket();
      Assert.assertEquals(ReconConstants.NUM_OF_BINS, fileDistBucket1.length);
      Assert.assertEquals(ReconConstants.NUM_OF_BINS, fileDistBucket2.length);

      Assert.assertEquals(1, fileDistBucket1[0]);
      for (int i = 1; i < (ReconConstants.NUM_OF_BINS - 1); ++i) {
        Assert.assertEquals(0, fileDistBucket1[i]);
      }
      Assert.assertEquals(1,
          fileDistBucket1[ReconConstants.NUM_OF_BINS - 1]);

      Assert.assertEquals(1, fileDistBucket2[1]);
      Assert.assertEquals(1, fileDistBucket2[2]);
      for (int i = 0; i < ReconConstants.NUM_OF_BINS; ++i) {
        if (i == 1 || i == 2) {
          continue;
        }
        Assert.assertEquals(0, fileDistBucket2[i]);
      }
    }
  }

  /**
   * Nested class for testing OBSNSSummaryTask process.
   */
  public static class TestProcess {

    private static NSSummary nsSummaryForBucket1;
    private static NSSummary nsSummaryForBucket2;

    private static OMDBUpdateEvent keyEvent1;
    private static OMDBUpdateEvent keyEvent2;
    private static OMDBUpdateEvent keyEvent3;

    @BeforeClass
    public static void setUp() throws IOException {
      obsNSSummaryTask.reprocess(reconOMMetadataManager);
      obsNSSummaryTask.process(processEventBatch());

      nsSummaryForBucket1 =
          reconNamespaceSummaryManager.getNSSummary(BUCKET_ONE_OBJECT_ID);
      Assert.assertNotNull(nsSummaryForBucket1);
      nsSummaryForBucket2 =
          reconNamespaceSummaryManager.getNSSummary(BUCKET_TWO_OBJECT_ID);
      Assert.assertNotNull(nsSummaryForBucket2);
    }

    private static OMUpdateEventBatch processEventBatch() throws IOException {
      // put file5 under bucket 2
      String omPutKey =
          OM_KEY_PREFIX + VOL +
              OM_KEY_PREFIX + BUCKET_TWO +
              OM_KEY_PREFIX + KEY_FIVE;
      OmKeyInfo omPutKeyInfo = buildOmKeyInfo(VOL, BUCKET_TWO, KEY_FIVE,
          FILE_FIVE, KEY_FIVE_OBJECT_ID, KEY_FIVE_SIZE);
      keyEvent1 = new OMDBUpdateEvent.
          OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(omPutKey)
          .setValue(omPutKeyInfo)
          .setTable(omMetadataManager.getKeyTable(getBucketLayout())
              .getName())
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.PUT)
          .build();

      // delete file 1 under bucket 1
      String omDeleteKey =
          OM_KEY_PREFIX + VOL +
              OM_KEY_PREFIX + BUCKET_ONE +
              OM_KEY_PREFIX + KEY_ONE;
      OmKeyInfo omDeleteInfo = buildOmKeyInfo(
          VOL, BUCKET_ONE, KEY_ONE,
          FILE_ONE, KEY_ONE_OBJECT_ID);
      keyEvent2 = new OMDBUpdateEvent.
          OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(omDeleteKey)
          .setValue(omDeleteInfo)
          .setTable(omMetadataManager.getKeyTable(getBucketLayout())
              .getName())
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.DELETE)
          .build();

      // update file 2's size under bucket 2
      String omUpdateKey =
          OM_KEY_PREFIX + VOL +
              OM_KEY_PREFIX + BUCKET_TWO +
              OM_KEY_PREFIX + KEY_TWO;
      OmKeyInfo omOldInfo = buildOmKeyInfo(
          VOL, BUCKET_TWO, KEY_TWO, FILE_TWO,
          KEY_TWO_OBJECT_ID, KEY_TWO_OLD_SIZE);
      OmKeyInfo omUpdateInfo = buildOmKeyInfo(
          VOL, BUCKET_TWO, KEY_TWO, FILE_TWO,
          KEY_TWO_OBJECT_ID, KEY_TWO_UPDATE_SIZE);
      keyEvent3 = new OMDBUpdateEvent.
          OMUpdateEventBuilder<String, OmKeyInfo>()
          .setKey(omUpdateKey)
          .setValue(omUpdateInfo)
          .setOldValue(omOldInfo)
          .setTable(omMetadataManager.getKeyTable(getBucketLayout())
              .getName())
          .setAction(OMDBUpdateEvent.OMDBUpdateAction.UPDATE)
          .build();

      OMUpdateEventBatch omUpdateEventBatch = new OMUpdateEventBatch(
          new ArrayList<OMDBUpdateEvent>() {{
              add(keyEvent1);
              add(keyEvent2);
              add(keyEvent3);
            }});

      return omUpdateEventBatch;
    }

    @Test
    public void testProcessUpdateFileSize() throws IOException {
      // file1 is gone, so bucket1 only contains dir1/dir2/file3
      Assert.assertNotNull(nsSummaryForBucket1);
      Assert.assertEquals(1, nsSummaryForBucket1.getNumOfFiles());
    }

    @Test
    public void testProcessBucket() throws IOException {
      // file5 is added under bucket2, so bucket2 has 3 keys now
      // file2 is updated with new datasize,
      // so file size dist for bucket2 should be updated
      Assert.assertNotNull(nsSummaryForBucket2);
      Assert.assertEquals(3, nsSummaryForBucket2.getNumOfFiles());
      // key4 + key5 + updated key 2
      Assert.assertEquals(KEY_FOUR_SIZE + KEY_FIVE_SIZE + KEY_TWO_UPDATE_SIZE,
          nsSummaryForBucket2.getSizeOfFiles());

      int[] fileSizeDist = nsSummaryForBucket2.getFileSizeBucket();
      Assert.assertEquals(ReconConstants.NUM_OF_BINS, fileSizeDist.length);
      // 1023L and 100L
      Assert.assertEquals(2, fileSizeDist[0]);
      // 2050L
      Assert.assertEquals(1, fileSizeDist[2]);
      for (int i = 0; i < ReconConstants.NUM_OF_BINS; ++i) {
        if (i == 0 || i == 2) {
          continue;
        }
        Assert.assertEquals(0, fileSizeDist[i]);
      }
    }
  }

  /**
   * Build a key info for put/update action.
   * @param volume volume name
   * @param bucket bucket name
   * @param key key name
   * @param fileName file name
   * @param objectID object ID
   * @param dataSize file size
   * @return the KeyInfo
   */
  private static OmKeyInfo buildOmKeyInfo(String volume,
                                          String bucket,
                                          String key,
                                          String fileName,
                                          long objectID,
                                          long dataSize) {
    return new OmKeyInfo.Builder()
        .setBucketName(bucket)
        .setVolumeName(volume)
        .setKeyName(key)
        .setFileName(fileName)
        .setReplicationConfig(
            StandaloneReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.ONE))
        .setObjectID(objectID)
        .setDataSize(dataSize)
        .build();
  }

  /**
   * Build a key info for delete action.
   * @param volume volume name
   * @param bucket bucket name
   * @param key key name
   * @param fileName file name
   * @param objectID object ID
   * @return the KeyInfo
   */
  private static OmKeyInfo buildOmKeyInfo(String volume,
                                          String bucket,
                                          String key,
                                          String fileName,
                                          long objectID) {
    return new OmKeyInfo.Builder()
        .setBucketName(bucket)
        .setVolumeName(volume)
        .setKeyName(key)
        .setFileName(fileName)
        .setReplicationConfig(
            StandaloneReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.ONE))
        .setObjectID(objectID)
        .build();
  }

  /**
   * Populate OMDB with the following configs.
   *              vol
   *            /     \
   *        bucket1   bucket2
   *        /    \      /    \
   *     file1  dir1  file2  file4
   *            /
   *         dir2
   *          /
   *        file3
   *
   * @throws IOException
   */
  private static void populateOMDB() throws IOException {
    writeKeyToOm(reconOMMetadataManager,
        KEY_ONE,
        BUCKET_ONE,
        VOL,
        FILE_ONE,
        KEY_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        KEY_ONE_SIZE,
        getBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
        KEY_TWO,
        BUCKET_TWO,
        VOL,
        FILE_TWO,
        KEY_TWO_OBJECT_ID,
        BUCKET_TWO_OBJECT_ID,
        BUCKET_TWO_OBJECT_ID,
        VOL_OBJECT_ID,
        KEY_TWO_OLD_SIZE,
        getBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
        KEY_FOUR,
        BUCKET_TWO,
        VOL,
        FILE_FOUR,
        KEY_FOUR_OBJECT_ID,
        BUCKET_TWO_OBJECT_ID,
        BUCKET_TWO_OBJECT_ID,
        VOL_OBJECT_ID,
        KEY_FOUR_SIZE,
        getBucketLayout());
    writeKeyToOm(reconOMMetadataManager,
        KEY_THREE,
        BUCKET_ONE,
        VOL,
        FILE_THREE,
        KEY_THREE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        BUCKET_ONE_OBJECT_ID,
        VOL_OBJECT_ID,
        KEY_THREE_SIZE,
        getBucketLayout());
  }

  /**
   * Create a new OM Metadata manager instance with one user, one vol, and two
   * buckets.
   * @throws IOException ioEx
   */
  private static void initializeNewOmMetadataManager(
      File omDbDir)
      throws IOException {
    OzoneConfiguration omConfiguration = new OzoneConfiguration();
    omConfiguration.set(OZONE_OM_DB_DIRS,
        omDbDir.getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(
        omConfiguration);

    String volumeKey = omMetadataManager.getVolumeKey(VOL);
    OmVolumeArgs args =
        OmVolumeArgs.newBuilder()
            .setObjectID(VOL_OBJECT_ID)
            .setVolume(VOL)
            .setAdminName(TEST_USER)
            .setOwnerName(TEST_USER)
            .build();
    omMetadataManager.getVolumeTable().put(volumeKey, args);

    OmBucketInfo bucketInfo1 = OmBucketInfo.newBuilder()
        .setVolumeName(VOL)
        .setBucketName(BUCKET_ONE)
        .setObjectID(BUCKET_ONE_OBJECT_ID)
        .setBucketLayout(getBucketLayout())
        .build();

    OmBucketInfo bucketInfo2 = OmBucketInfo.newBuilder()
        .setVolumeName(VOL)
        .setBucketName(BUCKET_TWO)
        .setObjectID(BUCKET_TWO_OBJECT_ID)
        .setBucketLayout(getBucketLayout())
        .build();

    String bucketKey = omMetadataManager.getBucketKey(
        bucketInfo1.getVolumeName(), bucketInfo1.getBucketName());
    String bucketKey2 = omMetadataManager.getBucketKey(
        bucketInfo2.getVolumeName(), bucketInfo2.getBucketName());

    omMetadataManager.getBucketTable().put(bucketKey, bucketInfo1);
    omMetadataManager.getBucketTable().put(bucketKey2, bucketInfo2);
  }

  private static BucketLayout getBucketLayout() {
    return BucketLayout.OBJECT_STORE;
  }
}