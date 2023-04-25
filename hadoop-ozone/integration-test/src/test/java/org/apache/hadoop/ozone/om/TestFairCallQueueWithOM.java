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
package org.apache.hadoop.ozone.om;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ozone.OzoneFileSystem;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.DecayRpcScheduler;
import org.apache.hadoop.ipc.FairCallQueue;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OzoneIdentityProvider;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.om.protocolPB.Hadoop3OmTransportFactory;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CALLQUEUE_IMPL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_SCHEDULER_IMPL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_BACKOFF_ENABLE;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_SCHEDULER_PRIORITY_LEVELS_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_IDENTITY_PROVIDER_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import static org.apache.hadoop.ozone.admin.scm.FinalizeUpgradeCommandUtil.isDone;
import static org.apache.hadoop.ozone.admin.scm.FinalizeUpgradeCommandUtil.isStarting;
import static org.apache.hadoop.ozone.om.OMConfigKeys.*;
import static org.apache.ozone.test.GenericTestUtils.getTempPath;

/**
 * Tests for using the FairCallQueue with OM.
 */
public class TestFairCallQueueWithOM {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestFairCallQueueWithOM.class);
  private static MiniOzoneCluster cluster;

  private static OzoneConfiguration conf;
  private static String clusterId;
  private static String scmId;
  private static String omId;
  private static OzoneClient client;
  private static Path metaDir;

  private static String volumeName;
  private static String bucketName;

  private static final String TENANT_ID = "tenant";
  private static final String USER_PRINCIPAL = "username";
  private static final String ACCESS_ID = "tenant$username";

  @BeforeAll
  public static void initCluster() throws Exception {
    conf = new OzoneConfiguration();

    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omId = UUID.randomUUID().toString();

    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, true);
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        BucketLayout.LEGACY.name());

    conf.set(OZONE_SCM_NAMES, "localhost");

//    String port = cluster.getOzoneManager().getRpcPort();
//    String port = conf.get(String.valueOf(OZONE_OM_PORT_DEFAULT));
    String port = "9872";
    conf.set(OZONE_OM_ADDRESS_KEY, "locahost:" + port);
    String namespace = "ipc." + port + ".";

    conf.set(OZONE_OM_S3_GPRC_SERVER_ENABLED, "false");
    conf.set(OZONE_OM_TRANSPORT_CLASS,
        Hadoop3OmTransportFactory.class.getName());

    // FairCallQueue configuration
    conf.set(namespace + IPC_CALLQUEUE_IMPL_KEY,
        FairCallQueue.class.getName());
    conf.set(namespace + IPC_SCHEDULER_IMPL_KEY,
        DecayRpcScheduler.class.getName());
    conf.set(namespace + IPC_IDENTITY_PROVIDER_KEY,
        OzoneIdentityProvider.class.getName());
    conf.set(namespace + IPC_BACKOFF_ENABLE, "true");
    conf.set(namespace + IPC_SCHEDULER_PRIORITY_LEVELS_KEY, "2");

    final String path = getTempPath(UUID.randomUUID().toString());
    metaDir = Paths.get(path, "om-meta");
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.toString());
    OzoneManager.setTestSecureOmFlag(true);

    cluster =  MiniOzoneCluster.newBuilder(conf)
        .setClusterId(clusterId)
        .setNumDatanodes(3)
        .setScmId(scmId)
        .setOmId(omId)
        .setOmLayoutVersion(OMLayoutFeature.INITIAL_VERSION.layoutVersion())
        .build();
//    cluster.waitForClusterToBeReady();
    client = cluster.newClient();

    volumeName = HddsClientUtils.getDefaultS3VolumeName(conf);
    bucketName = RandomStringUtils.randomAlphabetic(10).toLowerCase();

    preFinalizationChecks(getStoreForAccessID());
    finalizeOMUpgrade();
  }

  @AfterAll
  public static void shutdownCluster() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
    FileUtils.deleteQuietly(metaDir.toFile());
  }

  @Test
  public void testFCQUser() throws IOException {
    ObjectStore store = client.getObjectStore();
    Assertions.assertEquals(volumeName, store.getS3Volume().getName());

    // Create bucket.
    store.createS3Bucket(bucketName);
    Assertions.assertEquals(volumeName,
        store.getS3Bucket(bucketName).getVolumeName());


    LOG.info("xbis1: " + store.getS3Bucket(bucketName).getOwner());
  }

  private static void expectFailurePreFinalization(LambdaTestUtils.VoidCallable eval)
      throws Exception {
    LambdaTestUtils.intercept(OMException.class,
        "cannot be invoked before finalization", eval);
  }

  /**
   * Perform sanity checks before triggering upgrade finalization.
   */
  private static void preFinalizationChecks(ObjectStore store)
      throws Exception {
    // S3 get/set/revoke secret APIs still work before finalization
    final String accessId = "testUser1accessId1";
    S3SecretValue s3SecretValue = store.getS3Secret(accessId);
    Assertions.assertEquals(accessId, s3SecretValue.getAwsAccessKey());
    final String setSecret = "testsecret";
    s3SecretValue = store.setS3Secret(accessId, setSecret);
    Assertions.assertEquals(accessId, s3SecretValue.getAwsAccessKey());
    Assertions.assertEquals(setSecret, s3SecretValue.getAwsSecret());
    store.revokeS3Secret(accessId);
  }

  /**
   * Trigger OM upgrade finalization from the client and block until completion
   * (status FINALIZATION_DONE).
   */
  private static void finalizeOMUpgrade()
      throws IOException, InterruptedException, TimeoutException {

    // Trigger OM upgrade finalization. Ref: FinalizeUpgradeSubCommand#call
    final OzoneManagerProtocol omClient = client.getObjectStore()
        .getClientProxy().getOzoneManagerClient();
    final String upgradeClientID = "Test-Upgrade-Client-" + UUID.randomUUID();
    UpgradeFinalizer.StatusAndMessages finalizationResponse =
        omClient.finalizeUpgrade(upgradeClientID);

    // The status should transition as soon as the client call above returns
    Assert.assertTrue(isStarting(finalizationResponse.status()));

    // Wait for the finalization to be marked as done.
    // 10s timeout should be plenty.
    GenericTestUtils.waitFor(() -> {
      try {
        final UpgradeFinalizer.StatusAndMessages progress =
            omClient.queryUpgradeFinalizationProgress(
                upgradeClientID, false, false);
        return isDone(progress.status());
      } catch (IOException e) {
        Assert.fail("Unexpected exception while waiting for "
            + "the OM upgrade to finalize: " + e.getMessage());
      }
      return false;
    }, 500, 10000);
  }

  private static ObjectStore getStoreForAccessID()
      throws IOException {
    // Cluster provider will modify our provided configuration. We must use
    // this version to build the client.
    OzoneConfiguration conf = cluster.getOzoneManager().getConfiguration();
    // Manually construct an object store instead of using the cluster
    // provided one so we can specify the access ID.
    RpcClient rpcClient = new RpcClient(conf, null);
    // userPrincipal is set to be the same as accessId for the test
    rpcClient.setThreadLocalS3Auth(
        new S3Auth("testUser1accessId1", ACCESS_ID, ACCESS_ID, ACCESS_ID));
    return new ObjectStore(conf, rpcClient);
  }
}
