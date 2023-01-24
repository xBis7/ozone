package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.ipc.DecayRpcScheduler;
import org.apache.hadoop.ipc.FairCallQueue;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_BACKOFF_ENABLE;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CALLQUEUE_IMPL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_IDENTITY_PROVIDER_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_NAMESPACE;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_SCHEDULER_IMPL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_SCHEDULER_PRIORITY_LEVELS_KEY;
import static org.apache.hadoop.ipc.WeightedRoundRobinMultiplexer.IPC_CALLQUEUE_WRRMUX_WEIGHTS_KEY;
import static org.apache.hadoop.ozone.admin.scm.FinalizeUpgradeCommandUtil.isDone;
import static org.apache.hadoop.ozone.admin.scm.FinalizeUpgradeCommandUtil.isStarting;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTITENANCY_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_PORT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_S3_GPRC_SERVER_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_TRANSPORT_CLASS;

public class TestFairCallQueue {
  private static MiniOzoneCluster cluster;
  private static String s3VolumeName;

  private static final String TENANT_ID = "tenant";
  private static final String USER_PRINCIPAL = "username";
  private static final String BUCKET_NAME = "bucket";
  private static final String ACCESS_ID = "tenant$username";

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(
        OMMultiTenantManagerImpl.OZONE_OM_TENANT_DEV_SKIP_RANGER, true);
    conf.setBoolean(OZONE_OM_MULTITENANCY_ENABLED, true);
    conf.set(OZONE_OM_S3_GPRC_SERVER_ENABLED, "false");
    conf.set(OZONE_OM_TRANSPORT_CLASS,
        "org.apache.hadoop.ozone.om.protocolPB.Hadoop3OmTransportFactory");

    String ozonePort = String.valueOf(OZONE_OM_PORT_DEFAULT);
    String ipcNamespace = IPC_NAMESPACE + "." + ozonePort + ".";

    conf.set(OZONE_OM_ADDRESS_KEY, "localhost:" + ozonePort);
    conf.set((ipcNamespace + IPC_CALLQUEUE_IMPL_KEY), FairCallQueue.class.toString());
    conf.set((ipcNamespace + IPC_SCHEDULER_IMPL_KEY), DecayRpcScheduler.class.toString());
    conf.set((ipcNamespace + IPC_IDENTITY_PROVIDER_KEY), OzoneIdentityProvider.class.toString());
    conf.set((ipcNamespace + IPC_SCHEDULER_PRIORITY_LEVELS_KEY), "2");
    conf.set((ipcNamespace + IPC_BACKOFF_ENABLE), "true");
    conf.set((ipcNamespace + IPC_CALLQUEUE_WRRMUX_WEIGHTS_KEY), "99,1");

    MiniOzoneCluster.Builder builder = MiniOzoneCluster.newBuilder(conf)
        .withoutDatanodes()
        .setOmLayoutVersion(OMLayoutFeature.INITIAL_VERSION.layoutVersion());
    cluster = builder.build();
    s3VolumeName = HddsClientUtils.getDefaultS3VolumeName(conf);

//    preFinalizationChecks(getStoreForAccessID(ACCESS_ID));
    finalizeOMUpgrade();
  }

  @AfterAll
  public static void shutdown() {
    cluster.shutdown();
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

    // None of the tenant APIs is usable before the upgrade finalization step
    expectFailurePreFinalization(
        store::listTenant);
    expectFailurePreFinalization(() ->
        store.listUsersInTenant(TENANT_ID, ""));
    expectFailurePreFinalization(() ->
        store.tenantGetUserInfo(USER_PRINCIPAL));
    expectFailurePreFinalization(() ->
        store.createTenant(TENANT_ID));
    expectFailurePreFinalization(() ->
        store.tenantAssignUserAccessId(USER_PRINCIPAL, TENANT_ID, ACCESS_ID));
    expectFailurePreFinalization(() ->
        store.tenantAssignAdmin(USER_PRINCIPAL, TENANT_ID, true));
    expectFailurePreFinalization(() ->
        store.tenantRevokeAdmin(ACCESS_ID, TENANT_ID));
    expectFailurePreFinalization(() ->
        store.tenantRevokeUserAccessId(ACCESS_ID));
    expectFailurePreFinalization(() ->
        store.deleteTenant(TENANT_ID));

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
    final OzoneManagerProtocol client = cluster.getRpcClient().getObjectStore()
        .getClientProxy().getOzoneManagerClient();
    final String upgradeClientID = "Test-Upgrade-Client-" + UUID.randomUUID();
    UpgradeFinalizer.StatusAndMessages finalizationResponse =
        client.finalizeUpgrade(upgradeClientID);

    // The status should transition as soon as the client call above returns
    Assertions.assertTrue(isStarting(finalizationResponse.status()));

    // Wait for the finalization to be marked as done.
    // 10s timeout should be plenty.
    GenericTestUtils.waitFor(() -> {
      try {
        final UpgradeFinalizer.StatusAndMessages progress =
            client.queryUpgradeFinalizationProgress(
                upgradeClientID, false, false);
        return isDone(progress.status());
      } catch (IOException e) {
        Assertions.fail("Unexpected exception while waiting for "
            + "the OM upgrade to finalize: " + e.getMessage());
      }
      return false;
    }, 500, 10000);
  }

  @Test
  public void testS3TenantVolume() throws Exception {

    ObjectStore store = getStoreForAccessID(ACCESS_ID);

    store.createTenant(TENANT_ID);
    store.tenantAssignUserAccessId(USER_PRINCIPAL, TENANT_ID, ACCESS_ID);

    // S3 volume pointed to by the store should be for the tenant.
    Assertions.assertEquals(TENANT_ID, store.getS3Volume().getName());

    // Create bucket in the tenant volume.
    store.createS3Bucket(BUCKET_NAME);
    OzoneBucket bucket = store.getS3Bucket(BUCKET_NAME);
    Assertions.assertEquals(TENANT_ID, bucket.getVolumeName());

    cluster.getClient().getObjectStore();


    // A different user should not see bucket, since they will be directed to
    // the s3 volume.
    ObjectStore store2 = getStoreForAccessID(UUID.randomUUID().toString());
    assertS3BucketNotFound(store2, BUCKET_NAME);

    // Delete bucket.
    store.deleteS3Bucket(BUCKET_NAME);
    assertS3BucketNotFound(store, BUCKET_NAME);

    store.tenantRevokeUserAccessId(ACCESS_ID);
    store.deleteTenant(TENANT_ID);
    store.deleteVolume(TENANT_ID);
  }

  /**
   * Checks that the bucket is not found using
   * {@link ObjectStore#getS3Bucket} and the designated S3 volume pointed to
   * by the ObjectStore.
   */
  private void assertS3BucketNotFound(ObjectStore store, String bucketName)
      throws IOException {
    try {
      store.getS3Bucket(bucketName);
    } catch (OMException ex) {
      if (ex.getResult() != OMException.ResultCodes.BUCKET_NOT_FOUND) {
        throw ex;
      }
    }

    try {
      OzoneVolume volume = store.getS3Volume();
      volume.getBucket(bucketName);
    } catch (OMException ex) {
      if (ex.getResult() != OMException.ResultCodes.BUCKET_NOT_FOUND) {
        throw ex;
      }
    }
  }

  private static ObjectStore getStoreForAccessID(String accessID)
      throws IOException {
    // Cluster provider will modify our provided configuration. We must use
    // this version to build the client.
    OzoneConfiguration conf = cluster.getOzoneManager().getConfiguration();
    // Manually construct an object store instead of using the cluster
    // provided one so we can specify the access ID.
    RpcClient client = new RpcClient(conf, null);
    // userPrincipal is set to be the same as accessId for the test
    client.setThreadLocalS3Auth(
        new S3Auth("unused1", "unused2", accessID, accessID));
    return new ObjectStore(conf, client);
  }
}
