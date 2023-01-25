/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 *     http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.server.OzoneAdmins;
import org.apache.hadoop.ipc.DecayRpcScheduler;
import org.apache.hadoop.ipc.FairCallQueue;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.multitenant.Tenant;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.s3.security.S3GetSecretRequest;
import org.apache.hadoop.ozone.om.request.s3.tenant.OMTenantAssignUserAccessIdRequest;
import org.apache.hadoop.ozone.om.request.s3.tenant.OMTenantCreateRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.security.S3GetSecretResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMTenantAssignUserAccessIdResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMTenantCreateResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneNativeAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static java.util.Arrays.asList;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_BACKOFF_ENABLE;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CALLQUEUE_IMPL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_IDENTITY_PROVIDER_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_NAMESPACE;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_SCHEDULER_IMPL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_SCHEDULER_PRIORITY_LEVELS_KEY;
import static org.apache.hadoop.ipc.WeightedRoundRobinMultiplexer.IPC_CALLQUEUE_WRRMUX_WEIGHTS_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.admin.scm.FinalizeUpgradeCommandUtil.isDone;
import static org.apache.hadoop.ozone.admin.scm.FinalizeUpgradeCommandUtil.isStarting;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTITENANCY_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_PORT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_S3_GPRC_SERVER_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_TRANSPORT_CLASS;
import static org.apache.hadoop.security.authentication.util.KerberosName.DEFAULT_MECHANISM;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestFairCallQueue {
  private static MiniOzoneCluster cluster;
  private static OzoneManager ozoneManager;
  private final OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper =
      ((response, transactionIndex) -> null);
  private static String s3VolumeName;

  // Multi-tenant related vars
  private static final String USER_ALICE = "alice@EXAMPLE.COM";
  private static final String TENANT_ID = "tenant";
  private static final String USER_PRINCIPAL = "username";
  private static final String BUCKET_NAME = "bucket";
  private static final String ACCESS_ID = "tenant$username";
  private static final String ALICE_ACCESS_ID = "tenant$" + USER_ALICE;
  private static final String USER_BOB_SHORT = "bob";
  private static final String ACCESS_ID_BOB =
      OMMultiTenantManager.getDefaultAccessId(TENANT_ID, USER_BOB_SHORT);
  private static final String USER_CAROL = "carol@EXAMPLE.COM";

  private static UserGroupInformation ugiAlice;
  private static UserGroupInformation ugiCarol;

  private OMMultiTenantManager omMultiTenantManager;
  private Tenant tenant;

  @BeforeAll
  public static void setUp() throws Exception {
    KerberosName.setRuleMechanism(DEFAULT_MECHANISM);
    KerberosName.setRules(
        "RULE:[2:$1@$0](.*@EXAMPLE.COM)s/@.*//\n" +
            "RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//\n" +
            "DEFAULT");

    ugiAlice = UserGroupInformation.createRemoteUser(USER_ALICE);
    Assertions.assertEquals("alice", ugiAlice.getShortUserName());

    ugiCarol = UserGroupInformation.createRemoteUser(USER_CAROL);
    Assertions.assertEquals("carol", ugiCarol.getShortUserName());

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

    conf.set(OZONE_ADMINISTRATORS, USER_ALICE);

    Server.Call call = spy(new Server.Call(1, 1, null, null,
        RPC.RpcKind.RPC_BUILTIN, new byte[] {1, 2, 3}));
    // Run as alice, so that Server.getRemoteUser() won't return null.
    when(call.getRemoteUser()).thenReturn(ugiAlice);
    Server.getCurCall().set(call);

    MiniOzoneCluster.Builder builder = MiniOzoneCluster.newBuilder(conf)
        .withoutDatanodes()
        .setOmLayoutVersion(OMLayoutFeature.INITIAL_VERSION.layoutVersion());
    cluster = builder.build();
    s3VolumeName = HddsClientUtils.getDefaultS3VolumeName(conf);

//    preFinalizationChecks(getStoreForAccessID(ACCESS_ID));
    finalizeOMUpgrade();

    ozoneManager = cluster.getOzoneManager();
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

  @Test
  public void testGetSecretWithTenant() throws IOException {

//    // This effectively makes alice an S3 admin.
//    when(ozoneManager.isS3Admin(ugiAlice)).thenReturn(true);
//    // Make alice a non-delegated admin
//    when(omMultiTenantManager.isTenantAdmin(ugiAlice, TENANT_ID, false))
//        .thenReturn(true);

    // 1. CreateTenantRequest: Create tenant "finance".
    long txLogIndex = 1;
    // Run preExecute
    OMTenantCreateRequest omTenantCreateRequest =
        new OMTenantCreateRequest(
            new OMTenantCreateRequest(
                createTenantRequest(TENANT_ID)
            ).preExecute(ozoneManager)
        );
    // Run validateAndUpdateCache
    OMClientResponse omClientResponse =
        omTenantCreateRequest.validateAndUpdateCache(ozoneManager,
            txLogIndex, ozoneManagerDoubleBufferHelper);
    // Check response type and cast
    Assertions.assertTrue(omClientResponse instanceof OMTenantCreateResponse);
    final OMTenantCreateResponse omTenantCreateResponse =
        (OMTenantCreateResponse) omClientResponse;
    // Check response
    Assertions.assertTrue(omTenantCreateResponse.getOMResponse().getSuccess());
    Assertions.assertEquals(TENANT_ID,
        omTenantCreateResponse.getOmDBTenantState().getTenantId());

    ObjectStore store = getStoreForAccessID(ALICE_ACCESS_ID);
    store.tenantAssignUserAccessId(USER_ALICE, TENANT_ID, ALICE_ACCESS_ID);
    store.tenantAssignAdmin(ALICE_ACCESS_ID, TENANT_ID, false);

    // 2. AssignUserToTenantRequest: Assign "bob@EXAMPLE.COM" to "finance".
    ++txLogIndex;

    // Additional mock setup needed to pass accessId check
//    when(ozoneManager.getMultiTenantManager()).thenReturn(omMultiTenantManager);

    // Run preExecute
    OMTenantAssignUserAccessIdRequest omTenantAssignUserAccessIdRequest =
        new OMTenantAssignUserAccessIdRequest(
            new OMTenantAssignUserAccessIdRequest(
                assignUserToTenantRequest(TENANT_ID,
                    USER_BOB_SHORT, ACCESS_ID_BOB)
            ).preExecute(ozoneManager)
        );

    omMultiTenantManager = ozoneManager.getMultiTenantManager();

    // Run validateAndUpdateCache
    omClientResponse =
        omTenantAssignUserAccessIdRequest.validateAndUpdateCache(ozoneManager,
            txLogIndex, ozoneManagerDoubleBufferHelper);

    // Check response type and cast
    Assertions.assertTrue(
        omClientResponse instanceof OMTenantAssignUserAccessIdResponse);
    final OMTenantAssignUserAccessIdResponse
        omTenantAssignUserAccessIdResponse =
        (OMTenantAssignUserAccessIdResponse) omClientResponse;

    // Check response
    Assertions.assertTrue(omTenantAssignUserAccessIdResponse.getOMResponse()
        .getSuccess());
    Assertions.assertTrue(omTenantAssignUserAccessIdResponse.getOMResponse()
        .hasTenantAssignUserAccessIdResponse());
    final OmDBAccessIdInfo omDBAccessIdInfo =
        omTenantAssignUserAccessIdResponse.getOmDBAccessIdInfo();
    Assertions.assertNotNull(omDBAccessIdInfo);
    final S3SecretValue originalS3Secret =
        omTenantAssignUserAccessIdResponse.getS3Secret();
    Assertions.assertNotNull(originalS3Secret);


    // 3. S3GetSecretRequest: Get secret of "bob@EXAMPLE.COM" (as an admin).
    ++txLogIndex;

    // Run preExecute
    S3GetSecretRequest s3GetSecretRequest =
        new S3GetSecretRequest(
            new S3GetSecretRequest(
                s3GetSecretRequest(ACCESS_ID_BOB)
            ).preExecute(ozoneManager)
        );

    // Run validateAndUpdateCache
    omClientResponse =
        s3GetSecretRequest.validateAndUpdateCache(ozoneManager,
            txLogIndex, ozoneManagerDoubleBufferHelper);

    // Check response type and cast
    Assertions.assertTrue(omClientResponse instanceof S3GetSecretResponse);
    final S3GetSecretResponse s3GetSecretResponse =
        (S3GetSecretResponse) omClientResponse;

    // Check response
    Assertions.assertTrue(s3GetSecretResponse.getOMResponse().getSuccess());
    /*
       getS3SecretValue() should be null in this case because
       the entry is already inserted to DB in the previous request.
       The entry will get overwritten if it isn't null.
       See {@link S3GetSecretResponse#addToDBBatch}.
     */
    Assertions.assertNull(s3GetSecretResponse.getS3SecretValue());
    // The secret retrieved should be the same as previous response's.
    final OzoneManagerProtocolProtos.GetS3SecretResponse getS3SecretResponse =
        s3GetSecretResponse.getOMResponse().getGetS3SecretResponse();
    final OzoneManagerProtocolProtos.S3Secret s3Secret = getS3SecretResponse.getS3Secret();
    Assertions.assertEquals(ACCESS_ID_BOB, s3Secret.getKerberosID());
    Assertions.assertEquals(originalS3Secret.getAwsSecret(),
        s3Secret.getAwsSecret());
    Assertions.assertEquals(originalS3Secret.getKerberosID(),
        s3Secret.getKerberosID());
  }

  private OzoneManagerProtocolProtos.OMRequest createTenantRequest(String tenantNameStr) {

    return OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateTenant)
        .setCreateTenantRequest(
            OzoneManagerProtocolProtos.CreateTenantRequest.newBuilder()
                .setTenantId(tenantNameStr)
                .setVolumeName(tenantNameStr)
                .build()
        ).build();
  }

  private OzoneManagerProtocolProtos.OMRequest assignUserToTenantRequest(
      String tenantNameStr, String userPrincipalStr, String accessIdStr) {

    return OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.TenantAssignUserAccessId)
        .setTenantAssignUserAccessIdRequest(
            OzoneManagerProtocolProtos.TenantAssignUserAccessIdRequest.newBuilder()
                .setTenantId(tenantNameStr)
                .setUserPrincipal(userPrincipalStr)
                .setAccessId(accessIdStr)
                .build()
        ).build();
  }

  private OzoneManagerProtocolProtos.OMRequest s3GetSecretRequest(String userPrincipalStr) {

    return OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.GetS3Secret)
        .setGetS3SecretRequest(
            OzoneManagerProtocolProtos.GetS3SecretRequest.newBuilder()
                .setKerberosID(userPrincipalStr)
                .build()
        ).build();
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
