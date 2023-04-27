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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ipc.DecayRpcScheduler;
import org.apache.hadoop.ipc.FairCallQueue;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OzoneIdentityProvider;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.om.protocolPB.Hadoop3OmTransportFactory;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.s3.Gateway;
import org.apache.hadoop.ozone.s3.OzoneClientProducer;
import org.apache.hadoop.ozone.s3.OzoneConfigurationHolder;
import org.apache.hadoop.ozone.s3.S3GatewayHttpServer;
import org.apache.hadoop.ozone.s3.endpoint.ObjectEndpoint;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.metrics.S3GatewayMetrics;
import org.apache.hadoop.ozone.util.ShutdownHookManager;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.eclipse.jetty.server.Server;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CALLQUEUE_IMPL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_SCHEDULER_IMPL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_BACKOFF_ENABLE;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_SCHEDULER_PRIORITY_LEVELS_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_IDENTITY_PROVIDER_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.ozone.conf.OzoneServiceConfig.DEFAULT_SHUTDOWN_HOOK_PRIORITY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_S3_GPRC_SERVER_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_TRANSPORT_CLASS;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_KERBEROS_PRINCIPAL_KEY;
import static org.apache.ozone.test.GenericTestUtils.getTempPath;
import static org.mockito.Mockito.when;

/**
 * Tests for using the FairCallQueue with OM.
 */
public class TestFairCallQueueWithOM {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestFairCallQueueWithOM.class);
  private static final OzoneConfiguration CONF = 
      new OzoneConfiguration();
  private static final String CLUSTER_ID =
      UUID.randomUUID().toString();
  private static final String SCM_ID =
      UUID.randomUUID().toString();
  private static final String OM_ID =
      UUID.randomUUID().toString();

  private static MiniOzoneCluster cluster;
  private static OzoneClient client;
  private static ObjectEndpoint objectEndpoint;
  private static Path metaDir;

  private static String volumeName;
  private static String bucketName;
  private static final String KEY_NAME = "key=value/1";
  private static final String KEY_NAME_TWO = "key=value/2";

  private static final String CONTENT = "0123456789";
  private static final String TENANT_ID = "tenant";
  private static final String USER_PRINCIPAL = "username";
  private static final String ACCESS_ID = "tenant$username";

  @BeforeAll
  public static void initCluster() throws Exception {
    CONF.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, true);
    CONF.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        BucketLayout.LEGACY.name());

    CONF.set(OZONE_SCM_NAMES, "localhost");

    CONF.set(OZONE_OM_S3_GPRC_SERVER_ENABLED, "false");
    CONF.set(OZONE_OM_TRANSPORT_CLASS,
        Hadoop3OmTransportFactory.class.getName());

    final String path = getTempPath(UUID.randomUUID().toString());
    metaDir = Paths.get(path, "om-meta");
    CONF.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.toString());
    OzoneManager.setTestSecureOmFlag(true);

    cluster =  MiniOzoneCluster.newBuilder(CONF)
        .setClusterId(CLUSTER_ID)
        .setNumDatanodes(3)
        .setScmId(SCM_ID)
        .setOmId(OM_ID)
        .build();

    // MiniOzoneCluster assigns a random port to the OM
    // Get the random port
    String[] omAddressValues = CONF.get(OZONE_OM_ADDRESS_KEY).split(":");
    String port = omAddressValues[1];
    String namespace = "ipc." + port + ".";

    // Use the random port to set the FairCallQueue conf
    CONF.set(namespace + IPC_CALLQUEUE_IMPL_KEY,
        FairCallQueue.class.getName());
    CONF.set(namespace + IPC_SCHEDULER_IMPL_KEY,
        DecayRpcScheduler.class.getName());
    CONF.set(namespace + IPC_IDENTITY_PROVIDER_KEY,
        OzoneIdentityProvider.class.getName());
    CONF.set(namespace + IPC_BACKOFF_ENABLE, "true");
    CONF.set(namespace + IPC_SCHEDULER_PRIORITY_LEVELS_KEY, "2");

    CONF.setBoolean(S3Auth.S3_AUTH_CHECK, true);

    // Set again the cluster conf and restart the OM
    // so that it picks up the new conf
    cluster.setConf(CONF);
    cluster.getOzoneManager().stop();
    cluster.restartOzoneManager();
    // Wait for SCM to exit safe mode.
    cluster.waitForClusterToBeReady();

    volumeName = HddsClientUtils.getDefaultS3VolumeName(CONF);
    bucketName = RandomStringUtils.randomAlphabetic(10).toLowerCase();
  }

  @AfterAll
  public static void shutdownCluster() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
    FileUtils.deleteQuietly(metaDir.toFile());
  }

  public RpcClient getS3GInstance() throws Exception {
    UserGroupInformation.setConfiguration(CONF);

    // Login S3G user
    if (OzoneSecurityUtil.isSecurityEnabled(CONF)) {
      if (SecurityUtil.getAuthenticationMethod(CONF).equals(
          UserGroupInformation.AuthenticationMethod.KERBEROS)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Ozone security is enabled. Attempting login for S3G user. "
                  + "Principal: {}, keytab: {}",
              CONF.get(OZONE_S3G_KERBEROS_PRINCIPAL_KEY),
              CONF.get(OZONE_S3G_KERBEROS_KEYTAB_FILE_KEY));
        }

        SecurityUtil.login(CONF, OZONE_S3G_KERBEROS_KEYTAB_FILE_KEY,
            OZONE_S3G_KERBEROS_PRINCIPAL_KEY);
      } else {
        throw new AuthenticationException(SecurityUtil.getAuthenticationMethod(
            CONF) + " authentication method not supported. S3G user login "
            + "failed.");
      }
      LOG.info("S3Gateway login successful.");
    }

    // Set http base dir
    if (StringUtils.isEmpty(CONF.get(
        OzoneConfigKeys.OZONE_HTTP_BASEDIR))) {
      //Setting ozone.http.basedir to cwd if not set so that server setup
      // doesn't fail.
      File tmpMetaDir = Files.createTempDirectory(Paths.get(""),
          "ozone_s3g_tmp_base_dir").toFile();
      ShutdownHookManager.get().addShutdownHook(() -> {
        try {
          FileUtils.deleteDirectory(tmpMetaDir);
        } catch (IOException e) {
          LOG.error("Failed to cleanup temporary S3 Gateway Metadir {}",
              tmpMetaDir.getAbsolutePath(), e);
        }
      }, 0);
      CONF.set(OzoneConfigKeys.OZONE_HTTP_BASEDIR,
          tmpMetaDir.getAbsolutePath());
    }

    S3GatewayHttpServer httpServer = new S3GatewayHttpServer(CONF, "s3gateway");
    httpServer.start();

    ShutdownHookManager.get().addShutdownHook(() -> {
      try {
        httpServer.stop();
      } catch (Exception e) {
        LOG.error("Error during stop S3Gateway", e);
      }
    }, DEFAULT_SHUTDOWN_HOOK_PRIORITY);

//    OzoneClientProducer clientProducer = new OzoneClientProducer();
//    client = clientProducer.createClient();

    return new RpcClient(CONF, null);
  }

  @Test
  public void testFCQUser() throws Exception {
//    OzoneClientProducer clientProducer = new OzoneClientProducer();
//    clientProducer.setOzoneConfiguration(conf);
//    client = clientProducer.createClient();

    RpcClient rpcClient = getS3GInstance();

    ObjectStore store = new ObjectStore(CONF, rpcClient);

//    final String setSecret = "testsecret";
//    final String accessId = "testUser1accessId1";
//    S3SecretValue s3SecretValue = store.getS3Secret(accessId);
//    s3SecretValue = store.setS3Secret(accessId, setSecret);
//    rpcClient.setThreadLocalS3Auth(
//        new S3Auth("unused1", "unused2", ACCESS_ID, ACCESS_ID));

//    ObjectStore store = client.getObjectStore();
    Assertions.assertEquals(volumeName, store.getS3Volume().getName());

    LOG.info("xbis3: om address: " + CONF.get(OZONE_OM_ADDRESS_KEY));
    // Create bucket.
    store.createS3Bucket(bucketName);




    objectEndpoint = new ObjectEndpoint();
    objectEndpoint.setClient(client);
    objectEndpoint.setOzoneConfiguration(CONF);

    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    objectEndpoint.setHeaders(headers);

    Response response = objectEndpoint.put(bucketName, KEY_NAME, CONTENT
        .length(), 1, null, body);
    OzoneInputStream ozoneInputStream =
        store.getS3Bucket(bucketName)
            .readKey(KEY_NAME);
    String keyContent =
        org.apache.commons.io.IOUtils.toString(ozoneInputStream, UTF_8);

    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals(CONTENT, keyContent);

    String chunkedContent = "0a;chunk-signature=signature\r\n"
        + "1234567890\r\n"
        + "05;chunk-signature=signature\r\n"
        + "abcde\r\n";

    when(headers.getHeaderString("x-amz-content-sha256"))
        .thenReturn("STREAMING-AWS4-HMAC-SHA256-PAYLOAD");

    //WHEN
    response = objectEndpoint.put(bucketName, KEY_NAME_TWO,
        chunkedContent.length(), 1, null,
        new ByteArrayInputStream(chunkedContent.getBytes(UTF_8)));

    Assertions.assertEquals(volumeName,
        store.getS3Bucket(bucketName).getVolumeName());
  }
}
