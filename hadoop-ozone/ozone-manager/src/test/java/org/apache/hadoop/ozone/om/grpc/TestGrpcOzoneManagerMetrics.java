/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.ozone.om.grpc;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.grpc.metrics.GrpcOzoneManagerMetrics;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocolPB.GrpcOmTransportFactory;
import org.apache.hadoop.ozone.om.protocolPB.OmTransportFactory;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.GrpcOmTransport;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for GrpcOzoneManagerMetrics.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestGrpcOzoneManagerMetrics {

  private OzoneManager ozoneManager;
  private OzoneManagerProtocol grpcClient;
  private GrpcOzoneManagerMetrics metrics;

  private static final String S3_VOL_DEFAULT =
      OzoneConfigKeys.OZONE_S3_VOLUME_NAME_DEFAULT;
  private static final String BUCKET_ONE = "bucket1";
  private static final String TRANSPORT_CLASS =
      GrpcOmTransportFactory.class.getName();

  @BeforeAll
  public void setUp(@TempDir Path tempDir)
      throws IOException, AuthenticationException {
    OzoneConfiguration conf = new OzoneConfiguration();

    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, tempDir.toString());
    conf.set(OMConfigKeys.OZONE_OM_S3_GPRC_SERVER_ENABLED, "true");
    conf.set(OMConfigKeys.OZONE_OM_TRANSPORT_CLASS, TRANSPORT_CLASS);

    OmTestManagers omTestManagers = new OmTestManagers(conf);
    ozoneManager = omTestManagers.getOzoneManager();
    grpcClient = omTestManagers.getWriteClient();

//    String omServiceId = "";
//    String clientId = UUID.randomUUID().toString();
//    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
//    OmTransport omTransport = OmTransportFactory.create(conf, ugi, omServiceId);
//
//    clientSideTranslatorPB =
//        new OzoneManagerProtocolClientSideTranslatorPB(omTransport, clientId);

    GrpcOzoneManagerServer server = ozoneManager.getOmS3gGrpcServer(conf);
    // Metrics get created inside the server constructor
    metrics = server.getOmS3gGrpcMetrics();

    // Create bucket1 under s3v volume
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(S3_VOL_DEFAULT)
        .setBucketName(BUCKET_ONE)
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .build();

    grpcClient.createBucket(bucketInfo);
  }

  @Test
  public void testBytesSent() throws IOException {

    OmKeyArgs keyArgs = createKeyArgs();

    OpenKeySession keySession = grpcClient.openKey(keyArgs);
    grpcClient.commitKey(keyArgs, keySession.getId());

    // metrics.getSentBytes() should have a value greater than 0
    assertTrue(metrics.getSentBytes().value() > 0);
  }


  /**
   * ozoneManager.stop() will also unregister the metrics.
   */
  @AfterAll
  public void stopServer() {
    ozoneManager.stop();
  }

  private OmKeyArgs createKeyArgs() throws IOException {
    ReplicationConfig repConfig = RatisReplicationConfig
        .getInstance(HddsProtos.ReplicationFactor.THREE);

    OmKeyLocationInfo keyLocationInfo = new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(new ContainerBlockID(1, 1)))
        .setPipeline(MockPipeline.createSingleNodePipeline())
        .build();
    keyLocationInfo.setCreateVersion(0);

    String keyName = UUID.randomUUID().toString();
    return new OmKeyArgs.Builder()
        .setLocationInfoList(Collections.singletonList(keyLocationInfo))
        .setVolumeName(S3_VOL_DEFAULT)
        .setBucketName(BUCKET_ONE)
        .setKeyName(keyName)
        .setAcls(Lists.emptyList())
        .setReplicationConfig(repConfig)
        .build();
  }
}
