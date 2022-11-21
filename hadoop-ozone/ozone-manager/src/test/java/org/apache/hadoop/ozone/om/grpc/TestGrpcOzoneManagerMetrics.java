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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.grpc.metrics.GrpcOzoneManagerMetrics;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Tests for GrpcOzoneManagerMetrics.
 */
public class TestGrpcOzoneManagerMetrics {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestGrpcOzoneManagerMetrics.class);
  private OzoneManager ozoneManager;
  private OzoneManagerProtocolServerSideTranslatorPB omServerProtocol;
  private GrpcOzoneManagerServer server;
  private GrpcOzoneManagerMetrics metrics;
  private OzoneClient s3gClient1;
  private OzoneClient s3gClient2;
  private OzoneClient s3gClient3;
  private OzoneClient s3gClient4;

  @BeforeAll
  public void setUp() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_S3_GPRC_SERVER_ENABLED, "true");
    conf.set(OMConfigKeys.OZONE_OM_TRANSPORT_CLASS,
        "org.apache.hadoop.ozone.om.protocolPB.GrpcOmTransportFactory");

    ozoneManager = Mockito.mock(OzoneManager.class);
    omServerProtocol = ozoneManager.getOmServerProtocol();

    server = new GrpcOzoneManagerServer(conf,
        omServerProtocol,
        ozoneManager.getDelegationTokenMgr(),
        ozoneManager.getCertificateClient());

    // Metrics get created inside the server constructor
    metrics = server.getOmS3gGrpcMetrics();

    try {
      server.start();
    } catch (IOException ex) {
      LOG.error("Grpc Ozone Manager server failed to start", ex);
    }

    // Create s3g client stubs
//    s3gClient1 = new OzoneClient();
  }

  @Test
  public void testBytesSent() {

  }


  /**
   * Server.stop() will also unregister the metrics.
   */
  @AfterAll
  public void stopServer() {
    server.stop();
  }
}
