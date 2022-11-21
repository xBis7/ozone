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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.grpc.client.OzoneClientStub;
import org.apache.hadoop.ozone.om.grpc.metrics.GrpcOzoneManagerMetrics;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.s3.endpoint.ObjectEndpoint;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  private ObjectEndpoint objectEndpoint;

  private OzoneClient s3gClient1Stub;
  private OzoneClient s3gClient2;
  private OzoneClient s3gClient3;
  private OzoneClient s3gClient4;

  public static final String CONTENT = "0123456789";
  private String bucketName = "bucket1";
  private String keyName = "key=value/1";

  @BeforeEach
  public void setUp() throws IOException {
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

    try {
      server.start();
    } catch (IOException ex) {
      LOG.error("Grpc Ozone Manager server failed to start", ex);
    }

    // Metrics get created inside the server constructor
    metrics = server.getOmS3gGrpcMetrics();

    //Create client stub and object store stub.
    s3gClient1Stub = new OzoneClientStub();

    // Create bucket
    s3gClient1Stub.getObjectStore().createS3Bucket(bucketName);

    // Create PutObject and setClient to OzoneClientStub
    objectEndpoint = new ObjectEndpoint();
    objectEndpoint.setClient(s3gClient1Stub);
    objectEndpoint.setOzoneConfiguration(conf);
  }

  @Test
  public void testBytesSent() throws IOException, OS3Exception {
    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    objectEndpoint.setHeaders(headers);

    Response response = objectEndpoint.put(bucketName, keyName, CONTENT
        .length(), 1, null, body);

    OzoneInputStream ozoneInputStream =
        s3gClient1Stub.getObjectStore().getS3Bucket(bucketName)
            .readKey(keyName);
    String keyContent =
        IOUtils.toString(ozoneInputStream, UTF_8);

    assertEquals(200, response.getStatus());
    assertEquals(CONTENT, keyContent);

    // metrics.getSentBytes() should have a value greater than 0
    assertTrue(metrics.getSentBytes().value() > 0);
  }


  /**
   * Server.stop() will also unregister the metrics.
   */
  @AfterEach
  public void stopServer() {
    server.stop();
  }
}
