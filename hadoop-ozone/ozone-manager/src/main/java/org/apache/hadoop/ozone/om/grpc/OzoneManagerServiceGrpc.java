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
package org.apache.hadoop.ozone.om.grpc;

import io.grpc.Status;
import com.google.protobuf.RpcController;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.ipc.ClientId;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.om.grpc.metrics.GrpcOzoneManagerMetrics;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerServiceGrpc.OzoneManagerServiceImplBase;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.OzoneDelegationTokenSecretManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Grpc Service for handling S3 gateway OzoneManagerProtocol client requests.
 */
public class OzoneManagerServiceGrpc extends OzoneManagerServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerServiceGrpc.class);
  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;
  private final OzoneManagerProtocolServerSideTranslatorPB omTranslator;
  private final OzoneDelegationTokenSecretManager delegationTokenMgr;
  private final SecurityConfig secConfig;

  private final GrpcOzoneManagerMetrics omGrpcMetrics;
  private long sendCount;
  private long receiveCount;

  private final List<String> clientList;
  private final List<Long> avgProcessingTimeList;
  private final List<OMRequest> queueList;

  OzoneManagerServiceGrpc(
      OzoneManagerProtocolServerSideTranslatorPB omTranslator,
      OzoneDelegationTokenSecretManager delegationTokenMgr,
      OzoneConfiguration configuration,
      GrpcOzoneManagerMetrics omGrpcMetrics) {
    this.omTranslator = omTranslator;
    this.delegationTokenMgr = delegationTokenMgr;
    this.secConfig = new SecurityConfig(configuration);
    this.omGrpcMetrics = omGrpcMetrics;
    this.sendCount = 0;
    this.receiveCount = 0;
    this.clientList = new LinkedList<>();
    this.avgProcessingTimeList = new LinkedList<>();
    this.queueList = new LinkedList<>();
  }

  @Override
  public void submitRequest(OMRequest request,
                            io.grpc.stub.StreamObserver<OMResponse>
                                responseObserver) {
    long submitTime = System.nanoTime();
    queueList.add(request);
    omGrpcMetrics.setGrpcOmQueueLength(queueList.size());
    LOG.debug("OzoneManagerServiceGrpc: OzoneManagerServiceImplBase " +
        "processing s3g client submit request - for command {}",
        request.getCmdType().name());
    AtomicInteger callCount = new AtomicInteger(0);

    updateActiveClientNum(request);

    org.apache.hadoop.ipc.Server.getCurCall().set(new Server.Call(1,
        callCount.incrementAndGet(),
        null,
        null,
        RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        ClientId.getClientId()));
    // TODO: currently require setting the Server class for each request
    // with thread context (Server.Call()) that includes retries
    // and importantly random ClientId.  This is currently necessary for
    // Om Ratis Server to create createWriteRaftClientRequest.
    // Look to remove Server class requirement for issuing ratis transactions
    // for OMRequests.  Test through successful ratis-enabled OMRequest
    // handling without dependency on hadoop IPC based Server.
    long startTime = 0;
    try {
      startTime = System.nanoTime();
      queueList.remove(request);
      omGrpcMetrics.setGrpcOmQueueLength(queueList.size());

      OMResponse omResponse = this.omTranslator.
          submitRequest(NULL_RPC_CONTROLLER, request);
      responseObserver.onNext(omResponse);

      updateBytesTransferred(omResponse);
    } catch (Throwable e) {
      IOException ex = new IOException(e.getCause());
      responseObserver.onError(Status
          .INTERNAL
          .withDescription(ex.getMessage())
          .asRuntimeException());
    }
    responseObserver.onCompleted();

    long endTime = System.nanoTime();
    long queueTime = startTime - submitTime;
    long processingTime = endTime - startTime;

    // set metrics queue time
    omGrpcMetrics.addGrpcOmQueueTime(queueTime);

    // set metrics processing time
    omGrpcMetrics.addGrpcOmProcessingTime(processingTime);

    calculateProcessingTimeForDebugging(processingTime);
  }

  private void updateActiveClientNum(OMRequest omRequest) {
    String clientId = omRequest.getClientId();
    if (!clientList.contains(clientId)) {
      clientList.add(clientId);

      // This will get updated once we receive a request from the client.
      omGrpcMetrics.setNumActiveS3GClientConnections(clientList.size());
    }
  }

  private void updateBytesTransferred(OMResponse omResponse) {
    // bytes sent
    long createKeySize = omResponse
        .getCreateKeyResponse()
        .getKeyInfo()
        .getDataSize();
    sendCount += createKeySize;

    if (sendCount > 0) {
      omGrpcMetrics.setSentBytes(sendCount);
    }

    // bytes received
    long readKeySize = omResponse
        .getGetKeyInfoResponse()
        .getKeyInfo()
        .getDataSize();
    receiveCount += readKeySize;

    if (receiveCount > 0) {
      omGrpcMetrics.setReceivedBytes(receiveCount);
    }
  }

  private void calculateProcessingTimeForDebugging(long time) {
    avgProcessingTimeList.add(time);
    LOG.debug("Processing time in nanoseconds is {}", time);

    // calculate avg time for debugging
    // move it under a test method
    double avgTime = 0;
    for (long val : avgProcessingTimeList) {
      avgTime += val;
    }
    avgTime = avgTime / avgProcessingTimeList.size();

    LOG.debug("Avg proc time is {}", avgTime);
  }
}
