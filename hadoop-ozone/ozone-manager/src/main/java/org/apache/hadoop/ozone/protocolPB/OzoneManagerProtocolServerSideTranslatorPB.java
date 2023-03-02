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
package org.apache.hadoop.ozone.protocolPB;

import static org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus.LEADER_AND_READY;
import static org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus.NOT_LEADER;
import static org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils.createClientRequest;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.PrepareStatus;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.OzoneProtocolMessageDispatcher;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.ipc.ClientId;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.callqueue.CallHandler;
import org.apache.hadoop.ozone.callqueue.OMQueueCall;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerDoubleBuffer;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.validation.RequestValidations;
import org.apache.hadoop.ozone.om.request.validation.ValidationContext;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import com.google.protobuf.ProtocolMessageEnum;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.ozone.security.S3SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link OzoneManagerProtocolPB}
 * to the OzoneManagerService server implementation.
 */
public class OzoneManagerProtocolServerSideTranslatorPB implements
    OzoneManagerProtocolPB {
  private static final Logger LOG = LoggerFactory
      .getLogger(OzoneManagerProtocolServerSideTranslatorPB.class);
  private static final String OM_REQUESTS_PACKAGE = 
      "org.apache.hadoop.ozone";
  
  private final OzoneManagerRatisServer omRatisServer;
  private final RequestHandler handler;
  private final boolean isRatisEnabled;
  private final OzoneManager ozoneManager;
  private final OzoneManagerDoubleBuffer ozoneManagerDoubleBuffer;
  private final AtomicLong transactionIndex;
  private final OzoneProtocolMessageDispatcher<OMRequest, OMResponse,
      ProtocolMessageEnum> dispatcher;
  private final RequestValidations requestValidations;
  private final CallHandler callHandler;

  /**
   * Constructs an instance of the server handler.
   *
   * @param impl OzoneManagerProtocolPB
   */
  public OzoneManagerProtocolServerSideTranslatorPB(
      OzoneManager impl,
      OzoneManagerRatisServer ratisServer,
      ProtocolMessageMetrics<ProtocolMessageEnum> metrics,
      boolean enableRatis,
      long lastTransactionIndexForNonRatis) {
    this.ozoneManager = impl;
    callHandler = new CallHandler(ozoneManager.getConf());
    this.isRatisEnabled = enableRatis;
    // Update the transactionIndex with the last TransactionIndex read from DB.
    // New requests should have transactionIndex incremented from this index
    // onwards to ensure unique objectIDs.
    this.transactionIndex = new AtomicLong(lastTransactionIndexForNonRatis);

    if (isRatisEnabled) {
      // In case of ratis is enabled, handler in ServerSideTransaltorPB is used
      // only for read requests and read requests does not require
      // double-buffer to be initialized.
      this.ozoneManagerDoubleBuffer = null;
      handler = new OzoneManagerRequestHandler(impl, null);
    } else {
      this.ozoneManagerDoubleBuffer = new OzoneManagerDoubleBuffer.Builder()
          .setOmMetadataManager(ozoneManager.getMetadataManager())
          // Do nothing.
          // For OM NON-HA code, there is no need to save transaction index.
          // As we wait until the double buffer flushes DB to disk.
          .setOzoneManagerRatisSnapShot((i) -> {
          })
          .enableRatis(isRatisEnabled)
          .enableTracing(TracingUtil.isTracingEnabled(
              ozoneManager.getConfiguration()))
          .build();
      handler = new OzoneManagerRequestHandler(impl, ozoneManagerDoubleBuffer);
    }
    this.omRatisServer = ratisServer;
    dispatcher = new OzoneProtocolMessageDispatcher<>("OzoneProtocol",
        metrics, LOG, OMPBHelper::processForDebug, OMPBHelper::processForDebug);
    // TODO: make this injectable for testing...
    requestValidations =
        new RequestValidations()
            .fromPackage(OM_REQUESTS_PACKAGE)
            .withinContext(
                ValidationContext.of(ozoneManager.getVersionManager(),
                    ozoneManager.getMetadataManager()))
            .load();

  }

  /**
   * Submit mutating requests to Ratis server in OM, and process read requests.
   */
  @Override
  public OMResponse submitRequest(RpcController controller,
      OMRequest request) throws ServiceException {
    OMRequest validatedRequest;
    try {
      validatedRequest = requestValidations.validateRequest(request);
    } catch (Exception e) {
      if (e instanceof OMException) {
        return createErrorResponse(request, (OMException) e);
      }
      throw new ServiceException(e);
    }

    OMResponse response = dispatcher.processRequest(validatedRequest,
        this::processRequest,
        request.getCmdType(),
        request.getTraceID());

    return requestValidations.validateResponse(request, response);
  }

  private OMResponse processRequest(OMRequest request) {
    UserGroupInformation ugi;
    if (request.hasS3Authentication()) {
      ugi = UserGroupInformation
          .createRemoteUser(request.getS3Authentication().getAccessId());
      LOG.info("xbis111: S3 before queue: cmdType: " +
          request.getCmdType() + " / ugi: " + ugi);
    } else {
      ugi = ProtobufRpcEngine.Server.getRemoteUser();
      LOG.info("xbis222: non-S3 before queue: cmdType: " +
          request.getCmdType() + " / ugi: " + ugi);
    }

    FutureOMResponseTask futureResponseTask =
        new FutureOMResponseTask(request, ugi);
    FutureTask<OMResponse> omResponseFuture =
        new FutureTask<>(futureResponseTask);

    OMQueueCall omQueueCall = new OMQueueCall(request,
        omResponseFuture, ugi);

    callHandler.addRequestToQueue(omQueueCall);

    try {
      return omQueueCall.getOmResponseFuture().get();
    } catch (InterruptedException | ExecutionException ex) {
      throw new RuntimeException(ex);
    }
  }

  private class FutureOMResponseTask implements Callable<OMResponse> {

    private final OMRequest omRequest;
    private final UserGroupInformation ugi;

    FutureOMResponseTask(OMRequest omRequest,
                         UserGroupInformation ugi) {
      this.omRequest = omRequest;
      this.ugi = ugi;
    }

    @Override
    public OMResponse call() throws Exception {
      AtomicInteger callCount = new AtomicInteger(0);

      Server.Call call = new Server.Call(1,
          callCount.incrementAndGet(),
          null,
          null,
          RPC.RpcKind.RPC_PROTOCOL_BUFFER,
          ClientId.getClientId());
      Server.getCurCall().set(call);

      CallHandler.CURRENT_UGI.set(ugi);

      LOG.info("xbis333: after queue: cmdType: " + omRequest.getCmdType() +
          " / server user: " + ugi);
      return handleRequest(omRequest, ugi);
    }
  }

  private OMResponse handleRequest(OMRequest request, UserGroupInformation ugi)
      throws ServiceException {
    OMClientRequest omClientRequest = null;
    boolean s3Auth = false;

    try {
      if (request.hasS3Authentication()) {
        OzoneManager.setS3Auth(request.getS3Authentication());
        try {
          s3Auth = true;
          // If request has S3Authentication, validate S3 credentials.
          // If current OM is leader and then proceed with the request.
          S3SecurityUtil.validateS3Credential(request, ozoneManager);
        } catch (IOException ex) {
          return createErrorResponse(request, ex);
        }
      }

      if (!isRatisEnabled) {
        return submitRequestDirectlyToOM(request);
      }

      if (OmUtils.isReadOnly(request)) {
        return submitReadRequestToOM(request);
      }

      // To validate credentials we have already verified leader status.
      // This will skip of checking leader status again if request has S3Auth.
      if (!s3Auth) {
        OzoneManagerRatisUtils.checkLeaderStatus(ozoneManager);
      }
      try {
        omClientRequest = createClientRequest(request, ozoneManager);
        // TODO: Note: Due to HDDS-6055, createClientRequest() could now
        //  return null, which triggered the findbugs warning.
        //  Added the assertion.
        assert (omClientRequest != null);
        request = omClientRequest.preExecute(ozoneManager);
      } catch (IOException ex) {
        if (omClientRequest != null) {
          omClientRequest.handleRequestFailure(ozoneManager);
        }
        return createErrorResponse(request, ex);
      }

      OMResponse response = submitRequestToRatis(request);
      if (!response.getSuccess()) {
        omClientRequest.handleRequestFailure(ozoneManager);
      }
      return response;
    } finally {
      OzoneManager.setS3Auth(null);
    }
  }

  /**
   * Submits request to OM's Ratis server.
   */
  private OMResponse submitRequestToRatis(OMRequest request)
      throws ServiceException {
    return omRatisServer.submitRequest(request);
  }

  private OMResponse submitReadRequestToOM(OMRequest request)
      throws ServiceException {
    // Check if this OM is the leader.
    RaftServerStatus raftServerStatus = omRatisServer.checkLeaderStatus();
    if (raftServerStatus == LEADER_AND_READY ||
        request.getCmdType().equals(PrepareStatus)) {
      return handler.handleReadRequest(request);
    } else {
      throw createLeaderErrorException(raftServerStatus);
    }
  }

  private ServiceException createLeaderErrorException(
      RaftServerStatus raftServerStatus) {
    if (raftServerStatus == NOT_LEADER) {
      return createNotLeaderException();
    } else {
      return createLeaderNotReadyException();
    }
  }

  private ServiceException createNotLeaderException() {
    RaftPeerId raftPeerId = omRatisServer.getRaftPeerId();
    RaftPeerId raftLeaderId = omRatisServer.getRaftLeaderId();
    String raftLeaderAddress = omRatisServer.getRaftLeaderAddress();

    OMNotLeaderException notLeaderException =
        raftLeaderId == null ? new OMNotLeaderException(raftPeerId) :
            new OMNotLeaderException(raftPeerId, raftLeaderId,
                raftLeaderAddress);

    LOG.debug(notLeaderException.getMessage());

    return new ServiceException(notLeaderException);
  }

  private ServiceException createLeaderNotReadyException() {
    RaftPeerId raftPeerId = omRatisServer.getRaftPeerId();

    OMLeaderNotReadyException leaderNotReadyException =
        new OMLeaderNotReadyException(raftPeerId.toString() + " is Leader " +
            "but not ready to process request yet.");

    LOG.debug(leaderNotReadyException.getMessage());

    return new ServiceException(leaderNotReadyException);
  }

  /**
   * Submits request directly to OM.
   */
  private OMResponse submitRequestDirectlyToOM(OMRequest request) {
    OMClientResponse omClientResponse;
    try {
      if (OmUtils.isReadOnly(request)) {
        return handler.handleReadRequest(request);
      } else {
        OMClientRequest omClientRequest =
            createClientRequest(request, ozoneManager);
        request = omClientRequest.preExecute(ozoneManager);
        long index = transactionIndex.incrementAndGet();
        omClientResponse = handler.handleWriteRequest(request, index);
      }
    } catch (IOException ex) {
      // As some preExecute returns error. So handle here.
      return createErrorResponse(request, ex);
    }
    try {
      omClientResponse.getFlushFuture().get();
      if (LOG.isTraceEnabled()) {
        LOG.trace("Future for {} is completed", request);
      }
    } catch (ExecutionException | InterruptedException ex) {
      // terminate OM. As if we are in this stage means, while getting
      // response from flush future, we got an exception.
      String errorMessage = "Got error during waiting for flush to be " +
          "completed for " + "request" + request.toString();
      ExitUtils.terminate(1, errorMessage, ex, LOG);
      Thread.currentThread().interrupt();
    }
    return omClientResponse.getOMResponse();
  }

  /**
   * Create OMResponse from the specified OMRequest and exception.
   *
   * @param omRequest
   * @param exception
   * @return OMResponse
   */
  private OMResponse createErrorResponse(
      OMRequest omRequest, IOException exception) {
    // Added all write command types here, because in future if any of the
    // preExecute is changed to return IOException, we can return the error
    // OMResponse to the client.
    OMResponse.Builder omResponse = OMResponse.newBuilder()
        .setStatus(OzoneManagerRatisUtils.exceptionToResponseStatus(exception))
        .setCmdType(omRequest.getCmdType())
        .setTraceID(omRequest.getTraceID())
        .setSuccess(false);
    if (exception.getMessage() != null) {
      omResponse.setMessage(exception.getMessage());
    }
    return omResponse.build();
  }

  public void stop() {
    if (!isRatisEnabled) {
      ozoneManagerDoubleBuffer.stop();
    }
  }

  public static Logger getLog() {
    return LOG;
  }
}
