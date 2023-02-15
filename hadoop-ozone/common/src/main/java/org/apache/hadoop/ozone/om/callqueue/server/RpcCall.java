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
package org.apache.hadoop.ozone.om.callqueue.server;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcConstants;
import org.apache.hadoop.ipc.RpcServerException;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tracing.Span;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/** A RPC extended call queued for handling. */
public class RpcCall extends Call {
  final OzoneServer.Connection connection;  // connection to client
  final Writable rpcRequest;    // Serialized Rpc request from client
  ByteBuffer rpcResponse;       // the response for this call

  private RpcCall.ResponseParams responseParams; // the response params
  private Writable rv;                   // the byte response

  RpcCall(RpcCall call) {
    super(call);
    this.connection = call.connection;
    this.rpcRequest = call.rpcRequest;
    this.rv = call.rv;
    this.responseParams = call.responseParams;
  }

  RpcCall(OzoneServer.Connection connection, int id) {
    this(connection, id, RpcConstants.INVALID_RETRY_COUNT);
  }

  RpcCall(OzoneServer.Connection connection, int id, int retryCount) {
    this(connection, id, retryCount, null,
        RPC.RpcKind.RPC_BUILTIN, RpcConstants.DUMMY_CLIENT_ID,
        null, null);
  }

  RpcCall(OzoneServer.Connection connection, int id, int retryCount,
          Writable param, RPC.RpcKind kind, byte[] clientId,
          Span span, CallerContext context) {
    super(id, retryCount, kind, clientId, span, context);
    this.connection = connection;
    this.rpcRequest = param;
  }

  @Override
  boolean isOpen() {
    return connection.channel.isOpen();
  }

  void setResponseFields(Writable returnValue,
                         RpcCall.ResponseParams responseParams) {
    this.rv = returnValue;
    this.responseParams = responseParams;
  }

  @Override
  public String getProtocol() {
    return "rpc";
  }

  @Override
  public UserGroupInformation getRemoteUser() {
    return connection.user;
  }

  @Override
  public InetAddress getHostInetAddress() {
    return connection.getHostInetAddress();
  }

  @Override
  public Void run() throws Exception {
    if (!connection.channel.isOpen()) {
      LOG.info(Thread.currentThread().getName() + ": skipped " + this);
      return null;
    }

    long startNanos = Time.monotonicNowNanos();
    Writable value = null;
    RpcCall.ResponseParams responseParams = new RpcCall.ResponseParams();

    try {
      value = call(
          rpcKind, connection.protocolName, rpcRequest, timestampNanos);
    } catch (Throwable e) {
      populateResponseParamsOnError(e, responseParams);
    }
    if (!isResponseDeferred()) {
      long deltaNanos = Time.monotonicNowNanos() - startNanos;
      OzoneProcessingDetails details = getProcessingDetails();

      details.set(OzoneProcessingDetails.Timing.PROCESSING, deltaNanos, TimeUnit.NANOSECONDS);
      deltaNanos -= details.get(OzoneProcessingDetails.Timing.LOCKWAIT, TimeUnit.NANOSECONDS);
      deltaNanos -= details.get(OzoneProcessingDetails.Timing.LOCKSHARED, TimeUnit.NANOSECONDS);
      deltaNanos -= details.get(OzoneProcessingDetails.Timing.LOCKEXCLUSIVE, TimeUnit.NANOSECONDS);
      details.set(OzoneProcessingDetails.Timing.LOCKFREE, deltaNanos, TimeUnit.NANOSECONDS);
      startNanos = Time.monotonicNowNanos();

      setResponseFields(value, responseParams);
      sendResponse();

      deltaNanos = Time.monotonicNowNanos() - startNanos;
      details.set(OzoneProcessingDetails.Timing.RESPONSE, deltaNanos, TimeUnit.NANOSECONDS);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deferring response for callId: " + this.callId);
      }
    }
    return null;
  }

  /**
   * @param t              the {@link java.lang.Throwable} to use to set
   *                       errorInfo
   * @param responseParams the {@link RpcCall.ResponseParams} instance to populate
   */
  private void populateResponseParamsOnError(Throwable t,
                                             RpcCall.ResponseParams responseParams) {
    if (t instanceof UndeclaredThrowableException) {
      t = t.getCause();
    }
    logException(LOG, t, this);
    if (t instanceof RpcServerException) {
      RpcServerException rse = ((RpcServerException) t);
      responseParams.returnStatus = rse.getRpcStatusProto();
      responseParams.detailedErr = rse.getRpcErrorCodeProto();
    } else {
      responseParams.returnStatus = RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.ERROR;
      responseParams.detailedErr = RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.ERROR_APPLICATION;
    }
    responseParams.errorClass = t.getClass().getName();
    responseParams.error = StringUtils.stringifyException(t);
    // Remove redundant error class name from the beginning of the
    // stack trace
    String exceptionHdr = responseParams.errorClass + ": ";
    if (responseParams.error.startsWith(exceptionHdr)) {
      responseParams.error =
          responseParams.error.substring(exceptionHdr.length());
    }
  }

  void setResponse(ByteBuffer response) throws IOException {
    this.rpcResponse = response;
  }

  @Override
  void doResponse(Throwable t, RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto status) throws IOException {
    RpcCall call = this;
    if (t != null) {
      if (status == null) {
        status = RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.FATAL;
      }
      // clone the call to prevent a race with another thread stomping
      // on the response while being sent.  the original call is
      // effectively discarded since the wait count won't hit zero
      call = new RpcCall(this);
      setupResponse(call, status, RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.ERROR_RPC_SERVER,
          null, t.getClass().getName(), StringUtils.stringifyException(t));
    } else {
      setupResponse(call, call.responseParams.returnStatus,
          call.responseParams.detailedErr, call.rv,
          call.responseParams.errorClass,
          call.responseParams.error);
    }
    connection.sendResponse(call);
  }

  /**
   * Send a deferred response, ignoring errors.
   */
  private void sendDeferedResponse() {
    try {
      connection.sendResponse(this);
    } catch (Exception e) {
      // For synchronous calls, application code is done once it's returned
      // from a method. It does not expect to receive an error.
      // This is equivalent to what happens in synchronous calls when the
      // Responder is not able to send out the response.
      LOG.error("Failed to send deferred response. ThreadName=" + Thread
          .currentThread().getName() + ", CallId="
          + callId + ", hostname=" + getHostAddress());
    }
  }

  @Override
  public void setDeferredResponse(Writable response) {
    if (this.connection.getServer().running) {
      try {
        setupResponse(this, RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.SUCCESS, null, response,
            null, null);
      } catch (IOException e) {
        // For synchronous calls, application code is done once it has
        // returned from a method. It does not expect to receive an error.
        // This is equivalent to what happens in synchronous calls when the
        // response cannot be sent.
        LOG.error(
            "Failed to setup deferred successful response. ThreadName=" +
                Thread.currentThread().getName() + ", Call=" + this);
        return;
      }
      sendDeferedResponse();
    }
  }

  @Override
  public void setDeferredError(Throwable t) {
    if (this.connection.getServer().running) {
      if (t == null) {
        t = new IOException(
            "User code indicated an error without an exception");
      }
      try {
        RpcCall.ResponseParams responseParams = new RpcCall.ResponseParams();
        populateResponseParamsOnError(t, responseParams);
        setupResponse(this, responseParams.returnStatus,
            responseParams.detailedErr,
            null, responseParams.errorClass, responseParams.error);
      } catch (IOException e) {
        // For synchronous calls, application code is done once it has
        // returned from a method. It does not expect to receive an error.
        // This is equivalent to what happens in synchronous calls when the
        // response cannot be sent.
        LOG.error(
            "Failed to setup deferred error response. ThreadName=" +
                Thread.currentThread().getName() + ", Call=" + this);
      }
      sendDeferedResponse();
    }
  }

  /**
   * Holds response parameters. Defaults set to work for successful
   * invocations
   */
  private class ResponseParams {
    String errorClass = null;
    String error = null;
    RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto detailedErr = null;
    RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto returnStatus = RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.SUCCESS;
  }

  @Override
  public String toString() {
    return super.toString() + " " + rpcRequest + " from " + connection;
  }
}
