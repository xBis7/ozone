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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcConstants;
import org.apache.hadoop.ipc.Schedulable;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.tracing.Span;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** A generic call queued for handling. */
public class Call implements Schedulable,
    PrivilegedExceptionAction<Void> {
  private final OzoneProcessingDetails processingDetails =
      new OzoneProcessingDetails(TimeUnit.NANOSECONDS);
  // the method name to use in metrics
  private volatile String detailedMetricsName = "";
  final int callId;            // the client's call id
  final int retryCount;        // the retry count of the call
  long timestampNanos;         // time the call was received
  long responseTimestampNanos; // time the call was served
  private AtomicInteger responseWaitCount = new AtomicInteger(1);
  final RPC.RpcKind rpcKind;
  final byte[] clientId;
  private final Span span; // the trace span on the server side
  private final CallerContext callerContext; // the call context
  private boolean deferredResponse = false;
  private int priorityLevel;
  // the priority level assigned by scheduler, 0 by default
  private long clientStateId;
  private boolean isCallCoordinated;

  Call() {
    this(RpcConstants.INVALID_CALL_ID, RpcConstants.INVALID_RETRY_COUNT,
        RPC.RpcKind.RPC_BUILTIN, RpcConstants.DUMMY_CLIENT_ID);
  }

  Call(Call call) {
    this(call.callId, call.retryCount, call.rpcKind, call.clientId,
        call.span, call.callerContext);
  }

  Call(int id, int retryCount, RPC.RpcKind kind, byte[] clientId) {
    this(id, retryCount, kind, clientId, null, null);
  }

  @VisibleForTesting // primarily TestNamenodeRetryCache
  public Call(int id, int retryCount, Void ignore1, Void ignore2,
              RPC.RpcKind kind, byte[] clientId) {
    this(id, retryCount, kind, clientId, null, null);
  }

  Call(int id, int retryCount, RPC.RpcKind kind, byte[] clientId,
       Span span, CallerContext callerContext) {
    this.callId = id;
    this.retryCount = retryCount;
    this.timestampNanos = Time.monotonicNowNanos();
    this.responseTimestampNanos = timestampNanos;
    this.rpcKind = kind;
    this.clientId = clientId;
    this.span = span;
    this.callerContext = callerContext;
    this.clientStateId = Long.MIN_VALUE;
    this.isCallCoordinated = false;
  }

  /**
   * Indicates whether the call has been processed. Always true unless
   * overridden.
   *
   * @return true
   */
  boolean isOpen() {
    return true;
  }

  String getDetailedMetricsName() {
    return detailedMetricsName;
  }

  void setDetailedMetricsName(String name) {
    detailedMetricsName = name;
  }

  public OzoneProcessingDetails getProcessingDetails() {
    return processingDetails;
  }

  @Override
  public String toString() {
    return "Call#" + callId + " Retry#" + retryCount;
  }

  @Override
  public Void run() throws Exception {
    return null;
  }
  // should eventually be abstract but need to avoid breaking tests
  public UserGroupInformation getRemoteUser() {
    return null;
  }
  public InetAddress getHostInetAddress() {
    return null;
  }
  public String getHostAddress() {
    InetAddress addr = getHostInetAddress();
    return (addr != null) ? addr.getHostAddress() : null;
  }

  public String getProtocol() {
    return null;
  }

  /**
   * Allow a IPC response to be postponed instead of sent immediately
   * after the handler returns from the proxy method.  The intended use
   * case is freeing up the handler thread when the response is known,
   * but an expensive pre-condition must be satisfied before it's sent
   * to the client.
   */
  @InterfaceStability.Unstable
  @InterfaceAudience.LimitedPrivate({"HDFS"})
  public final void postponeResponse() {
    int count = responseWaitCount.incrementAndGet();
    assert count > 0 : "response has already been sent";
  }

  @InterfaceStability.Unstable
  @InterfaceAudience.LimitedPrivate({"HDFS"})
  public final void sendResponse() throws IOException {
    int count = responseWaitCount.decrementAndGet();
    assert count >= 0 : "response has already been sent";
    if (count == 0) {
      doResponse(null);
    }
  }

  @InterfaceStability.Unstable
  @InterfaceAudience.LimitedPrivate({"HDFS"})
  public final void abortResponse(Throwable t) throws IOException {
    // don't send response if the call was already sent or aborted.
    if (responseWaitCount.getAndSet(-1) > 0) {
      doResponse(t);
    }
  }

  void doResponse(Throwable t) throws IOException {
    doResponse(t, RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.FATAL);
  }

  void doResponse(Throwable t, RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto proto) throws IOException {}

  // For Schedulable
  @Override
  public UserGroupInformation getUserGroupInformation() {
    return getRemoteUser();
  }

  @Override
  public int getPriorityLevel() {
    return this.priorityLevel;
  }

  public void setPriorityLevel(int priorityLevel) {
    this.priorityLevel = priorityLevel;
  }

  public long getClientStateId() {
    return this.clientStateId;
  }

  public void setClientStateId(long stateId) {
    this.clientStateId = stateId;
  }

  public void markCallCoordinated(boolean flag) {
    this.isCallCoordinated = flag;
  }

  public boolean isCallCoordinated() {
    return this.isCallCoordinated;
  }

  @InterfaceStability.Unstable
  public void deferResponse() {
    this.deferredResponse = true;
  }

  @InterfaceStability.Unstable
  public boolean isResponseDeferred() {
    return this.deferredResponse;
  }

  public void setDeferredResponse(Writable response) {
  }

  public void setDeferredError(Throwable t) {
  }
}
