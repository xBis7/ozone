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
package org.apache.hadoop.ozone.callqueue;

import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.Schedulable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;

import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Class for holding info related to
 * a request added in FairCallQueue.
 */
public class OMRequestCall implements Schedulable,
    PrivilegedExceptionAction<Void> {

  private final OzoneProcessingDetails processingDetails =
      new OzoneProcessingDetails(TimeUnit.NANOSECONDS);
  private final OMRequest omRequest;
  private Future<OMResponse> omResponseFuture;
  private CallerContext callerContext;

  private int priorityLevel;

  // time the call was received
  private long timestampNanos;

  // time the call was served
  private long responseTimestampNanos;

  public OMRequestCall(OMRequest omRequest,
                       Future<OMResponse> omResponseFuture) {
    this.omRequest = omRequest;
    this.omResponseFuture = omResponseFuture;
    this.timestampNanos = Time.monotonicNowNanos();
    this.responseTimestampNanos = timestampNanos;
  }

  @Override
  public Void run() throws Exception {
    return null;
  }

  @Override
  public UserGroupInformation getUserGroupInformation() {
    UserGroupInformation ugi;
    if (omRequest.hasS3Authentication()) {
      ugi = UserGroupInformation
          .createRemoteUser(omRequest.getS3Authentication().getAccessId());
    } else {
      // debug the request to check for username
      ugi = null;
    }
    return ugi;
  }

  public void setPriorityLevel(int priorityLevel) {
    this.priorityLevel = priorityLevel;
  }

  public void setOmResponseFuture(Future<OMResponse> omResponseFuture) {
    this.omResponseFuture = omResponseFuture;
  }

  @Override
  public int getPriorityLevel() {
    return priorityLevel;
  }

  public OzoneProcessingDetails getOzoneProcessingDetails() {
    return processingDetails;
  }

  public OMRequest getOmRequest() {
    return omRequest;
  }

  public Future<OMResponse> getOmResponseFuture() {
    return omResponseFuture;
  }

  public long getTimestampNanos() {
    return timestampNanos;
  }

  public void setTimestampNanos(long timestampNanos) {
    this.timestampNanos = timestampNanos;
  }

  public long getResponseTimestampNanos() {
    return responseTimestampNanos;
  }

  public void setResponseTimestampNanos(long responseTimestampNanos) {
    this.responseTimestampNanos = responseTimestampNanos;
  }
}
