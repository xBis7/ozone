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

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.RpcScheduler;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Class for managing and adding all calls
 * made to OM to a FairCallQueue.
 */
public class CallHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(CallHandler.class);

  private static final ExecutorService executorService = Executors.newCachedThreadPool();
  private final OzoneCallQueueManager<OMRequestCall> callQueue;

  public CallHandler() {
    Class<? extends BlockingQueue<OMRequestCall>>
        queueClass = OzoneCallQueueManager
        .convertQueueClass(OzoneFairCallQueue.class, OMRequestCall.class);
    Class<? extends RpcScheduler> schedulerClass = OzoneCallQueueManager
        .convertSchedulerClass(OzoneDecayRpcScheduler.class);

    this.callQueue = new OzoneCallQueueManager<>(
        queueClass,
        schedulerClass,
        false, 100, "callqueue.9862", new Configuration());
  }

  public Future<OMRequest> handleRequest(OMRequest omRequest)
      throws ServiceException {
    CallerContext callerContext =
        new CallerContext.Builder(omRequest.getTraceID())
            .setSignature(omRequest.getTraceIDBytes().toByteArray())
            .build();
    OMRequestCall omRequestCall =
        new OMRequestCall(omRequest, callerContext);

    omRequestCall.setPriorityLevel(callQueue.getPriorityLevel(omRequestCall));

    try {
      internalQueueCall(omRequestCall);
    } catch (InterruptedException | IOException e) {
      throw new RuntimeException(e);
    }

    return executorService
        .submit(this::getRequestFromQueue);
  }

  private void internalQueueCall(OMRequestCall omRequestCall)
      throws IOException, InterruptedException {
    internalQueueCall(omRequestCall, true);
  }

  private void internalQueueCall(OMRequestCall omRequestCall, boolean blocking)
      throws IOException, InterruptedException {
    try {
      // queue the call, may be blocked if blocking is true.
      if (blocking) {
        callQueue.put(omRequestCall);
      } else {
        callQueue.add(omRequestCall);
      }
      long deltaNanos = Time.monotonicNowNanos() - omRequestCall.getTimestampNanos();
      omRequestCall.getOzoneProcessingDetails()
          .set(OzoneProcessingDetails.Timing.ENQUEUE,
              deltaNanos, TimeUnit.NANOSECONDS);
    } catch (OzoneCallQueueManager.CallQueueOverflowException cqe) {
      // If rpc scheduler indicates back off based on performance degradation
      // such as response time or rpc queue is full, we will ask the client
      // to back off by throwing RetriableException. Whether the client will
      // honor RetriableException and retry depends the client and its policy.
      // For example, IPC clients using FailoverOnNetworkExceptionRetry handle
      // RetriableException.
//      rpcMetrics.incrClientBackoff();
      // unwrap retriable exception.
      throw cqe.getCause();
    }
  }

  private void requeueCall(OMRequestCall omRequestCall)
      throws IOException, InterruptedException {
    try {
      internalQueueCall(omRequestCall, false);
    } catch (Exception ex) {
      LOG.error("Error while entering call back to queue: " + ex);
    }
  }

  public OMRequest getRequestFromQueue() {
    OMRequest requestFromQueue = null;
    try {
      OMRequestCall omRequestCall = callQueue.take();
      requestFromQueue = omRequestCall.getOmRequest();
    } catch (InterruptedException ex) {
      LOG.error(Thread.currentThread().getName() + " unexpectedly interrupted", ex);
    }
    return requestFromQueue;
  }
}
