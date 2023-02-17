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
package org.apache.hadoop.ozone.om.callqueue;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.RpcScheduler;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Class for managing and adding all calls
 * made to OM to a FairCallQueue.
 */
public class CallHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(CallHandler.class);

  private static final RpcController NULL_RPC_CONTROLLER = null;
  private final ThreadLocal<OMRequest> omRequestThreadLocal = new ThreadLocal<>();
  private final ThreadLocal<OMResponse> omResponseThreadLocal = new ThreadLocal<>();
  private final OzoneManagerProtocolPB omTranslator;
  private final OzoneCallQueueManager<OMRequestCall> callQueue;
  private final Handler handler;
  private boolean requestOnTheQueue;

  public CallHandler(OzoneManagerProtocolPB omTranslator) {
    this.omTranslator = omTranslator;
    this.requestOnTheQueue = false;
    Class<? extends BlockingQueue<OMRequestCall>>
        queueClass = OzoneCallQueueManager
        .convertQueueClass(OzoneFairCallQueue.class, OMRequestCall.class);
    Class<? extends RpcScheduler> schedulerClass = OzoneCallQueueManager
        .convertSchedulerClass(OzoneDecayRpcScheduler.class);

    this.callQueue = new OzoneCallQueueManager<>(
        queueClass,
        schedulerClass,
        false, 100, "callqueue.9862", new Configuration());

    // Start the handler thread
    this.handler = new Handler();
    handler.start();
  }

  public OMResponse handleRequest(OMRequest omRequest)
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
    requestOnTheQueue = true;

    // Return OMResponse instead of Void
    RunnableFuture<Void> handlerTask = new FutureTask<>(handler, null);
    handlerTask.run();
    try {
      handlerTask.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    return Objects.nonNull(omResponseThreadLocal.get()) ?
        omResponseThreadLocal.get() : null;
  }

  public boolean reqIsBlocked(OMRequest omRequest) {
    if (Objects.isNull(omRequestThreadLocal.get())) {
      return true;
    } else {
      return !omRequestThreadLocal.get().equals(omRequest);
    }
  }

  public OMResponse submitRequestFromQueue()
      throws ServiceException {
    while (true) {
      if (Objects.nonNull(omRequestThreadLocal.get())) {
        return submitRequest();
      }
    }
  }

  private OMResponse submitRequest()
      throws ServiceException {
    OMRequest omRequest = omRequestThreadLocal.get();
    return omTranslator.submitRequest(NULL_RPC_CONTROLLER, omRequest);
  }

  private OMResponse submitRequest(OMRequest omRequest)
      throws ServiceException {
    return omTranslator.submitRequest(NULL_RPC_CONTROLLER, omRequest);
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

  private class Handler extends Thread {

    public Handler() {
      this.setDaemon(true);
    }

    @Override
    public void run() {
      while (requestOnTheQueue) {
        OMRequestCall omRequestCall;
        try {
          omRequestCall = callQueue.take();
          OMRequest requestFromQueue = omRequestCall.getOmRequest();

          // Maybe used for checking that we the response belongs to the correct request
          // and that we are not sending back a response from another request
          omRequestThreadLocal.set(requestFromQueue);

          OMResponse omResponse = submitRequest(requestFromQueue);
          omResponseThreadLocal.set(omResponse);
        } catch (InterruptedException ex) {
          LOG.error(Thread.currentThread().getName() + " unexpectedly interrupted", ex);
        } catch (ServiceException e) {
          throw new RuntimeException(e);
        } //finally {
//          omResponseThreadLocal.set(null);
//        }

//        synchronized (this) {
//          this.notify();
//        }

        if (callQueue.size() == 0) {
//          omResponseThreadLocal.set(null);
          requestOnTheQueue = false;
          return;
        }
      }


    }
  }
}
