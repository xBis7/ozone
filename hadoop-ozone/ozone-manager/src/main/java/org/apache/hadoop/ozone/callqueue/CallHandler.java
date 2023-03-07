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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.DecayRpcScheduler;
import org.apache.hadoop.ipc.ProcessingDetails;
import org.apache.hadoop.ipc.RpcScheduler;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Class for managing and adding all calls
 * made to OM to a FairCallQueue.
 */
public class CallHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(CallHandler.class);

  public static final ThreadLocal<UserGroupInformation> CURRENT_UGI =
      new ThreadLocal<>();

  private final OzoneCallQueueManager<OMQueueCall> callQueueManager;

  public CallHandler(Configuration conf) {
    String namespace = "ozone.om";
    Class<? extends BlockingQueue<OMQueueCall>>
        queueClass = getQueueClass(namespace, conf);
    Class<? extends RpcScheduler> schedulerClass = getSchedulerClass(namespace, conf);

    this.callQueueManager = new OzoneCallQueueManager<>(
        queueClass,
        schedulerClass,
        getClientBackoffEnable(namespace, conf), 10000, namespace, conf);

    // Start the thread that takes calls from the queue
    QueueHandler queueHandler = new QueueHandler();
    queueHandler.start();
  }

  static Class<? extends BlockingQueue<OMQueueCall>> getQueueClass(
      String prefix, Configuration conf) {
    String name = prefix + "." + CommonConfigurationKeys.IPC_CALLQUEUE_IMPL_KEY;
    Class<?> queueClass = conf.getClass(name, LinkedBlockingQueue.class);
    return OzoneCallQueueManager.convertQueueClass(queueClass, OMQueueCall.class);
  }

  static Class<? extends RpcScheduler> getSchedulerClass(
      String prefix, Configuration conf) {
    String schedulerKeyname = prefix + "." + CommonConfigurationKeys
        .IPC_SCHEDULER_IMPL_KEY;
    Class<?> schedulerClass = conf.getClass(schedulerKeyname,
        DecayRpcScheduler.class);
    return OzoneCallQueueManager.convertSchedulerClass(schedulerClass);
  }

  static boolean getClientBackoffEnable(
      String prefix, Configuration conf) {
    String name = prefix + "." +
        CommonConfigurationKeys.IPC_BACKOFF_ENABLE;
    return conf.getBoolean(name,
        CommonConfigurationKeys.IPC_BACKOFF_ENABLE_DEFAULT);
  }

  public void addRequestToQueue(OMQueueCall omRequestCall) {
    omRequestCall.setPriorityLevel(callQueueManager
        .getPriorityLevel(omRequestCall));

    try {
      internalQueueCall(omRequestCall);
    } catch (InterruptedException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void internalQueueCall(OMQueueCall omRequestCall)
      throws IOException, InterruptedException {
    internalQueueCall(omRequestCall, true);
  }

  private void internalQueueCall(OMQueueCall omQueueCall, boolean blocking)
      throws IOException, InterruptedException {
    try {
      // queue the call, may be blocked if blocking is true.
      if (blocking) {
        callQueueManager.put(omQueueCall);
      } else {
        callQueueManager.add(omQueueCall);
      }
      long deltaNanos = Time.monotonicNowNanos() -
          omQueueCall.getTimestampNanos();
      omQueueCall.getOzoneProcessingDetails()
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

  private void requeueCall(OMQueueCall omRequestCall)
      throws IOException, InterruptedException {
    try {
      internalQueueCall(omRequestCall, false);
    } catch (Exception ex) {
      LOG.error("Error while entering call back to queue: " + ex);
    }
  }
/*
  void updateMetrics(OMQueueCall omQueueCall, long startTime) {
    // delta = handler + processing + response
    long deltaNanos = Time.monotonicNowNanos() - startTime;
    long timestampNanos = omQueueCall.timestampNanos;

    ProcessingDetails details = omQueueCall.getProcessingDetails();
    // queue time is the delta between when the call first arrived and when it
    // began being serviced, minus the time it took to be put into the queue
    details.set(ProcessingDetails.Timing.QUEUE,
        startTime - timestampNanos - details.get(ProcessingDetails.Timing.ENQUEUE));
    deltaNanos -= details.get(ProcessingDetails.Timing.PROCESSING);
    deltaNanos -= details.get(ProcessingDetails.Timing.RESPONSE);
    details.set(ProcessingDetails.Timing.HANDLER, deltaNanos);

//    long queueTime = details.get(ProcessingDetails.Timing.QUEUE, rpcMetrics.getMetricsTimeUnit());
//    rpcMetrics.addRpcQueueTime(queueTime);

//    if (call.isResponseDeferred()) {
      // call was skipped; don't include it in processing metrics
//      return;
//    }

//    long processingTime =
//        details.get(ProcessingDetails.Timing.PROCESSING, rpcMetrics.getMetricsTimeUnit());
//    long waitTime =
//        details.get(ProcessingDetails.Timing.LOCKWAIT, rpcMetrics.getMetricsTimeUnit());
//    rpcMetrics.addRpcLockWaitTime(waitTime);
//    rpcMetrics.addRpcProcessingTime(processingTime);
    // don't include lock wait for detailed metrics.
//    processingTime -= waitTime;
    String name = omQueueCall.getDetailedMetricsName();
//    rpcDetailedMetrics.addProcessingTime(name, processingTime);
    callQueueManager.addResponseTime(name, omQueueCall, details);
//    if (isLogSlowRPC()) {
//      logSlowRpcCalls(name, call, details);
//    }
  }
*/
  /**
   * Background daemon thread to pick requests
   * from the queue and process them.
   */
  private class QueueHandler extends Thread {

    private final ExecutorService executorPoolService =
        Executors.newCachedThreadPool();

    QueueHandler() {
      this.setDaemon(true);
    }

    @Override
    public void run() {
      while (true) {
        long startTimeNanos = 0;
        OMQueueCall omQueueCall;
        try {
          omQueueCall = callQueueManager.take();
          startTimeNanos = Time.monotonicNowNanos();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        executorPoolService.execute(omQueueCall.getOmResponseFuture());

//        updateMetrics(omQueueCall, startTimeNanos);
      }
    }
  }
}
