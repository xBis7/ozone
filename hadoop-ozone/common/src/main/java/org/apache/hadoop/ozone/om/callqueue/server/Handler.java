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

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.ProcessingDetails;
import org.apache.hadoop.ipc.RpcServerException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tracing.TraceScope;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import java.io.IOException;

/** Handles queued calls . */
public class Handler extends Thread {
  public Handler(int instanceNumber) {
    this.setDaemon(true);
    this.setName("IPC Server handler "+ instanceNumber +
        " on default port " + port);
  }

  @Override
  public void run() {
    LOG.debug(Thread.currentThread().getName() + ": starting");
    SERVER.set(OzoneServer.this);
    while (running) {
      TraceScope traceScope = null;
      Call call = null;
      long startTimeNanos = 0;
      // True iff the connection for this call has been dropped.
      // Set to true by default and update to false later if the connection
      // can be succesfully read.
      boolean connDropped = true;

      try {
        call = callQueue.take(); // pop the queue; maybe blocked here
        startTimeNanos = Time.monotonicNowNanos();
        if (alignmentContext != null && call.isCallCoordinated() &&
            call.getClientStateId() > alignmentContext.getLastSeenStateId()) {
          /*
           * The call processing should be postponed until the client call's
           * state id is aligned (<=) with the server state id.

           * NOTE:
           * Inserting the call back to the queue can change the order of call
           * execution comparing to their original placement into the queue.
           * This is not a problem, because Hadoop RPC does not have any
           * constraints on ordering the incoming rpc requests.
           * In case of Observer, it handles only reads, which are
           * commutative.
           */
          // Re-queue the call and continue
          requeueCall(call);
          call = null;
          continue;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug(Thread.currentThread().getName() + ": " + call + " for RpcKind " + call.rpcKind);
        }
        CurCall.set(call);
        if (call.span != null) {
          traceScope = tracer.activateSpan(call.span);
          call.span.addTimelineAnnotation("called");
        }
        // always update the current call context
        CallerContext.setCurrent(call.callerContext);
        UserGroupInformation remoteUser = call.getRemoteUser();
        connDropped = !call.isOpen();
        if (remoteUser != null) {
          remoteUser.doAs(call);
        } else {
          call.run();
        }
      } catch (InterruptedException e) {
        if (running) {                          // unexpected -- log it
          LOG.info(Thread.currentThread().getName() + " unexpectedly interrupted", e);
          if (traceScope != null) {
            traceScope.addTimelineAnnotation("unexpectedly interrupted: " +
                StringUtils.stringifyException(e));
          }
        }
      } catch (Exception e) {
        LOG.info(Thread.currentThread().getName() + " caught an exception", e);
        if (traceScope != null) {
          traceScope.addTimelineAnnotation("Exception: " +
              StringUtils.stringifyException(e));
        }
      } finally {
        CurCall.set(null);
        IOUtils.cleanupWithLogger(LOG, traceScope);
        if (call != null) {
          updateMetrics(call, startTimeNanos, connDropped);
          ProcessingDetails.LOG.debug(
              "Served: [{}]{} name={} user={} details={}",
              call, (call.isResponseDeferred() ? ", deferred" : ""),
              call.getDetailedMetricsName(), call.getRemoteUser(),
              call.getProcessingDetails());
        }
      }
    }
    LOG.debug(Thread.currentThread().getName() + ": exiting");
  }

  private void requeueCall(Call call)
      throws IOException, InterruptedException {
    try {
      internalQueueCall(call, false);
    } catch (RpcServerException rse) {
      call.doResponse(rse.getCause(), rse.getRpcStatusProto());
    }
  }

}
