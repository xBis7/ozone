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
package org.apache.hadoop.ozone.grpc.metrics;

import com.google.protobuf.AbstractMessage;
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interceptor to gather metrics based on grpc server request.
 */
public class GrpcMetricsServerRequestInterceptor implements ServerInterceptor {

  private static final Logger LOG =
      LoggerFactory.getLogger(
          GrpcMetricsServerRequestInterceptor.class);

  private final GrpcMetrics grpcMetrics;
  private long bytesReceived;
  private long receivedTime;
  private long startTime;
  private long endTime;

  public GrpcMetricsServerRequestInterceptor(
      GrpcMetrics grpcMetrics) {
    super();
    this.grpcMetrics = grpcMetrics;
    this.bytesReceived = 0;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> serverCall, Metadata headers,
      ServerCallHandler<ReqT, RespT> serverCallHandler) {

    // received time
    receivedTime = System.nanoTime();

    return new SimpleForwardingServerCallListener<ReqT>(
        serverCallHandler.startCall(serverCall, headers)) {

      @Override
      public void onMessage(ReqT message) {
        // start time
        startTime = System.nanoTime();

        long messageSize = 0;
        if (message instanceof AbstractMessage) {
          AbstractMessage parsedMessage = (AbstractMessage) message;
          messageSize += parsedMessage.getSerializedSize();
        } else {
          LOG.error("Unable to register number of bytes received. " +
              "Message is not an instance of AbstractMessage.");
        }

        bytesReceived += messageSize;

        if (bytesReceived > 0) {
          grpcMetrics.setReceivedBytes(bytesReceived);
        }
        super.onMessage(message);
      }

      @Override
      public void onComplete() {
        super.onComplete();
        // end time
        endTime = System.nanoTime();

        int queueTime = (int) (startTime - receivedTime);
        int processingTime = (int) (endTime - startTime);

        // set metrics queue time
        grpcMetrics.addGrpcQueueTime(queueTime);

        // set metrics processing time
        grpcMetrics.addGrpcProcessingTime(processingTime);
      }
    };
  }
}