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
package org.apache.hadoop.ozone.om.callqueue.grpc;

import io.grpc.Attributes;
import io.grpc.Grpc;
import io.grpc.ServerTransportFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;

/**
 * Transport filter class for tracking active client connections.
 */
public class GrpcServerTransportFilter extends ServerTransportFilter {

  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcServerTransportFilter.class);
  public GrpcServerTransportFilter() {
    super();
  }

  @Override
  public Attributes transportReady(Attributes transportAttrs) {
    // add to the FairCallQueue
    SocketAddress clientAddress = transportAttrs.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
    SocketAddress omAddress = transportAttrs.get(Grpc.TRANSPORT_ATTR_LOCAL_ADDR);

    LOG.info("xbis77: " + omAddress);
    return super.transportReady(transportAttrs);
  }

  @Override
  public void transportTerminated(Attributes transportAttrs) {
    // remove from the FairCallQueue
    super.transportTerminated(transportAttrs);
  }
}
