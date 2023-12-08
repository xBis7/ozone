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
package org.apache.hadoop.ozone.scm.node;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Assertions;

import java.util.concurrent.TimeoutException;

/**
 * Utility class with helper methods for testing node state and status.
 */
public final class TestNodeUtil {

  private TestNodeUtil() {
  }

  public static void waitForDnToReachOpState(NodeManager nodeManager,
      DatanodeDetails dn, HddsProtos.NodeOperationalState state)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(
        () -> getNodeStatus(nodeManager, dn)
                  .getOperationalState().equals(state),
        200, 30000);
  }

  public static NodeStatus getNodeStatus(NodeManager nodeManager,
      DatanodeDetails dn) {
    return Assertions.assertDoesNotThrow(
        () -> nodeManager.getNodeStatus(dn),
        "Unexpected exception getting the nodeState");
  }

  public static String getDNHostAndPort(DatanodeDetails dn) {
    return dn.getHostName() + ":" + dn.getPorts().get(0).getValue();
  }

  public static void waitForDnToReachPersistedOpState(DatanodeDetails dn,
      HddsProtos.NodeOperationalState state)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(
        () -> dn.getPersistedOpState().equals(state),
        200, 30000);
  }
}
