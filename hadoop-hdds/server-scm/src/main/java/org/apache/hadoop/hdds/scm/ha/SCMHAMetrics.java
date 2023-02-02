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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.ha;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Class to maintain metrics and info related to SCM HA.
 */
@Metrics(about = "StorageContainerManager HA Metrics",
    context = OzoneConsts.OZONE)
public final class SCMHAMetrics implements MetricsSource {

  /**
   * Private nested class to hold the values
   * of MetricsInfo for SCMHAMetrics.
   */
  private static final class SCMHAMetricsInfo {

    private static final MetricsInfo STORAGE_CONTAINER_MANAGER_HA_LEADER_STATE =
        Interns.info("StorageContainerManagerHALeaderState",
            "Leader active state of StorageContainerManager " +
                "node (1 leader, 0 follower)");

    private static final MetricsInfo NODE_ID =
        Interns.info("NodeId", "SCM node Id");

    private long storageContainerManagerHALeaderState;
    private String nodeId;

    SCMHAMetricsInfo() {
      this.storageContainerManagerHALeaderState = 0L;
      this.nodeId = "";
    }

    public long getStorageContainerManagerHALeaderState() {
      return storageContainerManagerHALeaderState;
    }

    public void setStorageContainerManagerHALeaderState(
        long storageContainerManagerHALeaderState) {
      this.storageContainerManagerHALeaderState =
          storageContainerManagerHALeaderState;
    }

    public String getNodeId() {
      return nodeId;
    }

    public void setNodeId(String nodeId) {
      this.nodeId = nodeId;
    }
  }

  public static final String SOURCE_NAME =
      SCMHAMetrics.class.getSimpleName();
  private final SCMHAMetricsInfo scmhaMetricsInfo = new SCMHAMetricsInfo();
  private MetricsRegistry metricsRegistry;

  private String currNodeId;
  private String leaderId;

  private SCMHAMetrics(String currNodeId, String leaderId) {
    this.currNodeId = currNodeId;
    this.leaderId = leaderId;
    this.metricsRegistry = new MetricsRegistry(SOURCE_NAME);
  }

  /**
   * Create and return SCMHAMetrics instance.
   * @return SCMHAMetrics
   */
  public static SCMHAMetrics create(
      String nodeId, String leaderId) {
    SCMHAMetrics metrics = new SCMHAMetrics(nodeId, leaderId);
    return DefaultMetricsSystem.instance()
        .register(SOURCE_NAME, "Metrics for SCM HA", metrics);
  }

  /**
   * Unregister the metrics instance.
   */
  public static void unRegister() {
    DefaultMetricsSystem.instance().unregisterSource(SOURCE_NAME);
  }

  @Override
  public synchronized void getMetrics(MetricsCollector collector, boolean all) {

    MetricsRecordBuilder recordBuilder = collector.addRecord(SOURCE_NAME);

    // Check current node state (1 leader, 0 follower)
    int state = currNodeId.equals(leaderId) ? 1 : 0;
    scmhaMetricsInfo.setNodeId(currNodeId);
    scmhaMetricsInfo.setStorageContainerManagerHALeaderState(state);

    recordBuilder
        .tag(SCMHAMetricsInfo.NODE_ID, currNodeId)
        .addGauge(SCMHAMetricsInfo
            .STORAGE_CONTAINER_MANAGER_HA_LEADER_STATE, state);

    recordBuilder.endRecord();
  }

  @VisibleForTesting
  public String getScmhaInfoNodeId() {
    return scmhaMetricsInfo.getNodeId();
  }

  @VisibleForTesting
  public long getScmhaInfoOzoneManagerHALeaderState() {
    return scmhaMetricsInfo.getStorageContainerManagerHALeaderState();
  }
}
