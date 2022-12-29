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
package org.apache.hadoop.ozone.om.ha;

import com.google.gson.Gson;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;

import java.util.LinkedList;
import java.util.List;

/**
 * Class to maintain metrics and info related to OM HA.
 */
@Metrics(about = "OM HA Metrics", context = OzoneConsts.OZONE)
public class OMHAMetrics {

  public static final String SOURCE_NAME =
      OMHAMetrics.class.getSimpleName();
  private final MetricsRegistry metricsRegistry;

  public OMHAMetrics(List<ServiceInfo> serviceList,
                     int portNum,
                     String leaderId) {
    List<String> omInfoList = new LinkedList<>();

    for (ServiceInfo serviceInfo : serviceList) {
      StringBuilder builder = new StringBuilder();
      if (serviceInfo.getNodeType().equals(HddsProtos.NodeType.OM)) {
        String nodeId = serviceInfo.getOmRoleInfo().getNodeId();

        builder.append("{ Hostname : ")
            .append(serviceInfo.getHostname())
            .append(", HostId : ")
            .append(nodeId);

        if (nodeId.equals(leaderId)) {
          builder.append(", Role : LEADER }");
        } else {
          builder.append(", Role : FOLLOWER }");
        }
        String entry = builder.toString();
        if (!omInfoList.contains(entry)) {
          omInfoList.add(entry);
        }
      }
    }

    Gson gson = new Gson();
    String port = String.valueOf(portNum);
    this.metricsRegistry = new MetricsRegistry(SOURCE_NAME)
        .tag("port", "Ratis port", port)
        .tag("OMRoles", "OM roles", gson.toJson(omInfoList));
  }

  /**
   * Create and return OMHAMetrics instance.
   * @return OMHAMetrics
   */
  public static synchronized OMHAMetrics create(
      List<ServiceInfo> nodes, int port, String leaderId) {
    OMHAMetrics metrics = new OMHAMetrics(nodes, port, leaderId);
    return DefaultMetricsSystem.instance().register(SOURCE_NAME,
        "Metrics for OM HA", metrics);
  }

  /**
   * Unregister the metrics instance.
   */
  public static void unRegister() {
    DefaultMetricsSystem.instance().unregisterSource(SOURCE_NAME);
  }

  @Metric("Number of OM nodes")
  private MutableGaugeInt numOfOMNodes;

  public void setNumOfOMNodes(int count) {
    numOfOMNodes.set(count);
  }

  public MutableGaugeInt getNumOfOMNodes() {
    return numOfOMNodes;
  }
}
