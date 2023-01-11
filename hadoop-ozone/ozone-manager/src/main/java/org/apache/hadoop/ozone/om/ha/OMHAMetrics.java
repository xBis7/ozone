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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;

import java.util.List;

/**
 * Class to maintain metrics and info related to OM HA.
 */
@Metrics(about = "OzoneManager HA Metrics", context = OzoneConsts.OZONE)
public final class OMHAMetrics implements MetricsSource {

  private enum OMHAMetricsInfo implements MetricsInfo {

    OzoneManagerHALeaderState("Leader active state " +
        "of OzoneManager node (1 leader, 0 follower)"),
    CurrNodeHostName("OM hostname");

    private final String description;

    OMHAMetricsInfo(String description) {
      this.description = description;
    }

    @Override
    public String description() {
      return description;
    }
  }

  public static final String SOURCE_NAME =
      OMHAMetrics.class.getSimpleName();
  private MetricsRegistry metricsRegistry;

  private List<ServiceInfo> serviceInfoList;
  private String leaderId;

  public OMHAMetrics(List<ServiceInfo> serviceInfoList,
                     String leaderId) {
    this.serviceInfoList = serviceInfoList;
    this.leaderId = leaderId;
    this.metricsRegistry = new MetricsRegistry(SOURCE_NAME);
  }

  /**
   * Create and return OMHAMetrics instance.
   * @return OMHAMetrics
   */
  public static synchronized OMHAMetrics create(
      List<ServiceInfo> serviceInfoList, String leaderId) {
    OMHAMetrics metrics = new OMHAMetrics(serviceInfoList, leaderId);
    return DefaultMetricsSystem.instance()
        .register(SOURCE_NAME, "Metrics for OM HA", metrics);
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

    for (ServiceInfo info : serviceInfoList) {
      if (info.getNodeType()
          .equals(HddsProtos.NodeType.OM)) {

        if (info.getOmRoleInfo().getNodeId()
            .equals(leaderId)) {
          recordBuilder.endRecord().addRecord(SOURCE_NAME)
              .tag(OMHAMetricsInfo.CurrNodeHostName, info.getHostname())
              .addGauge(OMHAMetricsInfo.OzoneManagerHALeaderState, 1);
        } else {
          recordBuilder.endRecord().addRecord(SOURCE_NAME)
              .tag(OMHAMetricsInfo.CurrNodeHostName, info.getHostname())
              .addGauge(OMHAMetricsInfo.OzoneManagerHALeaderState, 0);
        }
        recordBuilder.endRecord();
      }
    }
  }
}
