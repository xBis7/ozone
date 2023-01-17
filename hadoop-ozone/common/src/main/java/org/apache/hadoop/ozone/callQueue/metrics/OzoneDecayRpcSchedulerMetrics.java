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
package org.apache.hadoop.ozone.callQueue.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.hadoop.ipc.DecayRpcSchedulerMXBean;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.util.Metrics2Util;
import org.apache.hadoop.ozone.callQueue.OzoneDecayRpcScheduler;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.AtomicDoubleArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * OzoneDecayRpcScheduler metrics class.
 */
public class OzoneDecayRpcSchedulerMetrics
    implements DecayRpcSchedulerMXBean, MetricsSource {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneDecayRpcSchedulerMetrics.class);
  private static final ObjectWriter WRITER = new ObjectMapper().writer();

  // Track total call count and response time in current decay window
  private final AtomicLongArray responseTimeCountInCurrWindow;
  private final AtomicLongArray responseTimeTotalInCurrWindow;

  // Track average response time in previous decay window
  private final AtomicDoubleArray responseTimeAvgInLastWindow;
  private final AtomicLongArray responseTimeCountInLastWindow;
  private final OzoneDecayRpcScheduler scheduler;

  public OzoneDecayRpcSchedulerMetrics(OzoneDecayRpcScheduler scheduler) {
    this.scheduler = scheduler;
    int numLevels = scheduler.getNumLevels();

    // Setup response time metrics
    responseTimeTotalInCurrWindow = new AtomicLongArray(numLevels);
    responseTimeCountInCurrWindow = new AtomicLongArray(numLevels);
    responseTimeAvgInLastWindow = new AtomicDoubleArray(numLevels);
    responseTimeCountInLastWindow = new AtomicLongArray(numLevels);
  }

  @Override
  public double[] getAverageResponseTime() {
    double[] ret = new double[responseTimeAvgInLastWindow.length()];
    for (int i = 0; i < responseTimeAvgInLastWindow.length(); i++) {
      ret[i] = responseTimeAvgInLastWindow.get(i);
    }
    return ret;
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    String namespace = scheduler.getNamespace();

    // Metrics2 interface to act as a Metric source
    try {
      MetricsRecordBuilder rb = collector.addRecord(getClass().getName())
          .setContext(namespace);
      addDecayedCallVolume(rb);
      addUniqueIdentityCount(rb);
      addTopNCallerSummary(rb);
      addAvgResponseTimePerPriority(rb);
      addCallVolumePerPriority(rb);
      addRawCallVolume(rb);
    } catch (Exception e) {
      LOG.warn("Exception thrown while metric collection. Exception : "
          + e.getMessage());
    }
  }

  // Key: UniqueCallers
  private void addUniqueIdentityCount(MetricsRecordBuilder rb) {
    rb.addCounter(Interns.info("UniqueCallers", "Total unique callers"),
        getUniqueIdentityCount());
  }

  // Key: DecayedCallVolume
  private void addDecayedCallVolume(MetricsRecordBuilder rb) {
    rb.addCounter(Interns.info("DecayedCallVolume", "Decayed Total " +
        "incoming Call Volume"), getTotalCallVolume());
  }

  private void addRawCallVolume(MetricsRecordBuilder rb) {
    rb.addCounter(Interns.info("CallVolume", "Raw Total " +
        "incoming Call Volume"), getTotalRawCallVolume());
  }

  // Key: Priority.0.CompletedCallVolume
  private void addCallVolumePerPriority(MetricsRecordBuilder rb) {
    for (int i = 0; i < responseTimeCountInLastWindow.length(); i++) {
      rb.addGauge(Interns.info("Priority." + i + ".CompletedCallVolume",
          "Completed Call volume " +
              "of priority " + i), responseTimeCountInLastWindow.get(i));
    }
  }

  // Key: Priority.0.AvgResponseTime
  private void addAvgResponseTimePerPriority(MetricsRecordBuilder rb) {
    for (int i = 0; i < responseTimeAvgInLastWindow.length(); i++) {
      rb.addGauge(Interns.info("Priority." + i + ".AvgResponseTime", "Average" +
              " response time of priority " + i),
          responseTimeAvgInLastWindow.get(i));
    }
  }

  // Key: Caller(xyz).Volume and Caller(xyz).Priority
  private void addTopNCallerSummary(MetricsRecordBuilder rb) {
    Metrics2Util.TopN topNCallers =
        getTopCallers(scheduler.getTopUsersCount());
    Map<Object, Integer> decisions = scheduler.getScheduleCacheRef().get();
    final int actualCallerCount = topNCallers.size();
    for (int i = 0; i < actualCallerCount; i++) {
      Metrics2Util.NameValuePair entry =  topNCallers.poll();
      String topCaller = "Caller(" + entry.getName() + ")";
      String topCallerVolume = topCaller + ".Volume";
      String topCallerPriority = topCaller + ".Priority";
      rb.addCounter(Interns.info(topCallerVolume, topCallerVolume),
          entry.getValue());
      Integer priority = decisions.get(entry.getName());
      if (priority != null) {
        rb.addCounter(Interns.info(topCallerPriority, topCallerPriority),
            priority);
      }
    }
  }

  // Get the top N callers' raw call cost and scheduler decision
  private Metrics2Util.TopN getTopCallers(int n) {
    Metrics2Util.TopN topNCallers = new Metrics2Util.TopN(n);
    Iterator<Map.Entry<Object, List<AtomicLong>>> it =
        scheduler.getCallCosts().entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Object, List<AtomicLong>> entry = it.next();
      String caller = entry.getKey().toString();
      Long cost = entry.getValue().get(1).get();
      if (cost > 0) {
        topNCallers.offer(new Metrics2Util.NameValuePair(caller, cost));
      }
    }
    return topNCallers;
  }

  public String getSchedulingDecisionSummary() {
    Map<Object, Integer> decisions =
        scheduler.getScheduleCacheRef().get();
    if (decisions == null) {
      return "{}";
    } else {
      try {
        return WRITER.writeValueAsString(decisions);
      } catch (Exception e) {
        return "Error: " + e.getMessage();
      }
    }
  }

  public String getCallVolumeSummary() {
    try {
      return WRITER.writeValueAsString(getDecayedCallCosts());
    } catch (Exception e) {
      return "Error: " + e.getMessage();
    }
  }

  private Map<Object, Long> getDecayedCallCosts() {
    Map<Object, Long> decayedCallCosts =
        new HashMap<>(scheduler.getCallCosts().size());
    Iterator<Map.Entry<Object, List<AtomicLong>>> it =
        scheduler.getCallCosts().entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Object, List<AtomicLong>> entry = it.next();
      Object user = entry.getKey();
      long decayedCost = entry.getValue().get(0).get();
      if (decayedCost > 0) {
        decayedCallCosts.put(user, decayedCost);
      }
    }
    return decayedCallCosts;
  }

  public int getUniqueIdentityCount() {
    return scheduler.getCallCosts().size();
  }

  public long getTotalCallVolume() {
    return scheduler.getTotalDecayedCallCost().get();
  }

  public long getTotalRawCallVolume() {
    return scheduler.getTotalRawCallCost().get();
  }

  public AtomicLongArray getResponseTimeCountInCurrWindow() {
    return responseTimeCountInCurrWindow;
  }

  public AtomicLongArray getResponseTimeTotalInCurrWindow() {
    return responseTimeTotalInCurrWindow;
  }

  public AtomicDoubleArray getResponseTimeAvgInLastWindow() {
    return responseTimeAvgInLastWindow;
  }

  @Override
  public long[] getResponseTimeCountInLastWindow() {
    long[] ret = new long[responseTimeCountInLastWindow.length()];
    for (int i = 0; i < responseTimeCountInLastWindow.length(); i++) {
      ret[i] = responseTimeCountInLastWindow.get(i);
    }
    return ret;
  }

  public AtomicLongArray getResponseTimeCountInLastWindowAtomic() {
    return responseTimeCountInLastWindow;
  }
}
