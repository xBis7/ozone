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
package org.apache.hadoop.ozone.callQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.CostProvider;
import org.apache.hadoop.ipc.Schedulable;
import org.apache.hadoop.ipc.ProcessingDetails;
import org.apache.hadoop.ipc.WeightedTimeCostProvider;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.callQueue.metrics.OzoneDecayRpcSchedulerMetrics;
import org.apache.hadoop.ozone.callQueue.metrics.OzoneDecayRpcSchedulerMetricsProxy;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Timer;

import static org.mockito.Mockito.when;

/**
 * Tests for OzoneDecayRpcScheduler.
 */
public class TestOzoneDecayRpcScheduler {
  private Schedulable mockCall(String id) {
    Schedulable schedulable = Mockito.mock(Schedulable.class);
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(id);

    when(schedulable.getUserGroupInformation()).thenReturn(ugi);

    return schedulable;
  }

  private OzoneDecayRpcScheduler scheduler;

  @Test
  public void testNegativeScheduler() {
    Assertions.assertThrows(IllegalArgumentException.class,
        () -> new OzoneDecayRpcScheduler(-1, "", new Configuration()));
  }

  @Test
  public void testZeroScheduler() {
    Assertions.assertThrows(IllegalArgumentException.class,
        () -> new OzoneDecayRpcScheduler(0, "", new Configuration()));
  }

  @Test
  public void testParsePeriod() {
    // By default
    scheduler = new OzoneDecayRpcScheduler(1, "ipc.1", new Configuration());
    Assertions.assertEquals(OzoneDecayRpcScheduler
            .IPC_SCHEDULER_DECAYSCHEDULER_PERIOD_DEFAULT,
        scheduler.getDecayPeriodMillis());

    // Custom
    Configuration conf = new Configuration();
    conf.setLong("ipc.2." + OzoneDecayRpcScheduler
            .IPC_FCQ_DECAYSCHEDULER_PERIOD_KEY, 1058);
    scheduler = new OzoneDecayRpcScheduler(1, "ipc.2", conf);
    Assertions.assertEquals(1058L, scheduler.getDecayPeriodMillis());
  }

  @Test
  public void testParseFactorDefault() {
    // Default factor
    scheduler = new OzoneDecayRpcScheduler(1, "ipc.3", new Configuration());
    Timer timer = new Timer(true);
    OzoneDecayTask task = new OzoneDecayTask(scheduler, timer);

    Assertions.assertEquals(OzoneDecayTask
            .IPC_SCHEDULER_DECAYSCHEDULER_FACTOR_DEFAULT,
        task.getDecayFactor(), 0.00001);
  }

  @Test
  public void testParseFactorCustom() {
    // Custom factor
    Configuration conf = new Configuration();
    conf.set("ipc.4." + OzoneDecayTask.IPC_FCQ_DECAYSCHEDULER_FACTOR_KEY,
        "0.125");
    scheduler = new OzoneDecayRpcScheduler(1, "ipc.4", conf);
    Timer timer = new Timer(true);
    OzoneDecayTask task = new OzoneDecayTask(scheduler, timer);
    Assertions.assertEquals(0.125, task.getDecayFactor(), 0.00001);
  }

  public void assertEqualDecimalArrays(double[] a, double[] b) {
    Assertions.assertEquals(a.length, b.length);
    for (int i = 0; i < a.length; i++) {
      Assertions.assertEquals(a[i], b[i], 0.00001);
    }
  }

  @Test
  public void testParseThresholds() {
    // Defaults vary by number of queues
    Configuration conf = new Configuration();
    scheduler = new OzoneDecayRpcScheduler(1, "ipc.5", conf);
    assertEqualDecimalArrays(new double[]{}, scheduler.getThresholds());

    scheduler = new OzoneDecayRpcScheduler(2, "ipc.6", conf);
    assertEqualDecimalArrays(new double[]{0.5}, scheduler.getThresholds());

    scheduler = new OzoneDecayRpcScheduler(3, "ipc.7", conf);
    assertEqualDecimalArrays(new double[]{0.25, 0.5},
        scheduler.getThresholds());

    scheduler = new OzoneDecayRpcScheduler(4, "ipc.8", conf);
    assertEqualDecimalArrays(new double[]{0.125, 0.25, 0.5},
        scheduler.getThresholds());

    // Custom
    conf = new Configuration();
    conf.set("ipc.9." + OzoneDecayRpcScheduler
            .IPC_FCQ_DECAYSCHEDULER_THRESHOLDS_KEY, "1, 10, 20, 50, 85");
    scheduler = new OzoneDecayRpcScheduler(6, "ipc.9", conf);
    assertEqualDecimalArrays(new double[]{0.01, 0.1, 0.2, 0.5, 0.85},
        scheduler.getThresholds());
  }

  @Test
  public void testAccumulate() {
    Configuration conf = new Configuration();
    // Never flush
    conf.set("ipc.10." + OzoneDecayRpcScheduler
            .IPC_FCQ_DECAYSCHEDULER_PERIOD_KEY, "99999999");
    scheduler = new OzoneDecayRpcScheduler(1, "ipc.10", conf);

    // empty first
    Assertions.assertEquals(0,
        scheduler.getCallCostSnapshot().size());

    getPriorityIncrementCallCount("A");
    Assertions.assertEquals(1,
        scheduler.getCallCostSnapshot().get("A").longValue());
    Assertions.assertEquals(1,
        scheduler.getCallCostSnapshot().get("A").longValue());

    getPriorityIncrementCallCount("A");
    getPriorityIncrementCallCount("B");
    getPriorityIncrementCallCount("A");

    Assertions.assertEquals(3,
        scheduler.getCallCostSnapshot().get("A").longValue());
    Assertions.assertEquals(1,
        scheduler.getCallCostSnapshot().get("B").longValue());
  }

  @Test
  public void testDecay() throws Exception {
    Configuration conf = new Configuration();
    // Never decay
    conf.setLong("ipc.11." + OzoneDecayRpcScheduler
        .IPC_SCHEDULER_DECAYSCHEDULER_PERIOD_KEY, 999999999);
    conf.setDouble("ipc.11." + OzoneDecayTask
        .IPC_SCHEDULER_DECAYSCHEDULER_FACTOR_KEY, 0.5);
    scheduler = new OzoneDecayRpcScheduler(1, "ipc.11", conf);
    Timer timer = new Timer(true);
    OzoneDecayTask task = new OzoneDecayTask(scheduler, timer);

    Assertions.assertEquals(0, scheduler.getTotalCallSnapshot());

    for (int i = 0; i < 4; i++) {
      getPriorityIncrementCallCount("A");
    }

    Thread.sleep(1000L);

    for (int i = 0; i < 8; i++) {
      getPriorityIncrementCallCount("B");
    }

    Assertions.assertEquals(12, scheduler.getTotalCallSnapshot());
    Assertions.assertEquals(4,
        scheduler.getCallCostSnapshot().get("A").longValue());
    Assertions.assertEquals(8,
        scheduler.getCallCostSnapshot().get("B").longValue());

    task.forceDecay(scheduler);

    Assertions.assertEquals(6, scheduler.getTotalCallSnapshot());
    Assertions.assertEquals(2,
        scheduler.getCallCostSnapshot().get("A").longValue());
    Assertions.assertEquals(4,
        scheduler.getCallCostSnapshot().get("B").longValue());

    task.forceDecay(scheduler);

    Assertions.assertEquals(3, scheduler.getTotalCallSnapshot());
    Assertions.assertEquals(1,
        scheduler.getCallCostSnapshot().get("A").longValue());
    Assertions.assertEquals(2,
        scheduler.getCallCostSnapshot().get("B").longValue());

    task.forceDecay(scheduler);

    Assertions.assertEquals(1, scheduler.getTotalCallSnapshot());
    Assertions.assertEquals(1,
        scheduler.getCallCostSnapshot().get("B").longValue());
    Assertions.assertNull(scheduler.getCallCostSnapshot().get("A"));

    task.forceDecay(scheduler);

    Assertions.assertEquals(0, scheduler.getTotalCallSnapshot());
    Assertions.assertNull(scheduler.getCallCostSnapshot().get("A"));
    Assertions.assertNull(scheduler.getCallCostSnapshot().get("B"));
  }

  @Test
  public void testPriority() throws Exception {
    Configuration conf = new Configuration();
    final String namespace = "ipc.12";
    // Never flush
    conf.set(namespace + "." + OzoneDecayRpcScheduler
        .IPC_FCQ_DECAYSCHEDULER_PERIOD_KEY, "99999999");
    conf.set(namespace + "." + OzoneDecayRpcScheduler
        .IPC_FCQ_DECAYSCHEDULER_THRESHOLDS_KEY, "25, 50, 75");
    scheduler = new OzoneDecayRpcScheduler(4, namespace, conf);
    Timer timer = new Timer(true);
    OzoneDecayTask task = new OzoneDecayTask(scheduler, timer);

    // 0 out of 0 calls
    Assertions.assertEquals(0, getPriorityIncrementCallCount("A"));
    // 1 out of 1 calls
    Assertions.assertEquals(3, getPriorityIncrementCallCount("A"));
    // 0 out of 2 calls
    Assertions.assertEquals(0, getPriorityIncrementCallCount("B"));
    // 1 out of 3 calls
    Assertions.assertEquals(1, getPriorityIncrementCallCount("B"));
    // 0 out of 4 calls
    Assertions.assertEquals(0, getPriorityIncrementCallCount("C"));
    // 1 out of 5 calls
    Assertions.assertEquals(0, getPriorityIncrementCallCount("C"));
    // 2 out of 6 calls
    Assertions.assertEquals(1, getPriorityIncrementCallCount("A"));
    // 3 out of 7 calls
    Assertions.assertEquals(1, getPriorityIncrementCallCount("A"));
    // 4 out of 8 calls
    Assertions.assertEquals(2, getPriorityIncrementCallCount("A"));
    // 5 out of 9 calls
    Assertions.assertEquals(2, getPriorityIncrementCallCount("A"));

    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName mxBeanName = new ObjectName(
        "Hadoop:service=" + namespace + ",name=OzoneDecayRpcScheduler");

    String cvs1 = (String) mbs.getAttribute(mxBeanName, "CallVolumeSummary");
    Assertions.assertEquals(cvs1, "{\"A\":6,\"B\":2,\"C\":2}",
        "Get expected JMX of CallVolumeSummary before decay");

    task.forceDecay(scheduler);

    String cvs2 = (String) mbs.getAttribute(mxBeanName, "CallVolumeSummary");
    Assertions.assertEquals(cvs2, "{\"A\":3,\"B\":1,\"C\":1}",
        "Get expected JMX for CallVolumeSummary after decay");
  }

  @Test
  @Timeout(value = 2000)
  public void testPeriodic() throws InterruptedException {
    Configuration conf = new Configuration();
    conf.set(
        "ipc.13." + OzoneDecayRpcScheduler
            .IPC_FCQ_DECAYSCHEDULER_PERIOD_KEY, "10");
    conf.set(
        "ipc.13." + OzoneDecayTask
            .IPC_FCQ_DECAYSCHEDULER_FACTOR_KEY, "0.5");
    scheduler = new OzoneDecayRpcScheduler(1, "ipc.13", conf);

    Assertions.assertEquals(10, scheduler.getDecayPeriodMillis());
    Assertions.assertEquals(0, scheduler.getTotalCallSnapshot());

    for (int i = 0; i < 64; i++) {
      getPriorityIncrementCallCount("A");
    }

    // It should eventually decay to zero
    while (scheduler.getTotalCallSnapshot() > 0) {
      Thread.sleep(10L);
    }
  }

  @Test
  @Timeout(value = 60000)
  public void testNPEatInitialization() throws Exception {
    // Capture the LOG and check if there is NPE message
    // while initializing the DecayRpcScheduler
    try (GenericTestUtils.SystemOutCapturer capture =
             new GenericTestUtils.SystemOutCapturer()) {
      // Initializing DefaultMetricsSystem here would
      // set "monitoring" flag in MetricsSystemImpl to true
      DefaultMetricsSystem.initialize("NameNode");
      Configuration conf = new Configuration();
      scheduler = new OzoneDecayRpcScheduler(1, "ipc.14", conf);
      // Check if there is npe in log
      Assertions.assertFalse(capture.getOutput()
          .contains("NullPointerException"));
    }
  }

  @Test
  public void testUsingWeightedTimeCostProvider() {
    scheduler = getSchedulerWithWeightedTimeCostProvider(3, "ipc.15");
    OzoneDecayRpcSchedulerMetricsProxy metricsProxy =
        scheduler.getSchedulerMetricsProxy();
    OzoneDecayRpcSchedulerMetrics metrics =
        scheduler.getSchedulerMetrics();
    Timer timer = new Timer(true);
    OzoneDecayTask task = new OzoneDecayTask(scheduler, timer);

    // 3 details in increasing order of cost. Although medium has a longer
    // duration, the shared lock is weighted less than the exclusive lock
    ProcessingDetails callDetailsLow = Mockito.mock(ProcessingDetails.class);
    when(callDetailsLow.get(ProcessingDetails.Timing.LOCKFREE))
        .thenReturn(1L);

    ProcessingDetails callDetailsMedium = Mockito.mock(ProcessingDetails.class);
    when(callDetailsMedium.get(ProcessingDetails.Timing.LOCKSHARED))
        .thenReturn(500L);

    ProcessingDetails callDetailsHigh = Mockito.mock(ProcessingDetails.class);
    when(callDetailsHigh.get(ProcessingDetails.Timing.LOCKEXCLUSIVE))
        .thenReturn(100L);

    for (int i = 0; i < 10; i++) {
      scheduler.addResponseTime("ignored",
          mockCall("LOW"), callDetailsLow);
    }
    scheduler.addResponseTime("ignored",
        mockCall("MED"), callDetailsMedium);
    scheduler.addResponseTime("ignored",
        mockCall("HIGH"), callDetailsHigh);

    Assertions.assertEquals(0,
        scheduler.getPriorityLevel(mockCall("LOW")));
    Assertions.assertEquals(1,
        scheduler.getPriorityLevel(mockCall("MED")));
    Assertions.assertEquals(2,
        scheduler.getPriorityLevel(mockCall("HIGH")));

    Assertions.assertEquals(3, metricsProxy.getUniqueIdentityCount());
    long totalCallInitial = metrics.getTotalRawCallVolume();
    Assertions.assertEquals(totalCallInitial,
        metricsProxy.getTotalCallVolume());

    task.forceDecay(scheduler);

    // Relative priorities should stay the same after a single decay
    Assertions.assertEquals(0,
        scheduler.getPriorityLevel(mockCall("LOW")));
    Assertions.assertEquals(1,
        scheduler.getPriorityLevel(mockCall("MED")));
    Assertions.assertEquals(2,
        scheduler.getPriorityLevel(mockCall("HIGH")));

    Assertions.assertEquals(3, metricsProxy.getUniqueIdentityCount());
    Assertions.assertEquals(totalCallInitial, metrics.getTotalRawCallVolume());
    Assertions.assertTrue(metricsProxy.getTotalCallVolume() < totalCallInitial);

    for (int i = 0; i < 100; i++) {
      task.forceDecay(scheduler);
    }
    // After enough decay cycles, all callers should be high priority again
    Assertions.assertEquals(0,
        scheduler.getPriorityLevel(mockCall("LOW")));
    Assertions.assertEquals(0,
        scheduler.getPriorityLevel(mockCall("MED")));
    Assertions.assertEquals(0,
        scheduler.getPriorityLevel(mockCall("HIGH")));
  }

  @Test
  public void testUsingWeightedTimeCostProviderWithZeroCostCalls() {
    scheduler = getSchedulerWithWeightedTimeCostProvider(2, "ipc.16");

    ProcessingDetails emptyDetails = Mockito.mock(ProcessingDetails.class);

    for (int i = 0; i < 1000; i++) {
      scheduler.addResponseTime("ignored",
          mockCall("MANY"), emptyDetails);
    }
    scheduler.addResponseTime("ignored",
        mockCall("FEW"), emptyDetails);

    // Since the calls are all "free", they should have the same priority
    Assertions.assertEquals(0,
        scheduler.getPriorityLevel(mockCall("MANY")));
    Assertions.assertEquals(0,
        scheduler.getPriorityLevel(mockCall("FEW")));
  }

  @Test
  public void testUsingWeightedTimeCostProviderNoRequests() {
    scheduler = getSchedulerWithWeightedTimeCostProvider(2, "ipc.18");

    Assertions.assertEquals(0,
        scheduler.getPriorityLevel(mockCall("A")));
  }

  /**
   * Get a scheduler that uses {@link WeightedTimeCostProvider} and has
   * normal decaying disabled.
   */
  private static OzoneDecayRpcScheduler
      getSchedulerWithWeightedTimeCostProvider(int priorityLevels, String ns) {
    Configuration conf = new Configuration();
    conf.setClass(ns + "." + CommonConfigurationKeys.IPC_COST_PROVIDER_KEY,
        WeightedTimeCostProvider.class, CostProvider.class);
    conf.setLong(ns + "." + OzoneDecayRpcScheduler
        .IPC_SCHEDULER_DECAYSCHEDULER_PERIOD_KEY, 999999);
    return new OzoneDecayRpcScheduler(priorityLevels, ns, conf);
  }

  /**
   * Get the priority and increment the call count, assuming that
   * DefaultCostProvider is in use.
   */
  private int getPriorityIncrementCallCount(String callId) {
    Schedulable mockCall = mockCall(callId);
    int priority = scheduler.getPriorityLevel(mockCall);
    // The DefaultCostProvider uses a cost of 1 for all calls, ignoring
    // the processing details, so an empty one is fine
    ProcessingDetails emptyProcessingDetails =
        Mockito.mock(ProcessingDetails.class);
    scheduler.addResponseTime("ignored",
        mockCall, emptyProcessingDetails);
    return priority;
  }
}
