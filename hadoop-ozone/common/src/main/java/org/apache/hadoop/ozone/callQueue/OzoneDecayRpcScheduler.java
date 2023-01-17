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
package org.apache.hadoop.ozone.callQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.Schedulable;
import org.apache.hadoop.ipc.CostProvider;
import org.apache.hadoop.ipc.DefaultCostProvider;
import org.apache.hadoop.ipc.metrics.DecayRpcSchedulerDetailedMetrics;
import org.apache.hadoop.ipc.metrics.RpcMetrics;
import org.apache.hadoop.ipc.ProcessingDetails;
import org.apache.hadoop.ipc.RpcScheduler;
import org.apache.hadoop.ozone.callQueue.metrics.OzoneDecayRpcSchedulerMetrics;
import org.apache.hadoop.ozone.callQueue.metrics.OzoneDecayRpcSchedulerMetricsProxy;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.Timer;
import java.util.Collections;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Ozone implementation of Hadoop DecayRpcScheduler.
 */
public class OzoneDecayRpcScheduler implements RpcScheduler {
  /**
   * Period controls how many milliseconds between each decay sweep.
   */
  public static final String IPC_SCHEDULER_DECAYSCHEDULER_PERIOD_KEY =
      "decay-scheduler.period-ms";
  public static final long IPC_SCHEDULER_DECAYSCHEDULER_PERIOD_DEFAULT =
      5000L;
  public static final String IPC_FCQ_DECAYSCHEDULER_PERIOD_KEY =
      "faircallqueue.decay-scheduler.period-ms";

  /**
   * Thresholds are specified as integer percentages, and specify which usage
   * range each queue will be allocated to. For instance, specifying the list
   *  10, 40, 80
   * implies 4 queues, with
   * - q3 from 80% up
   * - q2 from 40 up to 80
   * - q1 from 10 up to 40
   * - q0 otherwise.
   */
  public static final String IPC_DECAYSCHEDULER_THRESHOLDS_KEY =
      "decay-scheduler.thresholds";

  public static final String IPC_FCQ_DECAYSCHEDULER_THRESHOLDS_KEY =
      "faircallqueue.decay-scheduler.thresholds";

  // Specifies the identity to use when the IdentityProvider cannot handle
  // a schedulable.
  public static final String DECAYSCHEDULER_UNKNOWN_IDENTITY =
      "IdentityProvider.Unknown";

  public static final String
      IPC_DECAYSCHEDULER_BACKOFF_RESPONSETIME_ENABLE_KEY =
      "decay-scheduler.backoff.responsetime.enable";
  public static final Boolean
      IPC_DECAYSCHEDULER_BACKOFF_RESPONSETIME_ENABLE_DEFAULT = false;

  // Specifies the average response time (ms) thresholds of each
  // level to trigger backoff
  public static final String
      IPC_DECAYSCHEDULER_BACKOFF_RESPONSETIME_THRESHOLDS_KEY =
      "decay-scheduler.backoff.responsetime.thresholds";

  // Specifies the top N user's call count and scheduler decision
  // Metrics2 Source
  public static final String DECAYSCHEDULER_METRICS_TOP_USER_COUNT =
      "decay-scheduler.metrics.top.user.count";
  public static final int DECAYSCHEDULER_METRICS_TOP_USER_COUNT_DEFAULT = 10;

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneDecayRpcScheduler.class);

  // Track the decayed and raw (no decay) number of calls for each schedulable
  // identity from all previous decay windows: idx 0 for decayed call cost and
  // idx 1 for the raw call cost
  private final ConcurrentHashMap<Object, List<AtomicLong>> callCosts =
      new ConcurrentHashMap<>();

  // Should be the sum of all AtomicLongs in decayed callCosts
  private final AtomicLong totalDecayedCallCost = new AtomicLong();
  // The sum of all AtomicLongs in raw callCosts
  private final AtomicLong totalRawCallCost = new AtomicLong();

  private final OzoneDecayRpcSchedulerMetrics metrics;

  // RPC queue time rates per queue
  private final DecayRpcSchedulerDetailedMetrics
      decayRpcSchedulerDetailedMetrics;

  // Pre-computed scheduling decisions during the decay sweep are
  // atomically swapped in as a read-only map
  private final AtomicReference<Map<Object, Integer>> scheduleCacheRef =
      new AtomicReference<>();

  // Tune the behavior of the scheduler
  private final long decayPeriodMillis; // How long between each tick
  private final int numLevels;
  private final double[] thresholds;
  private final OzoneIdentityProvider identityProvider;
  private final boolean backOffByResponseTimeEnabled;
  private final long[] backOffResponseTimeThresholds;
  private final String namespace;
  private final Configuration configuration;
  private final int topUsersCount; // e.g., report top 10 users' metrics
  private final TimeUnit metricsTimeUnit;
  private final OzoneDecayRpcSchedulerMetricsProxy metricsProxy;
  private final CostProvider costProvider;

  private final Map<String, Integer> staticPriorities = new HashMap<>();

  /**
   * Create a decay scheduler.
   * @param numLevels number of priority levels
   * @param ns config prefix, so that we can configure multiple schedulers
   *           in a single instance.
   * @param conf configuration to use.
   */
  public OzoneDecayRpcScheduler(int numLevels, String ns, Configuration conf) {
    if (numLevels < 1) {
      throw new IllegalArgumentException("Number of Priority Levels must be " +
          "at least 1");
    }
    this.numLevels = numLevels;
    this.namespace = ns;
    this.configuration = conf;
    this.decayPeriodMillis = parseDecayPeriodMillis(ns, conf);
    this.identityProvider = new OzoneIdentityProvider();
    this.costProvider = this.parseCostProvider(ns, conf);
    this.thresholds = parseThresholds(ns, conf, numLevels);
    this.backOffByResponseTimeEnabled = parseBackOffByResponseTimeEnabled(ns,
        conf);
    this.backOffResponseTimeThresholds =
        parseBackOffResponseTimeThreshold(ns, conf, numLevels);

    this.metrics =
        new OzoneDecayRpcSchedulerMetrics(this);

    topUsersCount =
        conf.getInt(DECAYSCHEDULER_METRICS_TOP_USER_COUNT,
            DECAYSCHEDULER_METRICS_TOP_USER_COUNT_DEFAULT);
    Preconditions.checkArgument(topUsersCount > 0,
        "the number of top users for scheduler metrics must be at least 1");

    decayRpcSchedulerDetailedMetrics =
        DecayRpcSchedulerDetailedMetrics.create(ns);
    decayRpcSchedulerDetailedMetrics.init(numLevels);

    metricsTimeUnit = RpcMetrics.getMetricsTimeUnit(conf);

    // Setup delay timer
    Timer timer = new Timer(true);
    OzoneDecayTask task =
        new OzoneDecayTask(this, timer);
    timer.scheduleAtFixedRate(task, decayPeriodMillis, decayPeriodMillis);

    metricsProxy = OzoneDecayRpcSchedulerMetricsProxy
        .getInstance(ns, numLevels, this);
    recomputeScheduleCache();
  }

  private CostProvider parseCostProvider(String ns, Configuration conf) {
    List<CostProvider> providers = conf.getInstances(
        ns + "." + CommonConfigurationKeys.IPC_COST_PROVIDER_KEY,
        CostProvider.class);

    if (providers.size() < 1) {
      LOG.info("CostProvider not specified, defaulting to DefaultCostProvider");
      return new DefaultCostProvider();
    } else if (providers.size() > 1) {
      LOG.warn("Found multiple CostProviders; using: {}",
          providers.get(0).getClass());
    }

    CostProvider provider = providers.get(0); // use the first
    provider.init(ns, conf);
    return provider;
  }

  private static long parseDecayPeriodMillis(String ns, Configuration conf) {
    long period = conf.getLong(ns + "." +
            IPC_FCQ_DECAYSCHEDULER_PERIOD_KEY,
        0);
    if (period == 0) {
      period = conf.getLong(ns + "." +
              IPC_SCHEDULER_DECAYSCHEDULER_PERIOD_KEY,
          IPC_SCHEDULER_DECAYSCHEDULER_PERIOD_DEFAULT);
    } else if (period > 0) {
      LOG.warn((IPC_FCQ_DECAYSCHEDULER_PERIOD_KEY +
          " is deprecated. Please use " +
          IPC_SCHEDULER_DECAYSCHEDULER_PERIOD_KEY));
    }
    if (period <= 0) {
      throw new IllegalArgumentException("Period millis must be >= 0");
    }

    return period;
  }

  private static double[] parseThresholds(String ns, Configuration conf,
                                          int numLevels) {
    int[] percentages = conf.getInts(ns + "." +
        IPC_FCQ_DECAYSCHEDULER_THRESHOLDS_KEY);

    if (percentages.length == 0) {
      percentages = conf.getInts(ns + "." + IPC_DECAYSCHEDULER_THRESHOLDS_KEY);
      if (percentages.length == 0) {
        return getDefaultThresholds(numLevels);
      }
    } else {
      LOG.warn(IPC_FCQ_DECAYSCHEDULER_THRESHOLDS_KEY +
          " is deprecated. Please use " +
          IPC_DECAYSCHEDULER_THRESHOLDS_KEY);
    }

    if (percentages.length != numLevels - 1) {
      throw new IllegalArgumentException("Number of thresholds should be " +
          (numLevels - 1) + ". Was: " + percentages.length);
    }

    // Convert integer percentages to decimals
    double[] decimals = new double[percentages.length];
    for (int i = 0; i < percentages.length; i++) {
      decimals[i] = percentages[i] / 100.0;
    }

    return decimals;
  }

  /**
   * Generate default thresholds if user did not specify. Strategy is
   * to halve each time, since queue usage tends to be exponential.
   * So if numLevels is 4, we would generate: double[]{0.125, 0.25, 0.5}
   * which specifies the boundaries between each queue's usage.
   * @param numLevels number of levels to compute for
   * @return array of boundaries of length numLevels - 1
   */
  private static double[] getDefaultThresholds(int numLevels) {
    double[] ret = new double[numLevels - 1];
    double div = Math.pow(2, numLevels - 1);

    for (int i = 0; i < ret.length; i++) {
      ret[i] = Math.pow(2, i) / div;
    }
    return ret;
  }

  private static long[] parseBackOffResponseTimeThreshold(
      String ns, Configuration conf, int numLevels) {
    long[] responseTimeThresholds = conf.getTimeDurations(ns + "." +
            IPC_DECAYSCHEDULER_BACKOFF_RESPONSETIME_THRESHOLDS_KEY,
        TimeUnit.MILLISECONDS);
    // backoff thresholds not specified
    if (responseTimeThresholds.length == 0) {
      return getDefaultBackOffResponseTimeThresholds(numLevels);
    }
    // backoff thresholds specified but not match with the levels
    if (responseTimeThresholds.length != numLevels) {
      throw new IllegalArgumentException(
          "responseTimeThresholds must match with the number of priority " +
              "levels");
    }
    // invalid thresholds
    for (long responseTimeThreshold: responseTimeThresholds) {
      if (responseTimeThreshold <= 0) {
        throw new IllegalArgumentException(
            "responseTimeThreshold millis must be >= 0");
      }
    }
    return responseTimeThresholds;
  }

  // 10s for level 0, 20s for level 1, 30s for level 2, ...
  private static long[] getDefaultBackOffResponseTimeThresholds(int numLevels) {
    long[] ret = new long[numLevels];
    for (int i = 0; i < ret.length; i++) {
      ret[i] = 10000L * (i + 1);
    }
    return ret;
  }

  private static Boolean parseBackOffByResponseTimeEnabled(String ns,
                                                           Configuration conf) {
    return conf.getBoolean(ns + "." +
            IPC_DECAYSCHEDULER_BACKOFF_RESPONSETIME_ENABLE_KEY,
        IPC_DECAYSCHEDULER_BACKOFF_RESPONSETIME_ENABLE_DEFAULT);
  }

  /**
   * Update the scheduleCache to match current conditions in callCosts.
   */
  protected void recomputeScheduleCache() {
    Map<Object, Integer> nextCache = new HashMap<Object, Integer>();

    for (Map.Entry<Object, List<AtomicLong>> entry : callCosts.entrySet()) {
      Object id = entry.getKey();
      AtomicLong value = entry.getValue().get(0);

      long snapshot = value.get();
      int computedLevel = computePriorityLevel(snapshot, id);

      nextCache.put(id, computedLevel);
    }

    // Swap in to activate
    scheduleCacheRef.set(Collections.unmodifiableMap(nextCache));
  }

  /**
   * Adjust the stored cost for a given identity.
   *
   * @param identity the identity of the user whose cost should be adjusted
   * @param costDelta the cost to add for the given identity
   */
  private void addCost(Object identity, long costDelta) {
    // We will increment the cost, or create it if no such cost exists
    List<AtomicLong> cost = this.callCosts.get(identity);
    if (cost == null) {
      // Create the costs since no such cost exists.
      // idx 0 for decayed call cost
      // idx 1 for the raw call cost
      cost = new ArrayList<AtomicLong>(2);
      cost.add(new AtomicLong(0));
      cost.add(new AtomicLong(0));

      // Put it in, or get the AtomicInteger that was put in by another thread
      List<AtomicLong> otherCost = callCosts.putIfAbsent(identity, cost);
      if (otherCost != null) {
        cost = otherCost;
      }
    }

    // Update the total
    totalDecayedCallCost.getAndAdd(costDelta);
    totalRawCallCost.getAndAdd(costDelta);

    // At this point value is guaranteed to be not null. It may however have
    // been clobbered from callCosts. Nonetheless, we return what
    // we have.
    cost.get(1).getAndAdd(costDelta);
    cost.get(0).getAndAdd(costDelta);
  }

  /**
   * Given the cost for an identity, compute a scheduling decision.
   *
   * @param cost the cost for an identity
   * @return scheduling decision from 0 to numLevels - 1
   */
  private int computePriorityLevel(long cost, Object identity) {
    Integer staticPriority = staticPriorities.get(identity);
    if (staticPriority != null) {
      return staticPriority.intValue();
    }
    long totalCallSnapshot = totalDecayedCallCost.get();

    double proportion = 0;
    if (totalCallSnapshot > 0) {
      proportion = (double) cost / totalCallSnapshot;
    }

    // Start with low priority levels, since they will be most common
    for (int i = (numLevels - 1); i > 0; i--) {
      if (proportion >= this.thresholds[i - 1]) {
        return i; // We've found our level number
      }
    }

    // If we get this far, we're at level 0
    return 0;
  }

  /**
   * Returns the priority level for a given identity by first trying the cache,
   * then computing it.
   * @param identity an object responding to toString and hashCode
   * @return integer scheduling decision from 0 to numLevels - 1
   */
  private int cachedOrComputedPriorityLevel(Object identity) {
    // Try the cache
    Map<Object, Integer> scheduleCache = scheduleCacheRef.get();
    if (scheduleCache != null) {
      Integer priority = scheduleCache.get(identity);
      if (priority != null) {
        LOG.debug("Cache priority for: {} with priority: {}", identity,
            priority);
        return priority;
      }
    }

    // Cache was no good, compute it
    List<AtomicLong> costList = callCosts.get(identity);
    long currentCost = costList == null ? 0 : costList.get(0).get();
    int priority = computePriorityLevel(currentCost, identity);
    LOG.debug("compute priority for {} priority {}", identity, priority);
    return priority;
  }

  private String getIdentity(Schedulable obj) {
    String identity = this.identityProvider.makeIdentity(obj);
    if (identity == null) {
      // Identity provider did not handle this
      identity = DECAYSCHEDULER_UNKNOWN_IDENTITY;
    }
    return identity;
  }

  /**
   * Compute the appropriate priority for a schedulable based on past requests.
   * @param obj the schedulable obj to query and remember
   * @return the level index which we recommend scheduling in
   */
  @Override
  public int getPriorityLevel(Schedulable obj) {
    // First get the identity
    String identity = getIdentity(obj);
    // highest priority users may have a negative priority but their
    // calls will be priority 0.
    return Math.max(0, cachedOrComputedPriorityLevel(identity));
  }

  @Override
  public boolean shouldBackOff(Schedulable obj) {
    Boolean backOff = false;
    if (backOffByResponseTimeEnabled) {
      int priorityLevel = obj.getPriorityLevel();
      if (LOG.isDebugEnabled()) {
        double[] responseTimes = metrics.getAverageResponseTime();
        LOG.debug("Current Caller: {}  Priority: {} ",
            obj.getUserGroupInformation().getUserName(),
            obj.getPriorityLevel());
        for (int i = 0; i < numLevels; i++) {
          LOG.debug("Queue: {} responseTime: {} backoffThreshold: {}", i,
              responseTimes[i], backOffResponseTimeThresholds[i]);
        }
      }
      // High priority rpc over threshold triggers back off of low priority rpc
      for (int i = 0; i < priorityLevel + 1; i++) {
        if (metrics.getResponseTimeCountInLastWindowAtomic().get(i) >
            backOffResponseTimeThresholds[i]) {
          backOff = true;
          break;
        }
      }
    }
    return backOff;
  }

  @Override
  public void addResponseTime(String callName, Schedulable schedulable,
                              ProcessingDetails details) {
    String user = identityProvider.makeIdentity(schedulable);
    long processingCost = costProvider.getCost(details);
    addCost(user, processingCost);

    int priorityLevel = schedulable.getPriorityLevel();
    long queueTime = details
        .get(ProcessingDetails.Timing.QUEUE, metricsTimeUnit);
    long processingTime = details.get(ProcessingDetails.Timing.PROCESSING,
        metricsTimeUnit);

    this.decayRpcSchedulerDetailedMetrics.addQueueTime(
        priorityLevel, queueTime);
    this.decayRpcSchedulerDetailedMetrics.addProcessingTime(
        priorityLevel, processingTime);

    metrics.getResponseTimeCountInCurrWindow()
        .getAndIncrement(priorityLevel);
    metrics.getResponseTimeTotalInCurrWindow()
        .getAndAdd(priorityLevel,
        queueTime + processingTime);
    if (LOG.isDebugEnabled()) {
      LOG.debug("addResponseTime for call: {} " +
              "priority: {} queueTime: {} " +
              "processingTime: {} ", callName, priorityLevel, queueTime,
          processingTime);
    }
  }

  public String getNamespace() {
    return namespace;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public ConcurrentHashMap<Object, List<AtomicLong>> getCallCosts() {
    return callCosts;
  }

  public OzoneDecayRpcSchedulerMetrics getSchedulerMetrics() {
    return metrics;
  }

  public int getNumLevels() {
    return numLevels;
  }

  public AtomicReference<Map<Object, Integer>> getScheduleCacheRef() {
    return scheduleCacheRef;
  }

  public AtomicLong getTotalDecayedCallCost() {
    return totalDecayedCallCost;
  }

  public AtomicLong getTotalRawCallCost() {
    return totalRawCallCost;
  }

  public int getTopUsersCount() {
    return topUsersCount;
  }

  @Override
  public void stop() {
    metricsProxy.unregisterSource(namespace);
    OzoneDecayRpcSchedulerMetricsProxy.removeInstance(namespace);
    decayRpcSchedulerDetailedMetrics.shutdown();
  }
}
