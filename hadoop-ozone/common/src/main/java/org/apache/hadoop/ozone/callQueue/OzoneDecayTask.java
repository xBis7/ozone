package org.apache.hadoop.ozone.callQueue;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.callQueue.metrics.OzoneDecayRpcSchedulerMetrics;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;
import java.util.TimerTask;
import java.util.Timer;
import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This TimerTask will call decayCurrentCosts until
 * the scheduler has been garbage collected.
 */
public class OzoneDecayTask extends TimerTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneDecayTask.class);
  /**
   * Decay factor controls how much each cost is suppressed by on each sweep.
   * Valid numbers are &gt; 0 and &lt; 1. Decay factor works in tandem with
   * period
   * to control how long the scheduler remembers an identity.
   */
  public static final String IPC_SCHEDULER_DECAYSCHEDULER_FACTOR_KEY =
      "decay-scheduler.decay-factor";
  public static final double IPC_SCHEDULER_DECAYSCHEDULER_FACTOR_DEFAULT =
      0.5;
  public static final String IPC_FCQ_DECAYSCHEDULER_FACTOR_KEY =
      "faircallqueue.decay-scheduler.decay-factor";
  private static final double PRECISION = 0.0001;

  // nextCost = currentCost * decayFactor
  private final double decayFactor;
  private final WeakReference<OzoneDecayRpcScheduler> schedulerRef;
  private Timer timer;

  public OzoneDecayTask(OzoneDecayRpcScheduler scheduler,
                        Timer timer) {
    this.schedulerRef = new WeakReference<>(scheduler);
    this.timer = timer;
    String namespace = scheduler.getNamespace();
    Configuration conf = scheduler.getConfiguration();
    this.decayFactor = parseDecayFactor(namespace, conf);
  }

  @Override
  public void run() {
    OzoneDecayRpcScheduler scheduler = schedulerRef.get();
    if (scheduler != null) {
      decayCurrentCosts(scheduler);
    } else {
      // Our scheduler was garbage collected since it is no longer in use,
      // so we should terminate the timer as well
      timer.cancel();
      timer.purge();
    }
  }

  /**
   * Decay the stored costs for each user and clean as necessary.
   * This method should be called periodically in order to keep
   * costs current.
   */
  private void decayCurrentCosts(OzoneDecayRpcScheduler scheduler) {
    LOG.debug("Start to decay current costs.");
    try {
      long totalDecayedCost = 0;
      long totalRawCost = 0;
      Iterator<Map.Entry<Object, List<AtomicLong>>> it =
          scheduler.getCallCosts().entrySet().iterator();

      while (it.hasNext()) {
        Map.Entry<Object, List<AtomicLong>> entry = it.next();
        AtomicLong decayedCost = entry.getValue().get(0);
        AtomicLong rawCost = entry.getValue().get(1);


        // Compute the next value by reducing it by the decayFactor
        totalRawCost += rawCost.get();
        long currentValue = decayedCost.get();
        long nextValue = (long) (currentValue * decayFactor);
        totalDecayedCost += nextValue;
        decayedCost.set(nextValue);

        LOG.debug(
            "Decaying costs for the user: {}, its decayedCost: {}, rawCost: {}",
            entry.getKey(), nextValue, rawCost.get());
        if (nextValue == 0) {
          LOG.debug("The decayed cost for the user {} is zero " +
              "and being cleaned.", entry.getKey());
          // We will clean up unused keys here. An interesting optimization
          // might be to have an upper bound on keyspace in callCosts and only
          // clean once we pass it.
          it.remove();
        }
      }

      // Update the total so that we remain in sync
      scheduler.getTotalDecayedCallCost().set(totalDecayedCost);
      scheduler.getTotalRawCallCost().set(totalRawCost);

      LOG.debug("After decaying the stored costs, totalDecayedCost: {}, " +
          "totalRawCallCost: {}.", totalDecayedCost, totalRawCost);
      // Now refresh the cache of scheduling decisions
      scheduler.recomputeScheduleCache();

      // Update average response time with decay
      updateAverageResponseTime(scheduler, true);
    } catch (Exception ex) {
      LOG.error("decayCurrentCosts exception: " +
          ExceptionUtils.getStackTrace(ex));
      throw ex;
    }
  }


  // Update the cached average response time at the end of the decay window
  private void updateAverageResponseTime(
      OzoneDecayRpcScheduler scheduler, boolean enableDecay) {
    OzoneDecayRpcSchedulerMetrics metrics = scheduler.getSchedulerMetrics();
    int numLevels = scheduler.getNumLevels();

    for (int i = 0; i < numLevels; i++) {
      double averageResponseTime = 0;
      long totalResponseTime = metrics
          .getResponseTimeTotalInCurrWindow().get(i);
      long responseTimeCount = metrics
          .getResponseTimeCountInCurrWindow().get(i);
      if (responseTimeCount > 0) {
        averageResponseTime = (double) totalResponseTime / responseTimeCount;
      }
      final double lastAvg = metrics
          .getResponseTimeAvgInLastWindow().get(i);
      if (lastAvg > PRECISION || averageResponseTime > PRECISION) {
        if (enableDecay) {
          final double decayed = decayFactor * lastAvg + averageResponseTime;
          metrics.getResponseTimeAvgInLastWindow().set(i, decayed);
        } else {
          metrics.getResponseTimeAvgInLastWindow().set(i, averageResponseTime);
        }
      } else {
        metrics.getResponseTimeAvgInLastWindow().set(i, 0);
      }
      metrics.getResponseTimeCountInLastWindowAtomic()
          .set(i, responseTimeCount);
      if (LOG.isDebugEnabled()) {
        LOG.debug("updateAverageResponseTime queue: {} Average: {} Count: {}",
            i, averageResponseTime, responseTimeCount);
      }
      // Reset for next decay window
      metrics.getResponseTimeTotalInCurrWindow().set(i, 0);
      metrics.getResponseTimeCountInCurrWindow().set(i, 0);
    }
  }

  private static double parseDecayFactor(String ns, Configuration conf) {
    double factor = conf.getDouble(ns + "." +
        IPC_FCQ_DECAYSCHEDULER_FACTOR_KEY, 0.0);
    if (factor == 0.0) {
      factor = conf.getDouble(ns + "." +
              IPC_SCHEDULER_DECAYSCHEDULER_FACTOR_KEY,
          IPC_SCHEDULER_DECAYSCHEDULER_FACTOR_DEFAULT);
    } else if ((factor > 0.0) && (factor < 1)) {
      LOG.warn(IPC_FCQ_DECAYSCHEDULER_FACTOR_KEY +
          " is deprecated. Please use " +
          IPC_SCHEDULER_DECAYSCHEDULER_FACTOR_KEY + ".");
    }
    if (factor <= 0 || factor >= 1) {
      throw new IllegalArgumentException("Decay Factor " +
          "must be between 0 and 1");
    }

    return factor;
  }

  // For testing
  @VisibleForTesting
  double getDecayFactor() {
    return decayFactor;
  }

  @VisibleForTesting
  void forceDecay(OzoneDecayRpcScheduler scheduler) {
    decayCurrentCosts(scheduler);
  }
}
