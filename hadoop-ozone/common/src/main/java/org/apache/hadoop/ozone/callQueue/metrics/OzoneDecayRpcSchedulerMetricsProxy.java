package org.apache.hadoop.ozone.callQueue.metrics;

import org.apache.hadoop.ipc.DecayRpcSchedulerMXBean;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.ozone.callQueue.OzoneDecayRpcScheduler;

import javax.management.ObjectName;
import java.lang.ref.WeakReference;
import java.util.HashMap;

/**
 * OzoneDecayRpcSchedulerMetricsProxy is the
 * Ozone implementation of #DecayRpcScheduler.MetricsProxy.
 * This class is a singleton because we may init multiple schedulers
 * and clean up resources when a new scheduler replaces the old one.
 */
public class OzoneDecayRpcSchedulerMetricsProxy
    implements DecayRpcSchedulerMXBean, MetricsSource {

  // One singleton per namespace
  private static final HashMap<String, OzoneDecayRpcSchedulerMetricsProxy>
      INSTANCES = new HashMap<>();

  // Weakref for delegate, so we don't retain it forever if it can be GC'd
  private WeakReference<OzoneDecayRpcScheduler> delegate;
  private double[] averageResponseTimeDefault;
  private long[] callCountInLastWindowDefault;
  private ObjectName decayRpcSchedulerInfoBeanName;

  private OzoneDecayRpcSchedulerMetricsProxy(String namespace, int numLevels,
                       OzoneDecayRpcScheduler drs) {
    averageResponseTimeDefault = new double[numLevels];
    callCountInLastWindowDefault = new long[numLevels];
    setDelegate(drs);
    decayRpcSchedulerInfoBeanName =
        MBeans.register(namespace, "OzoneDecayRpcScheduler", this);
    this.registerMetrics2Source(namespace);
  }

  public static synchronized OzoneDecayRpcSchedulerMetricsProxy getInstance(
      String namespace, int numLevels, OzoneDecayRpcScheduler drs) {
    OzoneDecayRpcSchedulerMetricsProxy mp = INSTANCES.get(namespace);
    if (mp == null) {
      // We must create one
      mp = new OzoneDecayRpcSchedulerMetricsProxy(namespace, numLevels, drs);
      INSTANCES.put(namespace, mp);
    } else  if (drs != mp.delegate.get()) {
      // in case of delegate is reclaimed, we should set it again
      mp.setDelegate(drs);
    }
    return mp;
  }

  public static synchronized void removeInstance(String namespace) {
    OzoneDecayRpcSchedulerMetricsProxy.INSTANCES.remove(namespace);
  }

  public void setDelegate(OzoneDecayRpcScheduler obj) {
    this.delegate = new WeakReference<>(obj);
  }

  void registerMetrics2Source(String namespace) {
    final String name = "OzoneDecayRpcSchedulerMetrics2." + namespace;
    DefaultMetricsSystem.instance().register(name, name, this);
  }

  public void unregisterSource(String namespace) {
    final String name = "OzoneDecayRpcSchedulerMetrics2." + namespace;
    DefaultMetricsSystem.instance().unregisterSource(name);
    if (decayRpcSchedulerInfoBeanName != null) {
      MBeans.unregister(decayRpcSchedulerInfoBeanName);
    }
  }

  @Override
  public String getSchedulingDecisionSummary() {
    OzoneDecayRpcScheduler scheduler = delegate.get();
    if (scheduler == null) {
      return "No Active Scheduler";
    } else {
      return scheduler.getSchedulerMetrics().getSchedulingDecisionSummary();
    }
  }

  @Override
  public String getCallVolumeSummary() {
    OzoneDecayRpcScheduler scheduler = delegate.get();
    if (scheduler == null) {
      return "No Active Scheduler";
    } else {
      return scheduler.getSchedulerMetrics().getCallVolumeSummary();
    }
  }

  @Override
  public int getUniqueIdentityCount() {
    OzoneDecayRpcScheduler scheduler = delegate.get();
    if (scheduler == null) {
      return -1;
    } else {
      return scheduler.getSchedulerMetrics().getUniqueIdentityCount();
    }
  }

  @Override
  public long getTotalCallVolume() {
    OzoneDecayRpcScheduler scheduler = delegate.get();
    if (scheduler == null) {
      return -1;
    } else {
      return scheduler.getSchedulerMetrics().getTotalCallVolume();
    }
  }

  @Override
  public double[] getAverageResponseTime() {
    OzoneDecayRpcScheduler scheduler = delegate.get();
    if (scheduler == null) {
      return averageResponseTimeDefault.clone();
    } else {
      return scheduler.getSchedulerMetrics().getAverageResponseTime();
    }
  }

  public long[] getResponseTimeCountInLastWindow() {
    OzoneDecayRpcScheduler scheduler = delegate.get();
    if (scheduler == null) {
      return callCountInLastWindowDefault.clone();
    } else {
      return scheduler.getSchedulerMetrics().getResponseTimeCountInLastWindow();
    }
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    OzoneDecayRpcScheduler scheduler = delegate.get();
    if (scheduler != null) {
      scheduler.getSchedulerMetrics().getMetrics(collector, all);
    }
  }
}
