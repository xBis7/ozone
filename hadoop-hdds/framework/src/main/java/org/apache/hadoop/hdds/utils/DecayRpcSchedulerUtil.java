package org.apache.hadoop.hdds.utils;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;

import java.util.ArrayList;
import java.util.List;

public class DecayRpcSchedulerUtil {

  private static final MetricsInfo USERNAME_INFO = new MetricsInfo() {
    @Override
    public String name() {
      return "username";
    }

    @Override
    public String description() {
      return "caller username";
    }
  };

  public static final List<String> usernameList = new ArrayList<>();

  /**
   * For Decay_Rpc_Scheduler, the metric name is in format
   * "Caller(<callers_username>).Volume"
   * or
   * "Caller(<callers_username>).Priority"
   * Split it and return only the metric.
   * @param recordName
   * @param metricName "Caller(xyz).Volume" or "Caller(xyz).Priority"
   * @return "Volume" or "Priority"
   */
  public static String splitMetricNameIfNeeded(String recordName,
                                               String metricName) {
    if (recordName.toLowerCase().contains("decayrpcscheduler") &&
        metricName.toLowerCase().contains("caller(")) {
      // names will contain ["Caller(xyz)", "Volume" / "Priority"]
      String[] names = metricName.split("[.]");

      // Caller(xyz)
      String caller = names[0];

      // subStrings will contain ["Caller", "xyz"]
      String[] subStrings = caller.split("[()]");

      String username = subStrings[1];
      usernameList.add(username);

      // "Volume" or "Priority"
      return names[1];
    }

    return metricName;
  }

  public static List<MetricsTag> tagListWithUsernameIfNeeded(
      MetricsRecord metricsRecord) {
    List<MetricsTag> list = new ArrayList<>(metricsRecord.tags());

    if (usernameList.size() > 0) {
      String username = usernameList.get(0);
      MetricsTag tag = new MetricsTag(USERNAME_INFO, username);
      list.add(tag);
    }
    return list;
  }
}
