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
package org.apache.hadoop.ozone.snapshot;

import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus;

import java.util.List;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * POJO for List Snapshot Diff Response.
 */
public class ListSnapshotDiffResponse {

  private final String volumeName;
  private final String bucketName;
  private final JobStatus jobStatus;
  private final List<String> jobList;

  public ListSnapshotDiffResponse(
      final String volumeName, final String bucketName,
      final JobStatus jobStatus,
      final List<String> jobList) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.jobStatus = jobStatus;
    this.jobList = jobList;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public JobStatus getJobStatus() {
    return jobStatus;
  }

  public List<String> getJobList() {
    return jobList;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("List of snapshotDiff jobs with status: ");
    builder.append(jobStatus);
    builder.append(", for ");
    builder.append(OM_KEY_PREFIX);
    builder.append(volumeName);
    builder.append(OM_KEY_PREFIX);
    builder.append(bucketName);
    builder.append("\n");
    for (String job : jobList) {
      builder.append(job);
      builder.append("\n");
    }
    return builder.toString();
  }
}
