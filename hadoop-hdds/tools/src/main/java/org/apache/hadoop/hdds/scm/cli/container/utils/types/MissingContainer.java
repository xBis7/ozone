/*
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
package org.apache.hadoop.hdds.scm.cli.container.utils.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;

/**
 * Class to map the missing containers
 * json array from Recon endpoint
 * /api/v1/containers/unhealthy/MISSING.
 */
public class MissingContainer {

  private long containerID;
  private String containerState;
  private long unhealthySince;
  private long expectedReplicaCount;
  private long actualReplicaCount;
  private long replicaDeltaCount;
  private String reason;
  private long keys;
  private String pipelineID;
  private ArrayList<ContainerReplica> replicas;

  @SuppressWarnings("parameternumber")
  @JsonCreator
  public MissingContainer(
      @JsonProperty("containerID") long containerID,
      @JsonProperty("containerState") String containerState,
      @JsonProperty("unhealthySince") long unhealthySince,
      @JsonProperty("expectedReplicaCount") long expectedReplicaCount,
      @JsonProperty("actualReplicaCount") long actualReplicaCount,
      @JsonProperty("replicaDeltaCount") long replicaDeltaCount,
      @JsonProperty("reason") String reason,
      @JsonProperty("keys") long keys,
      @JsonProperty("pipelineID") String pipelineID,
      @JsonProperty("replicas") ArrayList<ContainerReplica> replicas) {
    this.containerID = containerID;
    this.containerState = containerState;
    this.unhealthySince = unhealthySince;
    this.expectedReplicaCount = expectedReplicaCount;
    this.actualReplicaCount = actualReplicaCount;
    this.replicaDeltaCount = replicaDeltaCount;
    this.reason = reason;
    this.keys = keys;
    this.pipelineID = pipelineID;
    this.replicas = replicas;
  }

  public long getContainerID() {
    return containerID;
  }

  public void setContainerID(long containerID) {
    this.containerID = containerID;
  }

  public String getContainerState() {
    return containerState;
  }

  public void setContainerState(String containerState) {
    this.containerState = containerState;
  }

  public long getUnhealthySince() {
    return unhealthySince;
  }

  public void setUnhealthySince(long unhealthySince) {
    this.unhealthySince = unhealthySince;
  }

  public long getExpectedReplicaCount() {
    return expectedReplicaCount;
  }

  public void setExpectedReplicaCount(long expectedReplicaCount) {
    this.expectedReplicaCount = expectedReplicaCount;
  }

  public long getActualReplicaCount() {
    return actualReplicaCount;
  }

  public void setActualReplicaCount(long actualReplicaCount) {
    this.actualReplicaCount = actualReplicaCount;
  }

  public long getReplicaDeltaCount() {
    return replicaDeltaCount;
  }

  public void setReplicaDeltaCount(long replicaDeltaCount) {
    this.replicaDeltaCount = replicaDeltaCount;
  }

  public String getReason() {
    return reason;
  }

  public void setReason(String reason) {
    this.reason = reason;
  }

  public long getKeys() {
    return keys;
  }

  public void setKeys(long keys) {
    this.keys = keys;
  }

  public String getPipelineID() {
    return pipelineID;
  }

  public void setPipelineID(String pipelineID) {
    this.pipelineID = pipelineID;
  }

  public ArrayList<ContainerReplica> getReplicas() {
    return replicas;
  }

  public void setReplicas(ArrayList<ContainerReplica> replicas) {
    this.replicas = replicas;
  }
}
