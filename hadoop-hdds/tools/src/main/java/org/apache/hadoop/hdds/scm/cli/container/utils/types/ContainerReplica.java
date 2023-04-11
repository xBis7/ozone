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

/**
 * Class to map missing container replicas
 * json array from Recon endpoint
 * /api/v1/containers/unhealthy/MISSING.
 */
public class ContainerReplica {

  private long containerId;
  private String datanodeUuid;
  private String datanodeHost;
  private long firstSeenTime;
  private long lastSeenTime;
  private long lastBcsId;

  @SuppressWarnings("parameternumber")
  @JsonCreator
  public ContainerReplica(
      @JsonProperty("containerId") long containerId,
      @JsonProperty("datanodeUuid") String datanodeUuid,
      @JsonProperty("datanodeHost") String datanodeHost,
      @JsonProperty("firstSeenTime") long firstSeenTime,
      @JsonProperty("lastSeenTime") long lastSeenTime,
      @JsonProperty("lastBcsId") long lastBcsId) {
    this.containerId = containerId;
    this.datanodeUuid = datanodeUuid;
    this.datanodeHost = datanodeHost;
    this.firstSeenTime = firstSeenTime;
    this.lastSeenTime = lastSeenTime;
    this.lastBcsId = lastBcsId;
  }

  public long getContainerId() {
    return containerId;
  }

  public void setContainerId(long containerId) {
    this.containerId = containerId;
  }

  public String getDatanodeUuid() {
    return datanodeUuid;
  }

  public void setDatanodeUuid(String datanodeUuid) {
    this.datanodeUuid = datanodeUuid;
  }

  public String getDatanodeHost() {
    return datanodeHost;
  }

  public void setDatanodeHost(String datanodeHost) {
    this.datanodeHost = datanodeHost;
  }

  public long getFirstSeenTime() {
    return firstSeenTime;
  }

  public void setFirstSeenTime(long firstSeenTime) {
    this.firstSeenTime = firstSeenTime;
  }

  public long getLastSeenTime() {
    return lastSeenTime;
  }

  public void setLastSeenTime(long lastSeenTime) {
    this.lastSeenTime = lastSeenTime;
  }

  public long getLastBcsId() {
    return lastBcsId;
  }

  public void setLastBcsId(long lastBcsId) {
    this.lastBcsId = lastBcsId;
  }
}
