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
import java.util.HashMap;

/**
 * Class to map the container keys json array
 * from Recon endpoint /api/v1/containers/{ID}/keys.
 */
public class ContainerKey {

  private String volume;
  private String bucket;
  private String key;
  private long dataSize;
  private ArrayList<Integer> versions;
  private HashMap<Long, Object> blocks;
  private String creationTime;
  private String modificationTime;

  @SuppressWarnings("parameternumber")
  @JsonCreator
  public ContainerKey(
      @JsonProperty("Volume") String volume,
      @JsonProperty("Bucket") String bucket,
      @JsonProperty("Key") String key,
      @JsonProperty("DataSize") long dataSize,
      @JsonProperty("Versions") ArrayList<Integer> versions,
      @JsonProperty("Blocks") HashMap<Long, Object> blocks,
      @JsonProperty("CreationTime") String creationTime,
      @JsonProperty("ModificationTime") String modificationTime) {
    this.volume = volume;
    this.bucket = bucket;
    this.key = key;
    this.dataSize = dataSize;
    this.versions = versions;
    this.blocks = blocks;
    this.creationTime = creationTime;
    this.modificationTime = modificationTime;
  }

  public String getVolume() {
    return volume;
  }

  public void setVolume(String volume) {
    this.volume = volume;
  }

  public String getBucket() {
    return bucket;
  }

  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public long getDataSize() {
    return dataSize;
  }

  public void setDataSize(long dataSize) {
    this.dataSize = dataSize;
  }

  public ArrayList<Integer> getVersions() {
    return versions;
  }

  public void setVersions(ArrayList<Integer> versions) {
    this.versions = versions;
  }

  public HashMap<Long, Object> getBlocks() {
    return blocks;
  }

  public void setBlocks(HashMap<Long, Object> blocks) {
    this.blocks = blocks;
  }

  public String getCreationTime() {
    return creationTime;
  }

  public void setCreationTime(String creationTime) {
    this.creationTime = creationTime;
  }

  public String getModificationTime() {
    return modificationTime;
  }

  public void setModificationTime(String modificationTime) {
    this.modificationTime = modificationTime;
  }
}
