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

package org.apache.hadoop.ozone.om;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.util.CacheMetrics;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_CONTAINER_LOCATION_CACHE_SIZE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_CONTAINER_LOCATION_CACHE_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_CONTAINER_LOCATION_CACHE_TTL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_CONTAINER_LOCATION_CACHE_TTL_DEFAULT;

/**
 * Wrapper class for Scm protocol clients.
 */
public class ScmClient {

  private final ScmBlockLocationProtocol blockClient;
  private final StorageContainerLocationProtocol containerClient;
  private final LoadingCache<Long, Pipeline> containerLocationCache;
  private final CacheMetrics containerCacheMetrics;

  ScmClient(ScmBlockLocationProtocol blockClient,
            StorageContainerLocationProtocol containerClient,
            OzoneConfiguration configuration) {
    this.containerClient = containerClient;
    this.blockClient = blockClient;
    this.containerLocationCache =
        createContainerLocationCache(configuration, containerClient);
    this.containerCacheMetrics = CacheMetrics.create(containerLocationCache,
        "ContainerInfo");
  }

  static LoadingCache<Long, Pipeline> createContainerLocationCache(
      OzoneConfiguration configuration,
      StorageContainerLocationProtocol containerClient) {
    int maxSize = configuration.getInt(OZONE_OM_CONTAINER_LOCATION_CACHE_SIZE,
        OZONE_OM_CONTAINER_LOCATION_CACHE_SIZE_DEFAULT);
    TimeUnit unit =  OZONE_OM_CONTAINER_LOCATION_CACHE_TTL_DEFAULT.getUnit();
    long ttl = configuration.getTimeDuration(
        OZONE_OM_CONTAINER_LOCATION_CACHE_TTL,
        OZONE_OM_CONTAINER_LOCATION_CACHE_TTL_DEFAULT.getDuration(), unit);
    return CacheBuilder.newBuilder()
        .maximumSize(maxSize)
        .expireAfterWrite(ttl, unit)
        .recordStats()
        .build(new CacheLoader<Long, Pipeline>() {
          @NotNull
          @Override
          public Pipeline load(@NotNull Long key) throws Exception {
            return containerClient.getContainerWithPipeline(key).getPipeline();
          }

          @NotNull
          @Override
          public Map<Long, Pipeline> loadAll(
              @NotNull Iterable<? extends Long> keys) throws Exception {
            return containerClient.getContainerWithPipelineBatch(keys)
                .stream()
                .collect(Collectors.toMap(
                    x -> x.getContainerInfo().getContainerID(),
                    ContainerWithPipeline::getPipeline
                ));
          }
        });
  }

  public ScmBlockLocationProtocol getBlockClient() {
    return this.blockClient;
  }

  public StorageContainerLocationProtocol getContainerClient() {
    return this.containerClient;
  }

  public Map<Long, Pipeline> getContainerLocations(Iterable<Long> containerIds,
                                                  boolean forceRefresh)
      throws IOException {
    if (forceRefresh) {
      containerLocationCache.invalidateAll(containerIds);
    }
    try {
      Map<Long, Pipeline> result = containerLocationCache.getAll(containerIds);
      // Don't keep empty pipelines in the cache.
      List<Long> emptyPipelines = result.entrySet().stream()
          .filter(e -> e.getValue().isEmpty())
          .map(Map.Entry::getKey)
          .collect(Collectors.toList());
      containerLocationCache.invalidateAll(emptyPipelines);
      return result;
    } catch (ExecutionException e) {
      return handleCacheExecutionException(e);
    } catch (InvalidCacheLoadException e) {
      // this is thrown when a container is not found from SCM.
      // In this case, return  available, instead of propagating the
      // exception to client code.
      Map<Long, Pipeline> result = new HashMap<>();
      for (Long containerId : containerIds) {
        Pipeline p = containerLocationCache.getIfPresent(containerId);
        if (p != null) {
          result.put(containerId, p);
        }
      }
      return result;
    }
  }

  private <T> T handleCacheExecutionException(ExecutionException e)
      throws IOException {
    if (e.getCause() instanceof IOException) {
      throw (IOException) e.getCause();
    }
    throw new IllegalStateException("Unexpected exception accessing " +
        "container location", e.getCause());
  }

  public void close() {
    containerCacheMetrics.unregister();
  }

}
