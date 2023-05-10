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
package org.apache.hadoop.hdds.scm.cli.container;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.OzoneAdmin;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.cli.container.utils.ReconEndpointUtils;
import org.apache.hadoop.hdds.scm.cli.container.utils.types.ContainerKey;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaInfo;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;
import picocli.CommandLine.Command;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * The handler of cleanup container command,
 * used for missing containers.
 */
@Command(
    name = "cleanup",
    description = "Cleanup a missing container",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
@MetaInfServices(SubcommandWithParent.class)
public class CleanupSubcommand extends ScmSubcommand
    implements SubcommandWithParent {

  private static final Logger LOG =
      LoggerFactory.getLogger(CleanupSubcommand.class);

  private OzoneConfiguration conf;

  @ArgGroup(multiplicity = "1")
  private FilterOptions filterOptions;

  /**
   * Nested class to define exclusive filtering options.
   */
  private static class FilterOptions {
    @Option(names = {"--container-id", "--cid"},
        paramLabel = "CONTAINER_ID",
        description = "Cleanup missing container based on container ID.",
        defaultValue = "0")
    private long containerId;
  }

  private static final String CONTAINERS_ENDPOINT =
      "/api/v1/containers";

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    OzoneAdmin ozoneAdmin = new OzoneAdmin();
    conf = ozoneAdmin.getOzoneConf();

    if (filterOptions.containerId != 0) {
      // When a datanode dies, all replicas are removed.
      // If the container is missing then the container's
      // replica list should be empty.
      List<ContainerReplicaInfo> replicas = scmClient
          .getContainerReplicas(filterOptions.containerId);

      if (replicas.isEmpty()) {
        LOG.info("Verified Container " +
            filterOptions.containerId + " is missing.");
        // Delete keys in OM
        deleteContainerKeys(filterOptions.containerId);

        // Delete container in SCM container map
        scmClient.deleteContainerInSCM(filterOptions.containerId);
        LOG.info("Successfully cleaned up missing container " +
            filterOptions.containerId);
      } else {
        LOG.error("Provided ID doesn't belong to a missing container");
      }
    }
  }

  private void deleteContainerKeys(long containerID)
      throws IOException {
    // Get container keys list
    List<ContainerKey> containerKeyList =
        getContainerKeysFromRecon(containerID);

    if (containerKeyList.size() > 0) {
      for (ContainerKey info : containerKeyList) {
        try (OzoneClient client = new OzoneClient(conf,
            new RpcClient(conf, null))) {
          OzoneVolume volume = client.getObjectStore()
              .getVolume(info.getVolume());
          OzoneBucket bucket = volume.getBucket(info.getBucket());
          bucket.deleteKey(info.getKey());
        }
      }
      LOG.info("Successfully deleted all keys for Container " + containerID);
    } else {
      LOG.info("Container " + containerID + " has no keys");
    }
  }

  private List<ContainerKey> getContainerKeysFromRecon(long containerID)
      throws JsonProcessingException {
    StringBuilder urlBuilder = new StringBuilder();
    urlBuilder.append(ReconEndpointUtils.getReconWebAddress(conf))
        .append(CONTAINERS_ENDPOINT)
        .append("/")
        .append(containerID)
        .append("/keys");
    String containerKeysResponse = "";
    try {
      containerKeysResponse = ReconEndpointUtils.makeHttpCall(urlBuilder,
          ReconEndpointUtils.isHTTPSEnabled(conf), conf);
    } catch (Exception e) {
      LOG.error("Error getting container keys response from Recon");
    }

    if (Strings.isNullOrEmpty(containerKeysResponse)) {
      LOG.info("Container keys response from Recon is empty");
      // Return empty list
      return new LinkedList<>();
    }

    // Get the keys JSON array
    String keysJsonArray = containerKeysResponse.substring(
        containerKeysResponse.indexOf("keys\":") + 6,
        containerKeysResponse.length() - 1);

    final ObjectMapper objectMapper = new ObjectMapper();

    return objectMapper.readValue(keysJsonArray,
        new TypeReference<List<ContainerKey>>() { });
  }

  private void triggerContainerHealthCheckOnRecon() {
    StringBuilder urlBuilder = new StringBuilder();
    urlBuilder.append(ReconEndpointUtils.getReconWebAddress(conf))
        .append(CONTAINERS_ENDPOINT)
        .append("/triggerHealthCheck");
    String containerResponse = "";
    try {
      containerResponse = ReconEndpointUtils.makeHttpCall(urlBuilder,
          ReconEndpointUtils.isHTTPSEnabled(conf), conf);
    } catch (Exception e) {
      LOG.error("Error getting response from Recon while " +
          "triggering a container health check.");
    }

    if (Strings.isNullOrEmpty(containerResponse)) {
      LOG.info("Container health check trigger response from Recon is empty");
    } else {
      if (containerResponse.contains("true")) {
        LOG.info("Container health check" +
            " was successfully triggered on Recon");
      }
    }
  }

  @Override
  public Class<?> getParentType() {
    return OzoneAdmin.class;
  }
}
