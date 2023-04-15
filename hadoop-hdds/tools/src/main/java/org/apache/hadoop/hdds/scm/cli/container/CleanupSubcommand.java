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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.cli.container.utils.ReconEndpointUtils;
import org.apache.hadoop.hdds.scm.cli.container.utils.types.ContainerKey;
import org.apache.hadoop.hdds.scm.cli.container.utils.types.ContainerReplica;
import org.apache.hadoop.hdds.scm.cli.container.utils.types.MissingContainer;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
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
    description = "Cleanup a missing or unhealthy container",
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
        description = "Cleanup container based on container ID.",
        defaultValue = "0")
    private long containerId;

    @Option(names = {"--datanode-uuid", "--duuid"},
        paramLabel = "DATANODE_UUID",
        description = "Cleanup containers based on datanode UUID.",
        defaultValue = "")
    private String datanodeUuid;

    @Option(names = {"--datanode-host", "--dhost"},
        paramLabel = "DATANODE_HOST",
        description = "Cleanup containers based on datanode Hostname.",
        defaultValue = "")
    private String datanodeHost;

    @Option(names = {"--pipeline-id", "--pid"},
        paramLabel = "PIPELINE_ID",
        description = "Cleanup containers based on pipeline ID.",
        defaultValue = "")
    private String pipelineId;
  }

  private static final String CONTAINERS_ENDPOINT =
      "/api/v1/containers";
  private static final String MISSING_CONTAINERS_ENDPOINT =
      CONTAINERS_ENDPOINT + "/unhealthy/MISSING";

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    OzoneAdmin ozoneAdmin = new OzoneAdmin();
    conf = ozoneAdmin.getOzoneConf();

    // Get missing container list
    List<MissingContainer> missingContainers =
        getMissingContainersFromRecon();

    if (filterOptions.containerId != 0) {
      // Check if provided containerID belongs to a missing container
      boolean belongsToMissing = checkContainerID(missingContainers,
          filterOptions.containerId);

      if (belongsToMissing) {
        deleteContainerKeys(filterOptions.containerId);

        ContainerInfo containerInfo = scmClient
            .getContainer(filterOptions.containerId);
        containerInfo.setNumberOfKeys(0L);
        containerInfo.setState(HddsProtos.LifeCycleState.DELETED);
      } else {
        LOG.error("Provided ID doesn't belong to a missing container");
      }
    } else if (!Strings.isNullOrEmpty(filterOptions.pipelineId)) {
      // Check if provided pipelineID has any missing containers
      boolean hasMissing = checkPipelineID(missingContainers,
          filterOptions.pipelineId);

      if (hasMissing) {
        for (MissingContainer container : missingContainers) {
          if (container.getPipelineID()
              .equals(filterOptions.pipelineId)) {
            deleteContainerKeys(container.getContainerID());
          }
        }
      } else {
        LOG.error("There are no missing containers " +
            "associated with the provided Pipeline ID");
      }
    } else if (!Strings.isNullOrEmpty(filterOptions.datanodeUuid)) {
      // Check if provided datanode UUID has any missing containers
      boolean hasMissing = checkDatanodeUuid(missingContainers,
          filterOptions.datanodeUuid);

      if (hasMissing) {
        for (MissingContainer container : missingContainers) {
          for (ContainerReplica replica : container.getReplicas()) {
            if (replica.getDatanodeUuid()
                .equals(filterOptions.datanodeUuid)) {
              deleteContainerKeys(container.getContainerID());
            }
          }
        }
      } else {
        LOG.error("There are no missing containers " +
            "associated with the provided Datanode UUID");
      }
    } else {
      // Check if provided datanode Host has any missing containers
      boolean hasMissing = checkDatanodeHost(missingContainers,
          filterOptions.datanodeHost);

      if (hasMissing) {
        for (MissingContainer container : missingContainers) {
          for (ContainerReplica replica : container.getReplicas()) {
            if (replica.getDatanodeHost().equals(filterOptions.datanodeHost)) {
              deleteContainerKeys(container.getContainerID());
            }
          }
        }
      } else {
        LOG.error("There are no missing containers " +
            "associated with the provided Datanode Host");
      }
    }
  }

  private boolean checkContainerID(List<MissingContainer> missingContainers,
                                   long containerID) {
    List<Long> containerIDs = new LinkedList<>();

    for (MissingContainer container : missingContainers) {
      containerIDs.add(container.getContainerID());
    }

    return containerIDs.contains(containerID);
  }

  private boolean checkPipelineID(List<MissingContainer> missingContainers,
                                   String pipelineID) {
    List<String> pipelineIDs = new LinkedList<>();

    for (MissingContainer container : missingContainers) {
      pipelineIDs.add(container.getPipelineID());
    }

    return pipelineIDs.contains(pipelineID);
  }

  private boolean checkDatanodeUuid(List<MissingContainer> missingContainers,
                                   String datanodeUuid) {
    List<String> datanodeUuids = new LinkedList<>();

    for (MissingContainer container : missingContainers) {
      for (ContainerReplica replica : container.getReplicas()) {
        datanodeUuids.add(replica.getDatanodeUuid());
      }
    }
    return datanodeUuids.contains(datanodeUuid);
  }

  private boolean checkDatanodeHost(List<MissingContainer> missingContainers,
                                   String datanodeHost) {
    List<String> datanodeHosts = new LinkedList<>();

    for (MissingContainer container : missingContainers) {
      for (ContainerReplica replica : container.getReplicas()) {
        datanodeHosts.add(replica.getDatanodeHost());
      }
    }

    return datanodeHosts.contains(datanodeHost);
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

  private List<MissingContainer> getMissingContainersFromRecon()
      throws JsonProcessingException {
    StringBuilder urlBuilder = new StringBuilder();
    urlBuilder.append(ReconEndpointUtils.getReconWebAddress(conf))
        .append(MISSING_CONTAINERS_ENDPOINT);
    String missingContainerResponse = "";
    try {
      missingContainerResponse = ReconEndpointUtils.makeHttpCall(urlBuilder,
          ReconEndpointUtils.isHTTPSEnabled(conf), conf);
    } catch (Exception e) {
      LOG.error("Error getting missing container response from Recon");
    }

    if (Strings.isNullOrEmpty(missingContainerResponse)) {
      LOG.info("Missing container response from Recon is empty");
      return new LinkedList<>();
    }

    // Get the containers JSON array
    String containersJsonArray = missingContainerResponse.substring(
        missingContainerResponse.indexOf("containers\":") + 12,
        missingContainerResponse.length() - 1);

    final ObjectMapper objectMapper = new ObjectMapper();

    return objectMapper.readValue(containersJsonArray,
        new TypeReference<List<MissingContainer>>() { });
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

  @Override
  public Class<?> getParentType() {
    return OzoneAdmin.class;
  }
}
