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
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import static org.apache.hadoop.hdds.scm.cli.container.ReconEndpointUtils.getResponseMap;
import static org.apache.hadoop.hdds.scm.cli.container.ReconEndpointUtils.ContainerKeyJson;


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
public class CleanupSubcommand extends ScmSubcommand implements SubcommandWithParent {

  private static final Logger LOG =
      LoggerFactory.getLogger(CleanupSubcommand.class);

  private OzoneConfiguration conf;

  @Parameters(description = "Id of the container to cleanup")
  private long containerId;

  private static final String CONTAINERS_ENDPOINT =
      "/api/v1/containers";
  private static final String MISSING_CONTAINERS_ENDPOINT =
      CONTAINERS_ENDPOINT + "/unhealthy/MISSING";

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    OzoneAdmin ozoneAdmin = new OzoneAdmin();
    conf =  ozoneAdmin.getOzoneConf();

    // Get missing container list
    List<Long> missingContainerIDs =
        getMissingContainersFromRecon();

    // If containerID belongs to a missing container
    if (missingContainerIDs.contains(containerId)) {
      // Get container keys list
      List<ContainerKeyJson> containerKeyList =
          getContainerKeysFromRecon();

      if (containerKeyList.size() > 0) {
        for (ContainerKeyJson info : containerKeyList) {
          try (OzoneClient client = new OzoneClient(conf, new RpcClient(conf, null))) {
            OzoneVolume volume = client.getObjectStore().getVolume(info.getVolume());
            OzoneBucket bucket = volume.getBucket(info.getBucket());
            bucket.deleteKey(info.getKey());
            System.out.println("Successfully deleted key: " +
                "/" + info.getVolume() +
                "/" + info.getBucket() +
                "/" + info.getKey());
          }
        }
      } else {
        LOG.info("Container " + containerId + " has no keys");
      }
    } else {
      LOG.error("Provided ID doesn't belong to a missing container");
    }
  }

  private List<Long> getMissingContainersFromRecon() {
    StringBuilder urlBuilder = new StringBuilder();
    urlBuilder.append(ReconEndpointUtils.getReconWebAddress(conf))
        .append(MISSING_CONTAINERS_ENDPOINT);
    String missingContainerResponse = "";
    try {
      missingContainerResponse = ReconEndpointUtils.makeHttpCall(urlBuilder,
          ReconEndpointUtils.isHTTPSEnabled(conf), conf);
    } catch (Exception e) {
      LOG.error("Error getting a missing container response from Recon");
    }

    if (Strings.isNullOrEmpty(missingContainerResponse)) {
      LOG.info("Missing container response from Recon is empty");
    }

    List<Long> missingContainerIDs = new LinkedList<>();

    HashMap<String, Object> missingContainersResponseMap =
        getResponseMap(missingContainerResponse);

    // Get all the containers and split the values by ','
    String[] containerValues = missingContainersResponseMap
        .get("containers").toString().split(",");

    for (String entry : containerValues) {
      // Get only the lines that contain 'containerID'
      if (entry.contains("containerID")) {
        // Split the lines by '='
        String[] ids = entry.split("=");
        for (String id : ids) {
          // If it doesn't contain 'containerID' then it's the ID
          if (!id.contains("containerID")) {
            double doubleNum = Double.parseDouble(id);
            // Add the ID to the list
            missingContainerIDs.add(Double.valueOf(doubleNum).longValue());
          }
        }
      }
    }

    return missingContainerIDs;
  }

  private List<ContainerKeyJson> getContainerKeysFromRecon()
      throws JsonProcessingException {
    StringBuilder urlBuilder = new StringBuilder();
    urlBuilder.append(ReconEndpointUtils.getReconWebAddress(conf))
        .append(CONTAINERS_ENDPOINT)
        .append("/")
        .append(containerId)
        .append("/keys");
    String containerKeysResponse = "";
    try {
      containerKeysResponse = ReconEndpointUtils.makeHttpCall(urlBuilder,
          ReconEndpointUtils.isHTTPSEnabled(conf), conf);
    } catch (Exception e) {
      LOG.error("Error getting a container keys response from Recon");
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
        new TypeReference<List<ContainerKeyJson>>(){});

  }

  @Override
  public Class<?> getParentType() {
    return OzoneAdmin.class;
  }
}
