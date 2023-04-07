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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.OzoneAdmin;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdds.scm.cli.container.ContainerCommands.checkContainerExists;
import static org.apache.hadoop.hdds.scm.cli.container.ReconEndpointUtils.getResponseMap;

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

  @Option(names = { "--force" },
      defaultValue = "false",
      description = "Use force to cleanup the container")
  private boolean force;

  @Parameters(description = "Id of the container to cleanup")
  private long containerId;

  private static final String CONTAINERS_ENDPOINT =
      "/api/v1/containers";
  private static final String MISSING_CONTAINERS_ENDPOINT =
      CONTAINERS_ENDPOINT + "/unhealthy/MISSING";

  private final StringBuffer url = new StringBuffer();

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    OzoneConfiguration conf =  new OzoneAdmin().getOzoneConf();
    url.append(ReconEndpointUtils.getReconWebAddress(conf))
        .append(MISSING_CONTAINERS_ENDPOINT);
    String missingContainerResponse = "";
    try {
      missingContainerResponse = ReconEndpointUtils.makeHttpCall(url,
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
    String[] values = missingContainersResponseMap
        .get("containers").toString().split(",");

    for (String s : values) {
      // Get only the lines that contain 'containerID'
      if (s.contains("containerID")) {
        // Split the lines by '='
        String[] ids = s.split("=");
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

    if (missingContainerIDs.contains(containerId)) {

      // Clean the StringBuilder
      url.setLength(0);
      url.append(ReconEndpointUtils.getReconWebAddress(conf))
          .append(CONTAINERS_ENDPOINT)
          .append("/")
          .append(containerId)
          .append("/keys");
      String containerKeysResponse = "";
      try {
        containerKeysResponse = ReconEndpointUtils.makeHttpCall(url,
            ReconEndpointUtils.isHTTPSEnabled(conf), conf);
      } catch (Exception e) {
        LOG.error("Error getting a container keys response from Recon");
      }

      if (Strings.isNullOrEmpty(containerKeysResponse)) {
        LOG.info("Container keys response from Recon is empty");
      }

      HashMap<String, Object> containerKeysResponseMap =
          getResponseMap(containerKeysResponse);
      ReconEndpointUtils.printWithUnderline(containerKeysResponseMap.toString(), true);


//      scmClient.cleanupContainer(containerId, true);
      ReconEndpointUtils.printWithUnderline(missingContainerIDs.toString(), true);
    } else {
      LOG.error("Provided ID doesn't belong to a missing container");
    }
  }

  @Override
  public Class<?> getParentType() {
    return OzoneAdmin.class;
  }
}
