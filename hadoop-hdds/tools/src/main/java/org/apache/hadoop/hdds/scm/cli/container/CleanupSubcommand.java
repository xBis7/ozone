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

import com.google.common.base.Strings;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.OzoneAdmin;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.Callable;

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
public class CleanupSubcommand extends ScmSubcommand {

  private static final Logger LOG =
      LoggerFactory.getLogger(CleanupSubcommand.class);
  @ParentCommand
  private OzoneAdmin parent;

  @Option(names = { "--force" },
      defaultValue = "false",
      description = "Use force to cleanup the container")
  private boolean force;

  @Parameters(description = "Id of the container to cleanup")
  private long containerId;

  private static final String UNHEALTHY_CONTAINERS_ENDPOINT =
      "/api/v1/containers/unhealthy";
  private static final String MISSING_CONTAINERS_ENDPOINT =
      UNHEALTHY_CONTAINERS_ENDPOINT + "/MISSING";

  private StringBuffer url = new StringBuffer();

  @Override
  public void execute(ScmClient scmClient) throws IOException {
//    checkContainerExists(scmClient, containerId);
//    scmClient.cleanupContainer(containerId, force);
    OzoneConfiguration conf = parent.getOzoneConf();
    url.append(ReconEndpointUtils.getReconWebAddress(conf)).append(MISSING_CONTAINERS_ENDPOINT);
    String response = "";
    try {
      response = ReconEndpointUtils.makeHttpCall(url,
          ReconEndpointUtils.isHTTPSEnabled(conf), conf);
    } catch (Exception e) {
      LOG.error("Error getting a response from Recon");
    }

    if (Strings.isNullOrEmpty(response)) {
      LOG.info("No response from Recon");
    }

    HashMap<String, Object> responseMap = getResponseMap(response);

  }
}
