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
package org.apache.hadoop.ozone.om;


import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.OzoneAdmin;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.admin.om.ContainerCleanupSubCommand;
import org.apache.hadoop.ozone.admin.om.OMAdmin;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CleanupContainerResponse;
import org.apache.hadoop.ozone.shell.Shell;
import org.apache.hadoop.util.ToolRunner;
import org.apache.ozone.test.GenericTestUtils;
import org.checkerframework.checker.units.qual.C;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODES_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODE_ID_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;
import static org.mockito.Mockito.when;

/**
 * Unit tests for
 * {@link org.apache.hadoop.ozone.admin.om.ContainerCleanupSubCommand}
 */
public class TestContainerCleanupSubCommand {

  private OzoneAdmin ozoneAdmin;
  private OMAdmin omAdmin;
  private static OzoneManagerProtocol omClient;
//  private String[] args;
  private final static long CONTAINER_ID =
      ThreadLocalRandom.current().nextLong(100);

  @BeforeEach
  public void setUp() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_OM_SERVICE_IDS_KEY, "omservice");
    conf.set(OZONE_OM_NODES_KEY + ".omservice", "om1,om2,om3");
    conf.set(OZONE_OM_ADDRESS_KEY + ".omservice.om1", "om1");
    conf.set(OZONE_OM_ADDRESS_KEY + ".omservice.om2", "om2");
    conf.set(OZONE_OM_ADDRESS_KEY + ".omservice.om3", "om3");

    ozoneAdmin = new OzoneAdmin(conf);
//    ozoneAdmin = Mockito.spy(OzoneAdmin.class);
//    when(ozoneAdmin.getOzoneConf()).thenReturn(conf);
//    when(ozoneAdmin.createOzoneConfiguration()).thenReturn(conf);

//    when(ozoneAdmin.getOzoneConf()).thenReturn(new OzoneConfiguration());

    omClient = Mockito
        .mock(OzoneManagerProtocol.class);
//    args = {"om", "containerCleanup", "-cid=" + CONTAINER_ID};
  }

  @Test
  public void testCommandSuccess() throws Exception {
    when(omClient.cleanupContainer(CONTAINER_ID))
        .thenReturn(CleanupContainerResponse
            .StatusType.SUCCESS);

    String[] args = {"om", "containerCleanup", "-cid=" + CONTAINER_ID};

    String output = execShellCommandAndGetOutput(args);
    Assertions.assertTrue(output
        .contains("successfully submitted for cleanup"));
  }

  @Test
  public void testCleanupNotExistingContainer() throws Exception {
    when(omClient.cleanupContainer(CONTAINER_ID))
        .thenReturn(CleanupContainerResponse
            .StatusType.CONTAINER_NOT_FOUND_IN_SCM);
    String[] args = {"om", "containerCleanup", "-id=omservice",
        "-host=om1", "-cid=" + CONTAINER_ID};
    String output = execShellCommandAndGetOutput(args);
    Assertions.assertTrue(output
        .contains("doesn't exist"));
  }

  @Test
  public void testContainerIsNotMissing() throws Exception {
    when(omClient.cleanupContainer(CONTAINER_ID))
        .thenReturn(CleanupContainerResponse
            .StatusType.CONTAINER_IS_NOT_MISSING);
//    String[] args = {"om", "containerCleanup", "-cid=" + CONTAINER_ID};
//    String[] args = {"containerCleanup", "-cid=" + CONTAINER_ID};
    String[] args = {"-cid=" + CONTAINER_ID};
    String output = execShellCommandAndGetOutput(args);
    System.out.println(output);
    Assertions.assertTrue(output
        .contains("not missing"));
  }

  private String execShellCommandAndGetOutput(String[] args)
      throws Exception {
    ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();

    // Setup output streams
    System.setOut(new PrintStream(
        outputBytes, false, StandardCharsets.UTF_8.name()));

    // Execute command
    ozoneAdmin.execute(args);

    String output = outputBytes.toString(StandardCharsets.UTF_8.name());

    // Flush byte array stream
    outputBytes.flush();

    // Restore output stream
    System.setOut(new PrintStream(
        outputBytes, false, StandardCharsets.UTF_8.name()));

    return output;
  }
}
