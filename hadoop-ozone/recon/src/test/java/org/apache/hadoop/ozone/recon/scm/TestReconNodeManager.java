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

package org.apache.hadoop.ozone.recon.scm;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.ozone.container.upgrade.UpgradeUtils.defaultLayoutVersionProto;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.protocol.commands.ReregisterCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.protocol.commands.SetNodeOperationalStateCommand;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for Recon Node Manager.
 */
public class TestReconNodeManager {

  @TempDir
  private Path temporaryFolder;

  private OzoneConfiguration conf;
  private DBStore store;
  private ReconStorageConfig reconStorageConfig;
  private HDDSLayoutVersionManager versionManager;

  @BeforeEach
  public void setUp() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS, temporaryFolder.toAbsolutePath().toString());
    conf.set(OZONE_SCM_NAMES, "localhost");
    reconStorageConfig = new ReconStorageConfig(conf, new ReconUtils());
    versionManager = new HDDSLayoutVersionManager(
        reconStorageConfig.getLayoutVersion());
    store = DBStoreBuilder.createDBStore(conf, new ReconSCMDBDefinition());
  }

  @AfterEach
  public void tearDown() throws Exception {
    store.close();
  }

  @Test
  public void testReconNodeDB() throws IOException, NodeNotFoundException {
    ReconStorageConfig scmStorageConfig =
        new ReconStorageConfig(conf, new ReconUtils());
    EventQueue eventQueue = new EventQueue();
    NetworkTopology clusterMap = new NetworkTopologyImpl(conf);
    Table<UUID, DatanodeDetails> nodeTable =
        ReconSCMDBDefinition.NODES.getTable(store);
    ReconNodeManager reconNodeManager = new ReconNodeManager(conf,
        scmStorageConfig, eventQueue, clusterMap, nodeTable, versionManager);
    ReconNewNodeHandler reconNewNodeHandler =
        new ReconNewNodeHandler(reconNodeManager);
    assertThat(reconNodeManager.getAllNodes()).isEmpty();

    DatanodeDetails datanodeDetails = randomDatanodeDetails();
    String uuidString = datanodeDetails.getUuidString();

    // Register a random datanode.
    reconNodeManager.register(datanodeDetails, null, null);
    reconNewNodeHandler.onMessage(reconNodeManager.getNodeByUuid(uuidString),
        null);

    assertEquals(1, reconNodeManager.getAllNodes().size());
    assertNotNull(reconNodeManager.getNodeByUuid(uuidString));

    // If any commands are added to the eventQueue without using the onMessage
    // interface, then they should be filtered out and not returned to the DN
    // when it heartbeats.
    // This command should never be returned by Recon
    reconNodeManager.addDatanodeCommand(datanodeDetails.getUuid(),
        new SetNodeOperationalStateCommand(1234,
        DECOMMISSIONING, 0));

    // This one should be returned
    reconNodeManager.addDatanodeCommand(datanodeDetails.getUuid(),
        new ReregisterCommand());

    // OperationalState sanity check
    final DatanodeDetails dnDetails =
        reconNodeManager.getNodeByUuid(datanodeDetails.getUuidString());
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        dnDetails.getPersistedOpState());
    assertEquals(dnDetails.getPersistedOpState(),
        reconNodeManager.getNodeStatus(dnDetails)
            .getOperationalState());
    assertEquals(dnDetails.getPersistedOpStateExpiryEpochSec(),
        reconNodeManager.getNodeStatus(dnDetails)
            .getOpStateExpiryEpochSeconds());

    // Upon processing the heartbeat, the illegal command should be filtered out
    List<SCMCommand> returnedCmds =
        reconNodeManager.processHeartbeat(datanodeDetails,
            defaultLayoutVersionProto());
    assertEquals(1, returnedCmds.size());
    assertEquals(SCMCommandProto.Type.reregisterCommand,
        returnedCmds.get(0).getType());

    // Now feed a DECOMMISSIONED heartbeat of the same DN
    datanodeDetails.setPersistedOpState(
        HddsProtos.NodeOperationalState.DECOMMISSIONED);
    datanodeDetails.setPersistedOpStateExpiryEpochSec(12345L);
    reconNodeManager.processHeartbeat(datanodeDetails,
        defaultLayoutVersionProto());
    // Check both persistedOpState and NodeStatus#operationalState
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONED,
        dnDetails.getPersistedOpState());
    assertEquals(dnDetails.getPersistedOpState(),
        reconNodeManager.getNodeStatus(dnDetails)
            .getOperationalState());
    assertEquals(12345L, dnDetails.getPersistedOpStateExpiryEpochSec());
    assertEquals(dnDetails.getPersistedOpStateExpiryEpochSec(),
        reconNodeManager.getNodeStatus(dnDetails)
            .getOpStateExpiryEpochSeconds());

    // Close the DB, and recreate the instance of Recon Node Manager.
    eventQueue.close();
    reconNodeManager.close();
    reconNodeManager = new ReconNodeManager(conf, scmStorageConfig, eventQueue,
        clusterMap, nodeTable, versionManager);

    // Verify that the node information was persisted and loaded back.
    assertEquals(1, reconNodeManager.getAllNodes().size());
    assertNotNull(
        reconNodeManager.getNodeByUuid(datanodeDetails.getUuidString()));
  }

  @Test
  public void testUpdateNodeOperationalStateFromScm() throws Exception {
    ReconStorageConfig scmStorageConfig =
        new ReconStorageConfig(conf, new ReconUtils());
    EventQueue eventQueue = new EventQueue();
    NetworkTopology clusterMap = new NetworkTopologyImpl(conf);
    Table<UUID, DatanodeDetails> nodeTable =
        ReconSCMDBDefinition.NODES.getTable(store);
    ReconNodeManager reconNodeManager = new ReconNodeManager(conf,
        scmStorageConfig, eventQueue, clusterMap, nodeTable, versionManager);


    DatanodeDetails datanodeDetails = randomDatanodeDetails();
    HddsProtos.Node node = mock(HddsProtos.Node.class);

    assertThrows(NodeNotFoundException.class,
        () -> reconNodeManager
            .updateNodeOperationalStateFromScm(node, datanodeDetails));

    reconNodeManager.register(datanodeDetails, null, null);
    assertEquals(IN_SERVICE, reconNodeManager
        .getNodeByUuid(datanodeDetails.getUuidString()).getPersistedOpState());

    when(node.getNodeOperationalStates(eq(0)))
        .thenReturn(DECOMMISSIONING);
    reconNodeManager.updateNodeOperationalStateFromScm(node, datanodeDetails);
    assertEquals(DECOMMISSIONING, reconNodeManager
        .getNodeByUuid(datanodeDetails.getUuidString()).getPersistedOpState());
    List<DatanodeDetails> nodes =
        reconNodeManager.getNodes(DECOMMISSIONING, null);
    assertEquals(1, nodes.size());
    assertEquals(datanodeDetails.getUuid(), nodes.get(0).getUuid());
  }

  @Test
  public void testDatanodeUpdate() throws IOException {
    ReconStorageConfig scmStorageConfig =
        new ReconStorageConfig(conf, new ReconUtils());
    EventQueue eventQueue = new EventQueue();
    NetworkTopology clusterMap = new NetworkTopologyImpl(conf);
    Table<UUID, DatanodeDetails> nodeTable =
        ReconSCMDBDefinition.NODES.getTable(store);
    ReconNodeManager reconNodeManager = new ReconNodeManager(conf,
        scmStorageConfig, eventQueue, clusterMap, nodeTable, versionManager);
    ReconNewNodeHandler reconNewNodeHandler =
        new ReconNewNodeHandler(reconNodeManager);
    assertThat(reconNodeManager.getAllNodes()).isEmpty();

    DatanodeDetails datanodeDetails = randomDatanodeDetails();
    datanodeDetails.setHostName("hostname1");
    String uuidString = datanodeDetails.getUuidString();

    // Register "hostname1" datanode.
    reconNodeManager.register(datanodeDetails, null, null);
    reconNewNodeHandler.onMessage(reconNodeManager.getNodeByUuid(uuidString),
        null);

    assertEquals(1, reconNodeManager.getAllNodes().size());
    assertNotNull(reconNodeManager.getNodeByUuid(uuidString));
    assertEquals("hostname1",
        reconNodeManager.getNodeByUuid(uuidString).getHostName());

    datanodeDetails.setHostName("hostname2");
    // Upon processing the heartbeat, the illegal command should be filtered out
    List<SCMCommand> returnedCmds =
        reconNodeManager.processHeartbeat(datanodeDetails,
            defaultLayoutVersionProto());
    assertEquals(1, returnedCmds.size());
    assertEquals(SCMCommandProto.Type.reregisterCommand,
        returnedCmds.get(0).getType());

  }
}
