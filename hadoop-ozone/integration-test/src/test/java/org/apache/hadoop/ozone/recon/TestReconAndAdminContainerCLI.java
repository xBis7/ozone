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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.recon;

import com.google.common.base.Strings;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ScmUtils;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport.HealthState;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.recon.api.types.UnhealthyContainerMetadata;
import org.apache.hadoop.ozone.recon.api.types.UnhealthyContainersResponse;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskConfig;
import org.apache.hadoop.ozone.scm.node.TestNodeUtil;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.hadoop.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.event.Level;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_RECON_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_TASK_INTERVAL_DELAY;

/**
 * Integration tests for ensuring Recon's consistency
 * with the "ozone admin container" CLI.
 */
@Timeout(300)
public class TestReconAndAdminContainerCLI {

  private static final OzoneConfiguration CONF = new OzoneConfiguration();
  private static final String VOLUME = "vol1";
  private static final String BUCKET = "bucket1";
  private static final String KEY_RATIS_ONE = "key1";
  private static final String KEY_RATIS_THREE = "key2";
  private static ScmClient scmClient;
  private static MiniOzoneCluster cluster;
  private static NodeManager scmNodeManager;
  private static long containerIdR1;
  private static long containerIdR3;

  private static Stream<Arguments> outOfServiceNodeStateArgs() {
    return Stream.of(
        Arguments.of(NodeOperationalState.ENTERING_MAINTENANCE,
            NodeOperationalState.IN_MAINTENANCE, true),
        Arguments.of(NodeOperationalState.DECOMMISSIONING,
            NodeOperationalState.DECOMMISSIONED, false)
    );
  }

  @BeforeAll
  public static void init() throws Exception {
    setupConfigKeys();
    cluster = MiniOzoneCluster.newBuilder(CONF)
                  .setNumDatanodes(5)
                  .includeRecon(true)
                  .build();
    cluster.waitForClusterToBeReady();
    GenericTestUtils.setLogLevel(ReconNodeManager.LOG, Level.DEBUG);

    scmClient = new ContainerOperationClient(CONF);
    StorageContainerManager scm = cluster.getStorageContainerManager();
    PipelineManager scmPipelineManager = scm.getPipelineManager();
    ContainerManager scmContainerManager = scm.getContainerManager();
    scmNodeManager = scm.getScmNodeManager();

    ReconStorageContainerManagerFacade reconScm =
        (ReconStorageContainerManagerFacade)
            cluster.getReconServer().getReconStorageContainerManager();
    PipelineManager reconPipelineManager = reconScm.getPipelineManager();

    LambdaTestUtils.await(60000, 5000,
        () -> (reconPipelineManager.getPipelines().size() >= 4));

    // Verify that Recon has all the pipelines from SCM.
    scmPipelineManager.getPipelines().forEach(p -> {
      try {
        Assertions.assertNotNull(reconPipelineManager.getPipeline(p.getId()));
      } catch (PipelineNotFoundException e) {
        Assertions.fail();
      }
    });

    Assertions.assertTrue(scmContainerManager.getContainers().isEmpty());

    // Verify that all nodes are registered with Recon.
    NodeManager reconNodeManager = reconScm.getScmNodeManager();
    Assertions.assertEquals(scmNodeManager.getAllNodes().size(),
        reconNodeManager.getAllNodes().size());

    ContainerManager reconContainerManager = reconScm.getContainerManager();

    OzoneClient client = cluster.newClient();
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(
        client, VOLUME, BUCKET, BucketLayout.FILE_SYSTEM_OPTIMIZED);

    // Create keys and containers.
    OmKeyInfo omKeyInfoR1 = createTestKey(bucket, KEY_RATIS_ONE,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE));
    OmKeyInfo omKeyInfoR3 = createTestKey(bucket, KEY_RATIS_THREE,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));

    // Sync Recon with OM, to force it to get the new key entries.
    TestReconEndpointUtil.triggerReconDbSyncWithOm(CONF);

    List<Long> containerIDsR1 = getContainerIdsForKey(omKeyInfoR1);
    // The list has only 1 containerID.
    Assertions.assertEquals(1, containerIDsR1.size());
    containerIdR1 = containerIDsR1.get(0);

    List<Long> containerIDsR3 = getContainerIdsForKey(omKeyInfoR3);
    // The list has only 1 containerID.
    Assertions.assertEquals(1, containerIDsR3.size());
    containerIdR3 = containerIDsR3.get(0);

    // Verify Recon picked up the new containers.
    Assertions.assertEquals(scmContainerManager.getContainers(),
        reconContainerManager.getContainers());

    ReconContainerMetadataManager reconContainerMetadataManager =
        cluster.getReconServer().getReconContainerMetadataManager();

    // Verify Recon picked up the new keys and
    // updated its container key mappings.
    GenericTestUtils.waitFor(() -> {
      try {
        return reconContainerMetadataManager
                   .getKeyCountForContainer(containerIdR1) > 0 &&
               reconContainerMetadataManager
                   .getKeyCountForContainer(containerIdR3) > 0;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 1000, 20000);
  }

  @AfterAll
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * It's the same regardless of the ReplicationConfig,
   * but it's easier to test with Ratis ONE.
   */
  @Test
  public void testMissingContainer() throws Exception {
    Pipeline pipeline =
        scmClient.getContainerWithPipeline(containerIdR1).getPipeline();

    for (DatanodeDetails details : pipeline.getNodes()) {
      cluster.shutdownHddsDatanode(details);
    }
    TestHelper.waitForReplicaCount(containerIdR1, 0, cluster);

    GenericTestUtils.waitFor(() -> {
      try {
        return scmClient.getReplicationManagerReport()
                   .getStat(ReplicationManagerReport.HealthState.MISSING) == 1;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 1000, 20000);

    UnHealthyContainerStates containerStateForTesting =
        UnHealthyContainerStates.MISSING;
    long currentTimeStamp = System.currentTimeMillis();
    compareRMReportToReconResponse(
        containerStateForTesting.toString(), currentTimeStamp);
  }

  @ParameterizedTest
  @MethodSource("outOfServiceNodeStateArgs")
  public void testNodesInDecommissionOrMaintenance(
      NodeOperationalState initialState, NodeOperationalState finalState,
      boolean isMaintenance) throws Exception {
    Pipeline pipeline =
        scmClient.getContainerWithPipeline(containerIdR3).getPipeline();

    List<DatanodeDetails> details =
        pipeline.getNodes().stream()
            .filter(d -> d.getPersistedOpState().equals(IN_SERVICE))
            .collect(Collectors.toList());

    final DatanodeDetails nodeToGoOffline1 = details.get(0);
    final DatanodeDetails nodeToGoOffline2 = details.get(1);

    UnHealthyContainerStates underReplicatedState =
        UnHealthyContainerStates.UNDER_REPLICATED;
    UnHealthyContainerStates overReplicatedState =
        UnHealthyContainerStates.OVER_REPLICATED;

    // First node goes offline.
    if (isMaintenance) {
      scmClient.startMaintenanceNodes(Collections.singletonList(
          TestNodeUtil.getDNHostAndPort(nodeToGoOffline1)), 0);
    } else {
      scmClient.decommissionNodes(Collections.singletonList(
          TestNodeUtil.getDNHostAndPort(nodeToGoOffline1)));
    }

    TestNodeUtil.waitForDnToReachOpState(scmNodeManager,
        nodeToGoOffline1, initialState);

    long currentTimeStamp = System.currentTimeMillis();
    compareRMReportToReconResponse(
        underReplicatedState.toString(), currentTimeStamp);
    compareRMReportToReconResponse(
        overReplicatedState.toString(), currentTimeStamp);

    TestNodeUtil.waitForDnToReachOpState(scmNodeManager,
        nodeToGoOffline1, finalState);
    // Every time a node goes into decommission,
    // a new replica-copy is made to another node.
    // For maintenance, there is no replica-copy in this case.
    if (!isMaintenance) {
      TestHelper.waitForReplicaCount(containerIdR3, 4, cluster);
    }

    currentTimeStamp = System.currentTimeMillis();
    compareRMReportToReconResponse(
        underReplicatedState.toString(), currentTimeStamp);
    compareRMReportToReconResponse(
        overReplicatedState.toString(), currentTimeStamp);

    // Second node goes offline.
    if (isMaintenance) {
      scmClient.startMaintenanceNodes(Collections.singletonList(
          TestNodeUtil.getDNHostAndPort(nodeToGoOffline2)), 0);
    } else {
      scmClient.decommissionNodes(Collections.singletonList(
          TestNodeUtil.getDNHostAndPort(nodeToGoOffline2)));
    }

    TestNodeUtil.waitForDnToReachOpState(scmNodeManager,
        nodeToGoOffline2, initialState);

    currentTimeStamp = System.currentTimeMillis();
    compareRMReportToReconResponse(
        underReplicatedState.toString(), currentTimeStamp);
    compareRMReportToReconResponse(
        overReplicatedState.toString(), currentTimeStamp);

    TestNodeUtil.waitForDnToReachOpState(scmNodeManager,
        nodeToGoOffline2, finalState);

    // There will be a replica copy for both maintenance and decommission.
    // maintenance 3 -> 4, decommission 4 -> 5.
    int expectedReplicaNum = isMaintenance ? 4 : 5;
    TestHelper.waitForReplicaCount(containerIdR3, expectedReplicaNum, cluster);

    currentTimeStamp = System.currentTimeMillis();
    compareRMReportToReconResponse(
        underReplicatedState.toString(), currentTimeStamp);
    compareRMReportToReconResponse(
        overReplicatedState.toString(), currentTimeStamp);

    scmClient.recommissionNodes(Arrays.asList(
        TestNodeUtil.getDNHostAndPort(nodeToGoOffline1),
        TestNodeUtil.getDNHostAndPort(nodeToGoOffline2)));

    TestNodeUtil.waitForDnToReachOpState(scmNodeManager,
        nodeToGoOffline1, IN_SERVICE);
    TestNodeUtil.waitForDnToReachOpState(scmNodeManager,
        nodeToGoOffline2, IN_SERVICE);

    TestNodeUtil.waitForDnToReachPersistedOpState(
        nodeToGoOffline1, IN_SERVICE);
    TestNodeUtil.waitForDnToReachPersistedOpState(
        nodeToGoOffline2, IN_SERVICE);

    currentTimeStamp = System.currentTimeMillis();
    compareRMReportToReconResponse(
        underReplicatedState.toString(), currentTimeStamp);
    compareRMReportToReconResponse(
        overReplicatedState.toString(), currentTimeStamp);
  }

  /**
   * The purpose of this method, isn't to validate the numbers
   * but to make sure that they are consistent between
   * Recon and the ReplicationManager.
   */
  private static void compareRMReportToReconResponse(
      String containerState, long currentTimeStamp)
      throws IOException, InterruptedException, TimeoutException {
    Assertions.assertFalse(Strings.isNullOrEmpty(containerState));

    // Thread runs every 1 second. 10000 millis is more than enough.
    GenericTestUtils.waitFor(
        () -> {
          try {
            return scmClient.getReplicationManagerReport()
                       .getReportTimeStamp() >= currentTimeStamp;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        },
        100, 10000);

    ReplicationManagerReport rmReport = scmClient.getReplicationManagerReport();
    System.out.println("xbis: currentTimeStamp: " +
                       currentTimeStamp +
                       " | rmReport TimeStamp: " +
                       rmReport.getReportTimeStamp());
    UnhealthyContainersResponse reconResponse =
        TestReconEndpointUtil
            .getUnhealthyContainersFromRecon(CONF, containerState);

    long rmMissingCounter = rmReport.getStat(
        ReplicationManagerReport.HealthState.MISSING);
    long rmUnderReplCounter = rmReport.getStat(
        ReplicationManagerReport.HealthState.UNDER_REPLICATED);
    long rmOverReplCounter = rmReport.getStat(
        ReplicationManagerReport.HealthState.OVER_REPLICATED);
    long rmMisReplCounter = rmReport.getStat(
        ReplicationManagerReport.HealthState.MIS_REPLICATED);

    // Check that all counters have the same values.
    Assertions.assertEquals(rmMissingCounter,
        reconResponse.getMissingCount());
    System.out.println("xbis: compare: " + containerState +
                       " | rmMissingCounter: " + rmMissingCounter +
                       " | recon: " + reconResponse.getMissingCount());
    Assertions.assertEquals(rmUnderReplCounter,
        reconResponse.getUnderReplicatedCount());
    System.out.println("xbis: compare: " + containerState +
                       " | rmUnderReplCounter: " + rmUnderReplCounter +
                       " | recon: " + reconResponse.getUnderReplicatedCount());
    Assertions.assertEquals(rmOverReplCounter,
        reconResponse.getOverReplicatedCount());
    System.out.println("xbis: compare: " + containerState +
                       " | rmOverReplCounter: " + rmOverReplCounter +
                       " | recon: " + reconResponse.getOverReplicatedCount());
    Assertions.assertEquals(rmMisReplCounter,
        reconResponse.getMisReplicatedCount());
    System.out.println("xbis: compare: " + containerState +
                       " | rmMisReplCounter: " + rmMisReplCounter +
                       " | recon: " + reconResponse.getMisReplicatedCount());

    // Recon's UnhealthyContainerResponse contains a list of containers
    // for a particular state. Check if RMs sample of containers can be
    // found in Recon's list of containers for a particular state.
    HealthState rmState = HealthState.UNHEALTHY;

    if (UnHealthyContainerStates.valueOf(containerState)
            .equals(UnHealthyContainerStates.MISSING) &&
        rmMissingCounter > 0) {
      rmState = HealthState.MISSING;
    } else if (UnHealthyContainerStates.valueOf(containerState)
            .equals(UnHealthyContainerStates.UNDER_REPLICATED) &&
        rmUnderReplCounter > 0) {
      rmState = HealthState.UNDER_REPLICATED;
    } else if (UnHealthyContainerStates.valueOf(containerState)
            .equals(UnHealthyContainerStates.OVER_REPLICATED) &&
        rmOverReplCounter > 0) {
      rmState = HealthState.OVER_REPLICATED;
    } else if (UnHealthyContainerStates.valueOf(containerState)
            .equals(UnHealthyContainerStates.MIS_REPLICATED) &&
        rmMisReplCounter > 0) {
      rmState = HealthState.MIS_REPLICATED;
    }

    List<ContainerID> rmContainerIDs = rmReport.getSample(rmState);
    List<Long> rmIDsToLong = new ArrayList<>();
    for (ContainerID id : rmContainerIDs) {
      rmIDsToLong.add(id.getId());
    }
    List<Long> reconContainerIDs =
        reconResponse.getContainers()
            .stream()
            .map(UnhealthyContainerMetadata::getContainerID)
            .collect(Collectors.toList());
    Assertions.assertTrue(reconContainerIDs.containsAll(rmIDsToLong));
  }

  private static OmKeyInfo createTestKey(OzoneBucket ozoneBucket,
      String keyName, ReplicationConfig replicationConfig)
      throws IOException {
    byte[] textBytes = "Testing".getBytes(UTF_8);
    try (OutputStream out = ozoneBucket.createKey(keyName,
        textBytes.length, replicationConfig, emptyMap())) {
      out.write(textBytes);
    }

    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
                            .setVolumeName(ozoneBucket.getVolumeName())
                            .setBucketName(ozoneBucket.getName())
                            .setKeyName(keyName)
                            .build();
    return cluster.getOzoneManager().lookupKey(keyArgs);
  }

  private static List<Long> getContainerIdsForKey(OmKeyInfo omKeyInfo) {
    Assertions.assertNotNull(omKeyInfo.getLatestVersionLocations());
    List<OmKeyLocationInfo> locations =
        omKeyInfo.getLatestVersionLocations().getLocationList();

    List<Long> ids = new ArrayList<>();
    for (OmKeyLocationInfo location : locations) {
      ids.add(location.getContainerID());
    }
    return ids;
  }

  private static void setupConfigKeys() {
    CONF.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    CONF.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    CONF.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 1);
    CONF.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1, SECONDS);
    CONF.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 1, SECONDS);
    CONF.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 1, SECONDS);
    CONF.setTimeDuration(HDDS_NODE_REPORT_INTERVAL, 1, SECONDS);
    CONF.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, SECONDS);
    CONF.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, SECONDS);
    CONF.setTimeDuration(OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL,
        1, SECONDS);
    CONF.setTimeDuration(
        ScmConfigKeys.OZONE_SCM_EXPIRED_CONTAINER_REPLICA_OP_SCRUB_INTERVAL,
        1, SECONDS);
    CONF.setTimeDuration(HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        0, SECONDS);
    CONF.set(OzoneConfigKeys.OZONE_SCM_CLOSE_CONTAINER_WAIT_DURATION, "2s");
    CONF.set(ScmConfigKeys.OZONE_SCM_PIPELINE_SCRUB_INTERVAL, "2s");
    CONF.set(ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT, "5s");

    CONF.setTimeDuration(HDDS_RECON_HEARTBEAT_INTERVAL,
        1, TimeUnit.SECONDS);
    CONF.setTimeDuration(OZONE_RECON_OM_SNAPSHOT_TASK_INTERVAL_DELAY,
        1, TimeUnit.SECONDS);

    CONF.set(ScmUtils.getContainerReportConfPrefix() +
             ".queue.wait.threshold", "1");
    CONF.set(ScmUtils.getContainerReportConfPrefix() +
             ".execute.wait.threshold", "1");

    ReconTaskConfig reconTaskConfig = CONF.getObject(ReconTaskConfig.class);
    reconTaskConfig.setMissingContainerTaskInterval(Duration.ofSeconds(1));
    CONF.setFromObject(reconTaskConfig);

    ReplicationManager.ReplicationManagerConfiguration replicationConf =
        CONF.getObject(ReplicationManager
                           .ReplicationManagerConfiguration.class);
    replicationConf.setInterval(Duration.ofSeconds(1));
    replicationConf.setUnderReplicatedInterval(Duration.ofSeconds(1));
    replicationConf.setOverReplicatedInterval(Duration.ofSeconds(1));
    CONF.setFromObject(replicationConf);
  }
}
