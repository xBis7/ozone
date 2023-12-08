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

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmUtils;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.recon.api.types.UnhealthyContainersResponse;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.event.Level;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_RECON_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;

/**
 * Integration tests for ensuring Recon's consistency
 * with the "ozone admin container" CLI.
 */
@Timeout(300)
public class TestReconAndAdminContainerCLI {

  private static final OzoneConfiguration CONF = new OzoneConfiguration();
  private static final String VOLUME = "vol1";
  private static final String BUCKET = "bucket1";
  private static final String KEY = "key1";
  private static ScmClient scmClient;
  private static MiniOzoneCluster cluster;
  private static ContainerManager scmContainerManager;
  private static StorageContainerManager scm;
  private static ReconStorageContainerManagerFacade reconScm;
  private static PipelineManager reconPipelineManager;
  private static PipelineManager scmPipelineManager;
  private static long containerId;

  /**
   * Parameterize all tests to run for Ratis-ONE, Ratis-THREE, EC.
   *
   */
  @BeforeAll
  public static void init()
      throws Exception {
    CONF.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    CONF.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 3, TimeUnit.SECONDS);

    CONF.setTimeDuration(HDDS_RECON_HEARTBEAT_INTERVAL, 3, TimeUnit.SECONDS);

    ReplicationManager.ReplicationManagerConfiguration repConf =
        CONF.getObject(ReplicationManager
                           .ReplicationManagerConfiguration.class);
    repConf.setEnableLegacy(false);
    repConf.setInterval(Duration.ofSeconds(1));
    repConf.setUnderReplicatedInterval(Duration.ofSeconds(1));
    CONF.setFromObject(repConf);

    CONF.set(HDDS_CONTAINER_REPORT_INTERVAL, "3s");
    CONF.set(HDDS_PIPELINE_REPORT_INTERVAL, "3s");
    CONF.set(ScmUtils.getContainerReportConfPrefix() +
             ".queue.wait.threshold", "3");
    CONF.set(ScmUtils.getContainerReportConfPrefix() +
             ".execute.wait.threshold", "3");

    // Find how to make recon health task run more frequently.
    cluster = MiniOzoneCluster.newBuilder(CONF).setNumDatanodes(5)
                  .includeRecon(true).build();
    cluster.waitForClusterToBeReady();
    GenericTestUtils.setLogLevel(ReconNodeManager.LOG, Level.DEBUG);

    // ---


    scmClient = new ContainerOperationClient(CONF);

    reconScm =
        (ReconStorageContainerManagerFacade)
            cluster.getReconServer().getReconStorageContainerManager();
    scm = cluster.getStorageContainerManager();
    reconPipelineManager = reconScm.getPipelineManager();
    scmPipelineManager = scm.getPipelineManager();

    LambdaTestUtils.await(60000, 5000,
        () -> (reconPipelineManager.getPipelines().size() >= 4));

    // Verify if Recon has all the pipelines from SCM.
    scmPipelineManager.getPipelines().forEach(p -> {
      try {
        Assertions.assertNotNull(reconPipelineManager.getPipeline(p.getId()));
      } catch (PipelineNotFoundException e) {
        Assertions.fail();
      }
    });

    scmContainerManager = scm.getContainerManager();
    Assertions.assertTrue(scmContainerManager.getContainers().isEmpty());

    // Verify if all the 3 nodes are registered with Recon.
    NodeManager reconNodeManager = reconScm.getScmNodeManager();
    NodeManager scmNodeManager = scm.getScmNodeManager();
    Assertions.assertEquals(scmNodeManager.getAllNodes().size(),
        reconNodeManager.getAllNodes().size());

    // Create container
    ContainerManager reconContainerManager = reconScm.getContainerManager();

    OmKeyInfo omKeyInfo = createTestKey(cluster.newClient(),
        VOLUME, BUCKET, KEY, THREE);

    List<Long> containerIDs = getContainerIdsForKey(omKeyInfo);
    // The list has only 1 containerID.
    Assertions.assertEquals(1, containerIDs.size());

    containerId = containerIDs.get(0);

    ContainerInfo containerInfo =
        scmContainerManager.getContainer(
            ContainerID.valueOf(containerId));
    long containerID = containerInfo.getContainerID();
    Pipeline pipeline =
        scmPipelineManager.getPipeline(containerInfo.getPipelineID());
    XceiverClientGrpc client = new XceiverClientGrpc(pipeline, CONF);
    //    runTestOzoneContainerViaDataNode(containerID, client);

    // Verify Recon picked up the new container that was created.
    Assertions.assertEquals(scmContainerManager.getContainers(),
        reconContainerManager.getContainers());
  }

  @BeforeEach
  public void setup() throws Exception {
  }

  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Stop all Datanodes to make the container go MISSING.
   * Check recon and RM report.
   * Start all Datanodes and wait for the container to come back.
   * Check recon and RM report again.
   */
  @Test
  public void testMissingContainer() throws Exception {
    ContainerInfo containerInfo =
        scmContainerManager.getContainer(ContainerID.valueOf(containerId));
    long containerID = containerInfo.getContainerID();
    Pipeline pipeline =
        scmPipelineManager.getPipeline(containerInfo.getPipelineID());
    XceiverClientGrpc client = new XceiverClientGrpc(pipeline, CONF);


    for (DatanodeDetails details : pipeline.getNodes()) {
      cluster.shutdownHddsDatanode(details);
//      scmDeadNodeHandler.onMessage();
      System.out.println("xbis-dn-uuid: " + details.getUuid());
    }
    TestHelper.waitForReplicaCount(containerID, 0, cluster);

//    reconScm.getContainerManager().
//    containerHealthTask.triggerContainerHealthCheck();

    UnhealthyContainersResponse response =
        TestReconEndpointUtil
            .getUnhealthyContainersFromRecon(CONF, "MISSING");


    String msg = "xbis-recon-report: " +
                 "\n|missing: " +
                 response.getMissingCount() +
                 "\n|under-repl: " +
                 response.getUnderReplicatedCount() +
                 "\n|over-repl: " +
                 response.getOverReplicatedCount() +
                 "\n|containers: " +
                 response.getContainers().toString();
    System.out.println(msg);

    long start = System.currentTimeMillis();
    GenericTestUtils.waitFor(() -> {
      try {
        System.out.println("checking");
        return scmClient.getReplicationManagerReport()
                   .getStat(ReplicationManagerReport.HealthState.MISSING) == 1;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 1000, 20000);
    System.out.println("Time-after: " + (System.currentTimeMillis() - start) +
                       " ms / " + (System.currentTimeMillis() - start) / 1000 +
                       " secs");

    Assertions.assertEquals(0,
        scmClient.getContainerReplicas(containerInfo.getContainerID()).size());
    response =
        TestReconEndpointUtil
            .getUnhealthyContainersFromRecon(CONF, "MISSING");

    ReplicationManagerReport report = scmClient.getReplicationManagerReport();
    String msg2 = "xbis-recon-report: " +
                     "\n|missing: " +
                     response.getMissingCount() +
                     "\n|under-repl: " +
                     response.getUnderReplicatedCount() +
                     "\n|over-repl: " +
                     response.getOverReplicatedCount() +
                     "\n|containers: " +
                     response.getContainers().toString();
    System.out.println(msg2);

    long reconStart = System.currentTimeMillis();
//    Thread.sleep(10000);
//    GenericTestUtils.waitFor(() -> {
//      try {
//        return TestReconEndpointUtil
//            .getUnhealthyContainersFromRecon(CONF, "MISSING")
//                   .getMissingCount() == 1;
//      } catch (JsonProcessingException e) {
//        throw new RuntimeException(e);
//      }
//    }, 1000, 20000);
    System.out.println("Time-after-recon: " +
                       (System.currentTimeMillis() - reconStart) + " ms / " +
                       (System.currentTimeMillis() - reconStart) / 1000 +
                       " secs");

    System.out.println("xbis-recon: missing: " + response.getMissingCount() +
                       " | under-repl: " + response.getUnderReplicatedCount());

    report = scmClient.getReplicationManagerReport();
    System.out.println(
        "xbis-scm: missing: " +
        report.getStat(ReplicationManagerReport.HealthState.MISSING) +
        " | under-repl: " +
        report.getStat(ReplicationManagerReport.HealthState.UNDER_REPLICATED));
  }

  private static OmKeyInfo createTestKey(OzoneClient client, String volumeName,
      String bucketName, String keyName, HddsProtos.ReplicationFactor factor)
      throws IOException {
    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume(VOLUME);
    OzoneVolume volume = objectStore.getVolume(VOLUME);
    volume.createBucket(BUCKET);

    OzoneBucket bucket = volume.getBucket(BUCKET);

    try (OutputStream out = bucket.createKey(KEY, 0,
        RatisReplicationConfig.getInstance(factor), emptyMap())) {
      out.write("Testing".getBytes(UTF_8));
    }

    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
                            .setVolumeName(volumeName)
                            .setBucketName(bucketName)
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
}
