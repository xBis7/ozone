/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.scm.cli.container;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import picocli.CommandLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;

/**
 * Tests for CleanupSubCommand class.
 */
public class TestCleanupSubCommand {


  private ScmClient scmClient;
  private CleanupSubcommand cmd;
  private List<DatanodeDetails> datanodes;
  private Logger logger;
  private TestAppender appender;

  @BeforeEach
  public void setup() throws IOException {
    scmClient = mock(ScmClient.class);
    datanodes = createDatanodeDetails(3);
    Mockito.when(scmClient.getContainerWithPipeline(anyLong()))
        .thenReturn(getContainerWithPipeline());

    appender = new TestAppender();
    logger = Logger.getLogger(
        org.apache.hadoop.hdds.scm.cli.container.CleanupSubcommand.class);
    logger.addAppender(appender);
  }

  @AfterEach
  public void after() {
    logger.removeAppender(appender);
  }

  @Test
  public void testCleanupContainerOutput() throws Exception {
    cmd = new CleanupSubcommand();
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("1");
    cmd.execute(scmClient);

    // Ensure we have a line for Replicas:
    List<LoggingEvent> logs = appender.getLog();
    List<LoggingEvent> containers = logs.stream()
        .filter(m -> m.getRenderedMessage().matches("(?s)^Containers:.*"))
        .collect(Collectors.toList());
  }

  private static boolean inSort(List<Integer> list) {
    for (int i = 0; i < list.size(); i++) {
      if (list.indexOf(i) > list.indexOf(i + 1)) {
        return false;
      }
    }
    return true;
  }

  private void testJsonOutput() throws IOException {
    cmd = new CleanupSubcommand();
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("1", "--json");
    cmd.execute(scmClient);

    List<LoggingEvent> logs = appender.getLog();
    Assertions.assertEquals(1, logs.size());

    // Ensure each DN UUID is mentioned in the message after replicas:
    String json = logs.get(0).getRenderedMessage();
    Assertions.assertTrue(json.matches("(?s).*replicas.*"));
    for (DatanodeDetails dn : datanodes) {
      Pattern pattern = Pattern.compile(
          ".*replicas.*" + dn.getUuid().toString() + ".*", Pattern.DOTALL);
      Matcher matcher = pattern.matcher(json);
      Assertions.assertTrue(matcher.matches());
    }
    Pattern pattern = Pattern.compile(".*replicaIndex.*",
        Pattern.DOTALL);
    Matcher matcher = pattern.matcher(json);
    Assertions.assertTrue(matcher.matches());
  }


  private List<ContainerReplicaInfo> getReplicas(boolean includeIndex) {
    List<ContainerReplicaInfo> replicas = new ArrayList<>();
    int index = 1;
    for (DatanodeDetails dn : datanodes) {
      ContainerReplicaInfo.Builder container
          = new ContainerReplicaInfo.Builder()
          .setContainerID(1)
          .setBytesUsed(1234)
          .setState("CLOSED")
          .setPlaceOfBirth(dn.getUuid())
          .setDatanodeDetails(dn)
          .setKeyCount(1)
          .setSequenceId(1);
      if (includeIndex) {
        container.setReplicaIndex(index++);
      }
      replicas.add(container.build());
    }
    return replicas;
  }

  private ContainerWithPipeline getContainerWithPipeline() {
    Pipeline pipeline = new Pipeline.Builder()
        .setState(Pipeline.PipelineState.CLOSED)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setId(PipelineID.randomId())
        .setNodes(datanodes)
        .build();

    ContainerInfo container = new ContainerInfo.Builder()
        .setSequenceId(1)
        .setPipelineID(pipeline.getId())
        .setUsedBytes(1234)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setNumberOfKeys(1)
        .setState(CLOSED)
        .build();

    return new ContainerWithPipeline(container, pipeline);
  }

  private ContainerWithPipeline getECContainerWithPipeline() {
    Pipeline pipeline = new Pipeline.Builder()
        .setState(Pipeline.PipelineState.CLOSED)
        .setReplicationConfig(new ECReplicationConfig(3, 2))
        .setId(PipelineID.randomId())
        .setNodes(datanodes)
        .build();

    ContainerInfo container = new ContainerInfo.Builder()
        .setSequenceId(1)
        .setPipelineID(pipeline.getId())
        .setUsedBytes(1234)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setNumberOfKeys(1)
        .setState(CLOSED)
        .build();

    return new ContainerWithPipeline(container, pipeline);
  }

  private List<DatanodeDetails> createDatanodeDetails(int count) {
    List<DatanodeDetails> dns = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      HddsProtos.DatanodeDetailsProto dnd =
          HddsProtos.DatanodeDetailsProto.newBuilder()
              .setHostName("host" + i)
              .setIpAddress("1.2.3." + i + 1)
              .setNetworkLocation("/default")
              .setNetworkName("host" + i)
              .addPorts(HddsProtos.Port.newBuilder()
                  .setName("ratis").setValue(5678).build())
              .setUuid(UUID.randomUUID().toString())
              .build();
      dns.add(DatanodeDetails.getFromProtoBuf(dnd));
    }
    return dns;
  }

  private static class TestAppender extends AppenderSkeleton {
    private final List<LoggingEvent> log = new ArrayList<>();

    @Override
    public boolean requiresLayout() {
      return false;
    }

    @Override
    protected void append(final LoggingEvent loggingEvent) {
      log.add(loggingEvent);
    }

    @Override
    public void close() {
    }

    public List<LoggingEvent> getLog() {
      return new ArrayList<>(log);
    }
  }
}
