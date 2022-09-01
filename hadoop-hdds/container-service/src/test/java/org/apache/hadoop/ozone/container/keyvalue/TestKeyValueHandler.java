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

package org.apache.hadoop.ozone.container.keyvalue;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerType;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.security.token.TokenVerifier;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.ozone.test.GenericTestUtils;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_VOLUME_CHOOSING_POLICY;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.junit.Assert.assertEquals;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import org.mockito.Mockito;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.times;


/**
 * Unit tests for {@link KeyValueHandler}.
 */
@RunWith(Parameterized.class)
public class TestKeyValueHandler {

  @Rule
  public TestRule timeout = Timeout.seconds(300);

  private static final String DATANODE_UUID = UUID.randomUUID().toString();

  private static final long DUMMY_CONTAINER_ID = 9999;

  private final ContainerLayoutVersion layout;

  private HddsDispatcher dispatcher;
  private KeyValueHandler handler;

  public TestKeyValueHandler(ContainerLayoutVersion layout) {
    this.layout = layout;
  }

  @Parameterized.Parameters
  public static Iterable<Object[]> parameters() {
    return ContainerLayoutTestInfo.containerLayoutParameters();
  }

  @Before
  public void setup() throws StorageContainerException {
    // Create mock HddsDispatcher and KeyValueHandler.
    handler = Mockito.mock(KeyValueHandler.class);

    HashMap<ContainerType, Handler> handlers = new HashMap<>();
    handlers.put(ContainerType.KeyValueContainer, handler);

    dispatcher = new HddsDispatcher(
        new OzoneConfiguration(),
        Mockito.mock(ContainerSet.class),
        Mockito.mock(VolumeSet.class),
        handlers,
        Mockito.mock(StateContext.class),
        Mockito.mock(ContainerMetrics.class),
        Mockito.mock(TokenVerifier.class)
    );

  }

  public void commandHandlingInit() {
    Mockito.reset(handler);
    KeyValueContainer container = Mockito.mock(KeyValueContainer.class);

    DispatcherContext context = new DispatcherContext.Builder().build();
  }

  public void commandHandlingInit(
      ContainerProtos.Type protoType
  ) {
    Mockito.reset(handler);
    KeyValueContainer container = Mockito.mock(KeyValueContainer.class);

    DispatcherContext context = new DispatcherContext.Builder().build();
    ContainerCommandRequestProto request = getDummyCommandRequestProto(
        protoType
    );
    KeyValueHandler
        .dispatchRequest(handler, request, container, context);
  }
  /**
   * Test that Handler handles different command types correctly.
   */
  @Test
  public void testCreateCommandHandling() throws Exception {
    Mockito.reset(handler);
    // Test Create Container Request handling
    ContainerCommandRequestProto createContainerRequest =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.CreateContainer)
            .setContainerID(DUMMY_CONTAINER_ID)
            .setDatanodeUuid(DATANODE_UUID)
            .setCreateContainer(ContainerProtos.CreateContainerRequestProto
                .getDefaultInstance())
            .build();

    KeyValueContainer container = Mockito.mock(KeyValueContainer.class);

    DispatcherContext context = new DispatcherContext.Builder().build();
    KeyValueHandler
        .dispatchRequest(handler, createContainerRequest, container, context);
    Mockito.verify(handler, times(0)).handleListBlock(
        any(ContainerCommandRequestProto.class), any());
  }

  @Test
  public void testReadContainerRequest() {
    // Test Read Container Request handling
    commandHandlingInit(ContainerProtos.Type.ReadContainer);
    Mockito.verify(handler, times(1)).handleReadContainer(
        any(ContainerCommandRequestProto.class), any());
  }

  @Test
  public void testUpdateContainerRequest() {
    // Test Update Container Request handling
    commandHandlingInit(ContainerProtos.Type.UpdateContainer);
    Mockito.verify(handler, times(1)).handleUpdateContainer(
        any(ContainerCommandRequestProto.class), any());
  }

  @Test
  public void testDeleteContainerRequest() {
    // Test Delete Container Request handling
    commandHandlingInit(ContainerProtos.Type.DeleteContainer);
    Mockito.verify(handler, times(1)).handleDeleteContainer(
        any(ContainerCommandRequestProto.class), any());
  }

  @Test
  public void testListContainerRequestHandling() {

    // Test List Container Request handling
    commandHandlingInit(ContainerProtos.Type.ListContainer);
    Mockito.verify(handler, times(1)).handleUnsupportedOp(
        any(ContainerCommandRequestProto.class));
  }

  @Test
  public void testCloseContainerRequestHandling() {
    // Test Close Container Request handling
    commandHandlingInit(ContainerProtos.Type.CloseContainer);
    Mockito.verify(handler, times(1)).handleCloseContainer(
        any(ContainerCommandRequestProto.class), any());
  }

  @Test
  public void testPutBlockRequestHandling() {
    commandHandlingInit(ContainerProtos.Type.PutBlock);

    Mockito.verify(handler, times(1)).handlePutBlock(
        any(ContainerCommandRequestProto.class), any(), any());
  }

  @Test
  public void testGetBlockRequestHandling() {
    commandHandlingInit(ContainerProtos.Type.GetBlock);
    Mockito.verify(handler, times(1)).handleGetBlock(
        any(ContainerCommandRequestProto.class), any());
    }
    // Block Deletion is handled by BlockDeletingService and need not be
    // tested here.

    @Test
    public void testListBlockRequest() {
      commandHandlingInit(ContainerProtos.Type.ListBlock);
      Mockito.verify(handler, times(1)).handleUnsupportedOp(
          any(ContainerCommandRequestProto.class));
    }

    @Test
    public void testReadChunkRequest() {
      commandHandlingInit(ContainerProtos.Type.ReadChunk);
      Mockito.verify(handler, times(1)).handleReadChunk(
          any(ContainerCommandRequestProto.class), any(), any());
    }

    // Chunk Deletion is handled by BlockDeletingService and need not be
    // tested here.

    @Test
    public void testWriteChunkRequest() {
      commandHandlingInit(ContainerProtos.Type.WriteChunk);
      Mockito.verify(handler, times(1)).handleWriteChunk(
          any(ContainerCommandRequestProto.class), any(), any());
    }

    @Test
    public void testListChunkRequestHandling() {
      commandHandlingInit(ContainerProtos.Type.ListChunk);
      Mockito.verify(handler, times(2)).handleUnsupportedOp(
          any(ContainerCommandRequestProto.class));
    }

    @Test
    public void testPutSmallFileRequestHandling() {
      commandHandlingInit(ContainerProtos.Type.PutSmallFile);
      Mockito.verify(handler, times(1)).handlePutSmallFile(
          any(ContainerCommandRequestProto.class), any(), any());
    }

    // Test Get Small File Request handling
    @Test
    public void testGetSmallFileRequestHandling() {
      commandHandlingInit(ContainerProtos.Type.GetSmallFile);
      Mockito.verify(handler, times(1)).handleGetSmallFile(
          any(ContainerCommandRequestProto.class), any());
    }

  @Test
  public void testVolumeSetInKeyValueHandler() throws Exception {
    File path = GenericTestUtils.getRandomizedTestDir();
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HDDS_DATANODE_DIR_KEY, path.getAbsolutePath());
    conf.set(OZONE_METADATA_DIRS, path.getAbsolutePath());
    MutableVolumeSet
        volumeSet = new MutableVolumeSet(UUID.randomUUID().toString(), conf,
        null, StorageVolume.VolumeType.DATA_VOLUME, null);
    try {
      ContainerSet cset = new ContainerSet(1000);
      int[] interval = new int[1];
      interval[0] = 2;
      ContainerMetrics metrics = new ContainerMetrics(interval);
      DatanodeDetails datanodeDetails = Mockito.mock(DatanodeDetails.class);
      DatanodeStateMachine stateMachine = Mockito.mock(
          DatanodeStateMachine.class);
      StateContext context = Mockito.mock(StateContext.class);
      Mockito.when(stateMachine.getDatanodeDetails())
          .thenReturn(datanodeDetails);
      Mockito.when(context.getParent()).thenReturn(stateMachine);
      KeyValueHandler keyValueHandler = new KeyValueHandler(conf,
          context.getParent().getDatanodeDetails().getUuidString(), cset,
          volumeSet, metrics, c -> {
      });
      assertEquals("org.apache.hadoop.ozone.container.common" +
          ".volume.RoundRobinVolumeChoosingPolicy",
          keyValueHandler.getVolumeChoosingPolicyForTesting()
              .getClass().getName());

      //Set a class which is not of sub class of VolumeChoosingPolicy
      conf.set(HDDS_DATANODE_VOLUME_CHOOSING_POLICY,
          "org.apache.hadoop.ozone.container.common.impl.HddsDispatcher");
      try {
        new KeyValueHandler(conf,
            context.getParent().getDatanodeDetails().getUuidString(),
            cset, volumeSet, metrics, c -> { });
      } catch (RuntimeException ex) {
        GenericTestUtils.assertExceptionContains("class org.apache.hadoop" +
            ".ozone.container.common.impl.HddsDispatcher not org.apache" +
            ".hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy",
            ex);
      }
    } finally {
      volumeSet.shutdown();
      FileUtil.fullyDelete(path);
    }
  }

  private ContainerCommandRequestProto getDummyCommandRequestProto(
      ContainerProtos.Type cmdType) {
    return ContainerCommandRequestProto.newBuilder()
        .setCmdType(cmdType)
        .setContainerID(DUMMY_CONTAINER_ID)
        .setDatanodeUuid(DATANODE_UUID)
        .build();
  }

  @Test
  public void testDeleteInternal() throws IOException {
    //  Init KeyValueHandler
    final String testDir = GenericTestUtils.getTempPath(
        TestKeyValueHandler.class.getSimpleName() +
            "-" + UUID.randomUUID().toString());
    final long containerID = 1L;
    OzoneConfiguration conf = new OzoneConfiguration();

    final ContainerSet containerSet = new ContainerSet(1000);
    MutableVolumeSet volumeSet = new MutableVolumeSet(
        DATANODE_UUID, conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null
    );

    final int[] interval = new int[1];
    interval[0] = 2;
    final ContainerMetrics metrics = new ContainerMetrics(interval);

    final AtomicInteger icrReceived = new AtomicInteger(0);

    final KeyValueHandler kvHandler = new KeyValueHandler(conf,
        UUID.randomUUID().toString(), containerSet, volumeSet, metrics,
        c -> icrReceived.incrementAndGet());
    kvHandler.setClusterID(UUID.randomUUID().toString());

    //  Create Container
    KeyValueContainerData kvData = new KeyValueContainerData(containerID,
        layout,
        (long) StorageUnit.GB.toBytes(1), UUID.randomUUID().toString(),
        UUID.randomUUID().toString());
    KeyValueContainer container = new KeyValueContainer(kvData, conf);
    int prevCount = containerSet.containerCount();

    kvHandler.deleteContainer(container, true);

    assertTrue(
        prevCount > containerSet.containerCount()
    );
    assertEquals(
        container.getContainerState(),
        ContainerProtos.ContainerDataProto.State.DELETED
    );
    Assert.assertNull(
        new File(kvData.getChunksPath())
    );
    Assert.assertNull(
        new File(kvData.getMetadataPath())
    );
  }


  @Test
  public void testCloseInvalidContainer() throws IOException {
    long containerID = 1234L;
    OzoneConfiguration conf = new OzoneConfiguration();
    KeyValueContainerData kvData = new KeyValueContainerData(containerID,
        layout,
        (long) StorageUnit.GB.toBytes(1), UUID.randomUUID().toString(),
        UUID.randomUUID().toString());
    KeyValueContainer container = new KeyValueContainer(kvData, conf);
    kvData.setState(ContainerProtos.ContainerDataProto.State.INVALID);

    // Create Close container request
    ContainerCommandRequestProto closeContainerRequest =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.CloseContainer)
            .setContainerID(DUMMY_CONTAINER_ID)
            .setDatanodeUuid(DATANODE_UUID)
            .setCloseContainer(ContainerProtos.CloseContainerRequestProto
                .getDefaultInstance())
            .build();
    dispatcher.dispatch(closeContainerRequest, null);

    Mockito.when(handler.handleCloseContainer(any(), any()))
        .thenCallRealMethod();
    doCallRealMethod().when(handler).closeContainer(any());
    // Closing invalid container should return error response.
    ContainerProtos.ContainerCommandResponseProto response =
        handler.handleCloseContainer(closeContainerRequest, container);

    assertEquals("Close container should return Invalid container error",
        ContainerProtos.Result.INVALID_CONTAINER_STATE, response.getResult());
  }

  @Test
  public void testDeleteContainer() throws IOException {
    final String testDir = GenericTestUtils.getTempPath(
        TestKeyValueHandler.class.getSimpleName() +
            "-" + UUID.randomUUID().toString());
    try {
      final long containerID = 1L;
      final ConfigurationSource conf = new OzoneConfiguration();
      final ContainerSet containerSet = new ContainerSet(1000);
      final VolumeSet volumeSet = Mockito.mock(VolumeSet.class);

      Mockito.when(volumeSet.getVolumesList())
          .thenReturn(Collections.singletonList(
              new HddsVolume.Builder(testDir).conf(conf).build()));

      final int[] interval = new int[1];
      interval[0] = 2;
      final ContainerMetrics metrics = new ContainerMetrics(interval);

      final AtomicInteger icrReceived = new AtomicInteger(0);

      final KeyValueHandler kvHandler = new KeyValueHandler(conf,
          UUID.randomUUID().toString(), containerSet, volumeSet, metrics,
          c -> icrReceived.incrementAndGet());
      kvHandler.setClusterID(UUID.randomUUID().toString());

      final ContainerCommandRequestProto createContainer =
          ContainerCommandRequestProto.newBuilder()
              .setCmdType(ContainerProtos.Type.CreateContainer)
              .setDatanodeUuid(UUID.randomUUID().toString())
              .setCreateContainer(
                  ContainerProtos.CreateContainerRequestProto.newBuilder()
                      .setContainerType(ContainerType.KeyValueContainer)
                      .build())
              .setContainerID(containerID)
              .setPipelineID(UUID.randomUUID().toString())
              .build();

      kvHandler.handleCreateContainer(createContainer, null);
      Assert.assertEquals(1, icrReceived.get());
      Assert.assertNotNull(containerSet.getContainer(containerID));

      kvHandler.deleteContainer(containerSet.getContainer(containerID), true);
      Assert.assertEquals(2, icrReceived.get());
      Assert.assertNull(containerSet.getContainer(containerID));
    } finally {
      FileUtils.deleteDirectory(new File(testDir));
    }
  }
}
