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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.response.key;

import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import java.io.IOException;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.MISSING_CONTAINER_TABLE;

/**
 * Response for ContainerCleanup request.
 */
@CleanupTableInfo(cleanupTables = {MISSING_CONTAINER_TABLE})
public class OMCleanupContainerResponse extends OMClientResponse {

  private final String containerId;
  private final ContainerInfo containerInfo;

  public OMCleanupContainerResponse(@Nonnull OMResponse omResponse,
                                    String containerId,
                                    ContainerInfo containerInfo) {
    super(omResponse);
    this.containerId = containerId;
    this.containerInfo = containerInfo;
  }

  public OMCleanupContainerResponse(@Nonnull OMResponse omResponse,
                                    String containerId) {
    super(omResponse);
    this.containerId = containerId;
    this.containerInfo = null;
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
                              BatchOperation batchOperation)
      throws IOException {
    omMetadataManager.getMissingContainerTable()
        .putWithBatch(batchOperation, containerId, containerInfo);
  }
}
