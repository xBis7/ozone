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
package org.apache.hadoop.ozone.om.request.key;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMContainerCleanupResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CleanupContainerRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CleanupContainerArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;

import java.io.IOException;

/**
 * Handles CleanupContainer Request.
 */
public class OMContainerCleanupRequest extends OMClientRequest {

  public OMContainerCleanupRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager)
      throws IOException {
    CleanupContainerRequest cleanupContainerRequest =
        getOmRequest().getCleanupContainerRequest();
    Preconditions.checkNotNull(cleanupContainerRequest);

    CleanupContainerArgs containerArgs = cleanupContainerRequest
        .getCleanupContainerArgs();

    CleanupContainerRequest newContainerReq =
        CleanupContainerRequest.newBuilder()
            .setCleanupContainerArgs(containerArgs)
            .build();

    return getOmRequest().toBuilder()
        .setCleanupContainerRequest(newContainerReq)
        .setUserInfo(getUserIfNotExists(ozoneManager))
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {
    OMRequest omRequest = getOmRequest();
    // Generate end user response
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    long containerId = omRequest.getCleanupContainerRequest()
        .getCleanupContainerArgs().getContainerId();

    OMContainerCleanupResponse cleanupContainerResponse;

    try {
      ContainerInfo containerInfo = ozoneManager.getScmClient()
          .getContainerClient().getContainer(containerId);
      ozoneManager.getMetadataManager().getMissingContainerTable()
          .addCacheEntry(new CacheKey<>(containerId),
              CacheValue.get(transactionLogIndex));
      cleanupContainerResponse = new OMContainerCleanupResponse(
          omResponse.build(), containerId, containerInfo);
    } catch (IOException ex) {
      cleanupContainerResponse = new OMContainerCleanupResponse(
          createErrorOMResponse(omResponse, ex), containerId);
    }

    addResponseToDoubleBuffer(transactionLogIndex, cleanupContainerResponse,
        ozoneManagerDoubleBufferHelper);

    return cleanupContainerResponse;
  }
}
