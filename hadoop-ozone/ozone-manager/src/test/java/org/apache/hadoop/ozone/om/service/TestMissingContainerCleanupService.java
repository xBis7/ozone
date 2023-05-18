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
package org.apache.hadoop.ozone.om.service;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link MissingContainerCleanupService}.
 */
@Timeout(300)
public class TestMissingContainerCleanupService {

  @TempDir
  private static File dbDir;

  private static final OzoneConfiguration CONFIG =
      new OzoneConfiguration();
  private static final String VOLUME = "vol";
  private static final String BUCKET = "bucket";
  private static OzoneManager ozoneManager;
  private static OzoneManagerProtocol omWriteClient;
  private static OMMetadataManager omMetadataManager;
  private static ScmBlockLocationProtocol scmBlockClient;
  private static StorageContainerLocationProtocol scmContainerClient;


  @BeforeAll
  public static void init()
      throws AuthenticationException, IOException {
    CONFIG.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        dbDir.getAbsolutePath());
    CONFIG.setTimeDuration(OzoneConfigKeys
        .OZONE_CONTAINER_CLEANUP_SERVICE_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    OmTestManagers omTestManagers = new OmTestManagers(CONFIG);
    ozoneManager = omTestManagers.getOzoneManager();
    omWriteClient = omTestManagers.getWriteClient();
    omMetadataManager = omTestManagers.getMetadataManager();
    scmBlockClient = omTestManagers.getScmBlockClient();
    scmContainerClient = ozoneManager.getScmClient().getContainerClient();

    createVolumeAndBucket();
  }

  @Test
  public void testContainerCleanupService() {

  }

  @Test
  public void testContainerNotMissingAnymore() {

  }

  @Test
  public void testBlocksInMultipleContainers() {

  }

  private static void createVolumeAndBucket() throws IOException {
    OMRequestTestUtils.addVolumeToOM(omMetadataManager,
        OmVolumeArgs.newBuilder()
            .setOwnerName("o")
            .setAdminName("a")
            .setVolume(VOLUME)
            .build());

    OMRequestTestUtils.addBucketToOM(omMetadataManager,
        OmBucketInfo.newBuilder().setVolumeName(VOLUME)
            .setBucketName(BUCKET)
            .setIsVersionEnabled(true)
            .build());
  }

  private void createKey(String keyName, long containerId) {
    
  }
}
