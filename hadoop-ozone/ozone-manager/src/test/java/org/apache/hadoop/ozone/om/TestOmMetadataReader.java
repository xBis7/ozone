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
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.slf4j.Logger;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdds.server.ServerUtils.getRemoteUserName;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link OmMetadataReader}.
 */
public class TestOmMetadataReader {

  private static OmMetadataReader metadataReader;
  private static KeyManager keyManager;
  private static PrefixManager prefixManager;
  private static OzoneManager ozoneManager;
  private static Logger log;
  private static AuditLogger auditLogger;
  private static OmMetadataReaderMetrics omMetadataReaderMetrics;
  private static IAccessAuthorizer authorizer;
  private static OmKeyArgs omKeyArgs;
  private static final String VOLUME_NAME = "vol";
  private static final String BUCKET_NAME = "bucket";
  private static final String KEY_NAME = "key";

  @BeforeAll
  public static void setUp() {
    keyManager = Mockito.mock(KeyManagerImpl.class);
    prefixManager = Mockito.mock(PrefixManager.class);
    ozoneManager = Mockito.mock(OzoneManager.class);
    log = Mockito.mock(Logger.class);
    auditLogger = Mockito.mock(AuditLogger.class);
    omMetadataReaderMetrics = Mockito.spy(OMMetrics.create());

    OMPerformanceMetrics perfMetrics = Mockito.spy(OMPerformanceMetrics.register());
    when(ozoneManager.getPerfMetrics()).thenReturn(perfMetrics);

    authorizer = Mockito.mock(IAccessAuthorizer.class);
//    metadataReader = Mockito.mock(OmMetadataReader.class);
    metadataReader = new OmMetadataReader(keyManager, prefixManager, ozoneManager, log, auditLogger, omMetadataReaderMetrics, authorizer);
    omKeyArgs = new OmKeyArgs.Builder()
                    .setVolumeName(VOLUME_NAME)
                    .setBucketName(BUCKET_NAME)
                    .setKeyName(KEY_NAME)
                    .setHeadOp(true)
                    .build();
  }

  @Test
  public void testGetKeyOzoneAcls() throws IOException {
    String user = "test";
    String group = "test";

    List<OzoneAcl> aclList = new ArrayList<>();
    OzoneAcl userAcl = new OzoneAcl(
        IAccessAuthorizer.ACLIdentityType.USER,
        user,
        IAccessAuthorizer.ACLType.ALL,
        OzoneAcl.AclScope.ACCESS);
    OzoneAcl groupAcl = new OzoneAcl(
        IAccessAuthorizer.ACLIdentityType.GROUP,
        group,
        IAccessAuthorizer.ACLType.ALL,
        OzoneAcl.AclScope.ACCESS);

    OzoneAclUtil.addAcl(aclList, userAcl);
    OzoneAclUtil.addAcl(aclList, groupAcl);

    OmKeyInfo omKeyInfo = new OmKeyInfo.Builder()
                              .setVolumeName(VOLUME_NAME)
                              .setBucketName(BUCKET_NAME)
                              .setKeyName(KEY_NAME)
                              .setAcls(aclList)
                              .build();

    OzoneFileStatus fileStatus = new OzoneFileStatus(omKeyInfo, 0L, false);

//    OzoneFileStatus status = Mockito.spy(fileStatus);
    when(fileStatus.getKeyInfo()).thenReturn(omKeyInfo);

    AuditMessage auditMessage = new AuditMessage.Builder()
                                    .setUser(getRemoteUserName())
                                    .atIp(Server.getRemoteAddress())
                                    .forOperation(OMAction.GET_FILE_STATUS)
                                    .withParams(new HashMap<>())
                                    .withResult(AuditEventStatus.SUCCESS)
                                    .build();
//    when(auditLogger.logReadSuccess(auditMessage));

    ResolvedBucket bucket = Mockito.mock(ResolvedBucket.class);
    when(ozoneManager.resolveBucketLink(omKeyArgs)).thenReturn(bucket);

    when(metadataReader.getFileStatus(omKeyArgs)).thenReturn(fileStatus);

    List<OzoneAcl> aclListFromClass = metadataReader.getKeyOzoneAcls(
        VOLUME_NAME, BUCKET_NAME, KEY_NAME);

    Assertions.assertEquals(aclList, aclListFromClass);

  }

}
