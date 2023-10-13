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

import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IOzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneNativeAuthorizer;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.mockito.Mockito.when;

/**
 * Tests for {@link OzoneAclUtils}.
 */
public class TestOzoneAclUtils {

  private static OmMetadataReader omMetadataReader;
  private static UserGroupInformation ugi;
  private static final String VOLUME_NAME = "vol";
  private static final String BUCKET_NAME = "bucket";
  private static final String KEY_NAME = "key";
  private static final String BUCKET_OWNER = "hadoop";

  @BeforeAll
  public static void setUp() throws IOException {
    omMetadataReader = Mockito.mock(OmMetadataReader.class);
    ugi = UserGroupInformation.getCurrentUser();
  }

  private static Stream<Arguments> aclTypeArgs() {
    return Stream.of(
        Arguments.of(IAccessAuthorizer.ACLType.CREATE),
        Arguments.of(IAccessAuthorizer.ACLType.WRITE));
  }

  @ParameterizedTest
  @ValueSource(classes = {
      OzoneAccessAuthorizer.class,
      OzoneNativeAuthorizer.class
  })
  public void testGetKeyOwnerWithOzoneAuthorizer(
      Class<? extends IAccessAuthorizer> authorizerClass)
      throws IOException, InstantiationException, IllegalAccessException {
    when(omMetadataReader.getAccessAuthorizer())
        .thenReturn(authorizerClass.newInstance());

    String owner = OzoneAclUtils.getKeyOwner(
        IAccessAuthorizer.ACLType.WRITE,
        omMetadataReader, ugi, VOLUME_NAME,
        BUCKET_NAME, KEY_NAME, BUCKET_OWNER);

    Assertions.assertEquals(BUCKET_OWNER, owner);
  }

  @ParameterizedTest
  @MethodSource("aclTypeArgs")
  public void testGetKeyOwnerWithExternalAuthorizer(
      IAccessAuthorizer.ACLType aclType) throws IOException {
    when(omMetadataReader.getAccessAuthorizer())
        .thenReturn(new MockExternalAuthorizer());

    String owner = OzoneAclUtils.getKeyOwner(
        aclType,
        omMetadataReader, ugi, VOLUME_NAME,
        BUCKET_NAME, KEY_NAME, BUCKET_OWNER);

    Assertions.assertEquals(ugi.getUserName(), owner);
  }

  @Test
  public void testGetKeyOwnerAclUserHasAccessAll() throws IOException {
    String user = "test";
    String group = "test";

    when(omMetadataReader.getAccessAuthorizer())
        .thenReturn(new MockExternalAuthorizer());

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

    when(omMetadataReader
             .getKeyOzoneAcls(VOLUME_NAME, BUCKET_NAME, KEY_NAME))
        .thenReturn(aclList);

    String owner = OzoneAclUtils.getKeyOwner(
        IAccessAuthorizer.ACLType.DELETE,
        omMetadataReader, ugi, VOLUME_NAME,
        BUCKET_NAME, KEY_NAME, BUCKET_OWNER);

    Assertions.assertEquals(user, owner);
  }

  @Test
  public void testGetKeyOwnerWithEmptyAclList()
      throws IOException {
    when(omMetadataReader.getAccessAuthorizer())
        .thenReturn(new MockExternalAuthorizer());

    List<OzoneAcl> aclList = new ArrayList<>();

    when(omMetadataReader
             .getKeyOzoneAcls(VOLUME_NAME, BUCKET_NAME, KEY_NAME))
        .thenReturn(aclList);

    String owner = OzoneAclUtils.getKeyOwner(
        IAccessAuthorizer.ACLType.DELETE,
        omMetadataReader, ugi, VOLUME_NAME,
        BUCKET_NAME, KEY_NAME, BUCKET_OWNER);

    Assertions.assertEquals(BUCKET_OWNER, owner);
  }

  @Test
  public void testGetKeyOwnerAclUserWithoutAccessAll()
      throws IOException {
    when(omMetadataReader.getAccessAuthorizer())
        .thenReturn(new MockExternalAuthorizer());

    List<OzoneAcl> aclList = new ArrayList<>();
    OzoneAcl userAcl = new OzoneAcl(
        IAccessAuthorizer.ACLIdentityType.USER,
        ugi.getUserName(),
        IAccessAuthorizer.ACLType.READ,
        OzoneAcl.AclScope.ACCESS);
    OzoneAcl groupAcl = new OzoneAcl(
        IAccessAuthorizer.ACLIdentityType.GROUP,
        ugi.getUserName(),
        IAccessAuthorizer.ACLType.READ,
        OzoneAcl.AclScope.ACCESS);

    OzoneAclUtil.addAcl(aclList, userAcl);
    OzoneAclUtil.addAcl(aclList, groupAcl);

    when(omMetadataReader
             .getKeyOzoneAcls(VOLUME_NAME, BUCKET_NAME, KEY_NAME))
        .thenReturn(aclList);

    String owner = OzoneAclUtils.getKeyOwner(
        IAccessAuthorizer.ACLType.DELETE,
        omMetadataReader, ugi, VOLUME_NAME,
        BUCKET_NAME, KEY_NAME, BUCKET_OWNER);

    Assertions.assertEquals(BUCKET_OWNER, owner);
  }

  /**
   * Non-native authorizer for tests.
   */
  public static class MockExternalAuthorizer implements IAccessAuthorizer {
    @Override
    public boolean checkAccess(IOzoneObj ozoneObject, RequestContext context) {
      return false;
    }
  }
}

