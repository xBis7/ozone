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
  private static UserGroupInformation currUgi;
  private static String currUser;
  private static final List<OzoneAcl> ACL_LIST_USER_ALL = new ArrayList<>();
  private static final List<OzoneAcl> ACL_LIST_USER_RESTRICTED = new ArrayList<>();
  private static final List<OzoneAcl> ACL_LIST_MULTIPLE_USERS = new ArrayList<>();
  private static final List<OzoneAcl> EMPTY_ACL_LIST = new ArrayList<>();
  private static final String TMP_VOLUME_NAME = "tmp";
  private static final String TMP_BUCKET_NAME = "tmp";
  private static final String KEY_NAME = "key";
  private static final String BUCKET_OWNER = "hadoop";
  private static final String ACL_USER_ALL = "ozone";
  private static final String ACL_USER_RESTRICTED = "test";


  @BeforeAll
  public static void setUp() throws IOException {
    omMetadataReader = Mockito.mock(OmMetadataReader.class);
    currUgi = UserGroupInformation.getCurrentUser();
    currUser = currUgi.getUserName();
    setupACLLists();
  }

  private static void setupACLLists() {
    // Create user and group ACL with all access
    OzoneAcl userAclAll = new OzoneAcl(
        IAccessAuthorizer.ACLIdentityType.USER,
        ACL_USER_ALL,
        IAccessAuthorizer.ACLType.ALL,
        OzoneAcl.AclScope.ACCESS);
    OzoneAcl groupAclAll = new OzoneAcl(
        IAccessAuthorizer.ACLIdentityType.GROUP,
        ACL_USER_ALL,
        IAccessAuthorizer.ACLType.ALL,
        OzoneAcl.AclScope.ACCESS);

    // Create user and group ACL with restricted access
    OzoneAcl userAclRestr = new OzoneAcl(
        IAccessAuthorizer.ACLIdentityType.USER,
        ACL_USER_RESTRICTED,
        IAccessAuthorizer.ACLType.READ,
        OzoneAcl.AclScope.ACCESS);
    OzoneAcl groupAclRestr = new OzoneAcl(
        IAccessAuthorizer.ACLIdentityType.GROUP,
        ACL_USER_RESTRICTED,
        IAccessAuthorizer.ACLType.READ,
        OzoneAcl.AclScope.ACCESS);

    // ACL list with 1 user will all access
    OzoneAclUtil.addAcl(ACL_LIST_USER_ALL, userAclAll);
    OzoneAclUtil.addAcl(ACL_LIST_USER_ALL, groupAclAll);

    // ACL list with 1 user with restricted access
    OzoneAclUtil.addAcl(ACL_LIST_USER_RESTRICTED, userAclRestr);
    OzoneAclUtil.addAcl(ACL_LIST_USER_RESTRICTED, groupAclRestr);

    // ACL list with 2 users, 1 with all access and 1 with restricted
    OzoneAclUtil.addAcl(ACL_LIST_MULTIPLE_USERS, userAclAll);
    OzoneAclUtil.addAcl(ACL_LIST_MULTIPLE_USERS, groupAclAll);
    OzoneAclUtil.addAcl(ACL_LIST_MULTIPLE_USERS, userAclRestr);
    OzoneAclUtil.addAcl(ACL_LIST_MULTIPLE_USERS, groupAclRestr);
  }

  private static Stream<Arguments> aclTypeArgs() {
    return Stream.of(
        Arguments.of(IAccessAuthorizer.ACLType.CREATE),
        Arguments.of(IAccessAuthorizer.ACLType.WRITE));
  }

  /**
   * In all cases, we will have a non-native authorizer, ACLType.DELETE.
   * If path isn't /tmp/tmp, aclList will be ignored.
   * We will test these key ACL cases:
   * 1. empty ACL list                                    -> return bucketOwner
   * 2. user same as the current but with only read access-> return bucketOwner
   * 3. user with all access that's not the current user  -> return bucketOwner
   * 4. user with all access and same as the current user -> return aclUser
   * 5. multiple users, the current user has all access   -> return aclUser
   * 6. multiple users, none of which is the current      -> return bucketOwner
   */
  private static Stream<Arguments> aclListArgs() {
    return Stream.of(
        Arguments.of(EMPTY_ACL_LIST, TMP_VOLUME_NAME,
            TMP_BUCKET_NAME, currUser, BUCKET_OWNER),
        Arguments.of(ACL_LIST_USER_RESTRICTED, TMP_VOLUME_NAME,
            TMP_BUCKET_NAME, ACL_USER_RESTRICTED, BUCKET_OWNER),
        Arguments.of(ACL_LIST_USER_ALL, TMP_VOLUME_NAME,
            TMP_BUCKET_NAME, currUser, BUCKET_OWNER),
        Arguments.of(ACL_LIST_USER_ALL, TMP_VOLUME_NAME,
            TMP_BUCKET_NAME, ACL_USER_ALL, ACL_USER_ALL),
        Arguments.of(ACL_LIST_MULTIPLE_USERS, TMP_VOLUME_NAME,
            TMP_BUCKET_NAME, ACL_USER_ALL, ACL_USER_ALL),
        Arguments.of(ACL_LIST_MULTIPLE_USERS, TMP_VOLUME_NAME,
            TMP_BUCKET_NAME, currUser, BUCKET_OWNER),
        Arguments.of(ACL_LIST_USER_ALL, TMP_VOLUME_NAME,
            "bucket1", ACL_USER_ALL, BUCKET_OWNER),
        Arguments.of(ACL_LIST_USER_ALL, "vol1",
            TMP_BUCKET_NAME, ACL_USER_ALL, BUCKET_OWNER),
        Arguments.of(ACL_LIST_USER_ALL, "vol1",
            "bucket1", ACL_USER_ALL, BUCKET_OWNER));
  }

  @ParameterizedTest
  @ValueSource(classes = {
      OzoneAccessAuthorizer.class,
      OzoneNativeAuthorizer.class
  })
  public void testGetKeyOwnerWithOzoneNativeAuthorizer(
      Class<? extends IAccessAuthorizer> authorizerClass)
      throws IOException, InstantiationException, IllegalAccessException {
    when(omMetadataReader.getAccessAuthorizer())
        .thenReturn(authorizerClass.newInstance());

    String owner = OzoneAclUtils.getKeyOwner(
        IAccessAuthorizer.ACLType.WRITE,
        omMetadataReader, currUgi, TMP_VOLUME_NAME,
        TMP_BUCKET_NAME, KEY_NAME, BUCKET_OWNER);

    Assertions.assertEquals(BUCKET_OWNER, owner);
  }

  @ParameterizedTest
  @MethodSource("aclTypeArgs")
  public void testGetKeyOwnerWithCreateWriteAclType(
      IAccessAuthorizer.ACLType aclType) throws IOException {
    when(omMetadataReader.getAccessAuthorizer())
        .thenReturn(new MockExternalAuthorizer());

    String owner = OzoneAclUtils.getKeyOwner(
        aclType,
        omMetadataReader, currUgi, TMP_VOLUME_NAME,
        TMP_BUCKET_NAME, KEY_NAME, BUCKET_OWNER);

    Assertions.assertEquals(currUgi.getUserName(), owner);
  }

  @ParameterizedTest
  @MethodSource("aclListArgs")
  public void testGetKeyOwnerAclList(List<OzoneAcl> aclList, String volumeName,
      String bucketName, String currUser, String expUser) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(currUser);

    when(omMetadataReader.getAccessAuthorizer())
        .thenReturn(new MockExternalAuthorizer());

    when(omMetadataReader.getKeyOzoneAcls(
        volumeName, bucketName, KEY_NAME)).thenReturn(aclList);

    String owner = OzoneAclUtils.getKeyOwner(
        IAccessAuthorizer.ACLType.DELETE,
        omMetadataReader, ugi, volumeName,
        bucketName, KEY_NAME, BUCKET_OWNER);

    Assertions.assertEquals(expUser, owner);
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

