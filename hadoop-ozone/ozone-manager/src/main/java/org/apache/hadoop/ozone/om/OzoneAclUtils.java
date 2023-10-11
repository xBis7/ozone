/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Objects;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.BUCKET;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.VOLUME;

/**
 * Ozone Acl Wrapper class.
 */
public final class OzoneAclUtils {

  private static OMMultiTenantManager multiTenantManager;

  private OzoneAclUtils() {
  }

  public static void setOMMultiTenantManager(
      OMMultiTenantManager tenantManager) {
    multiTenantManager = tenantManager;
  }

  /**
   * Converts the given access ID to a kerberos principal.
   * If the access ID does not belong to a tenant, the access ID is returned
   * as is to be used as the principal.
   */
  public static String accessIdToUserPrincipal(String accessID) {
    if (multiTenantManager == null) {
      return accessID;
    }

    String principal = multiTenantManager.getUserNameGivenAccessId(accessID);
    if (principal == null) {
      principal = accessID;
    }

    return principal;
  }

  /**
   * Check Acls of ozone object with volume owner and bucket owner.
   * @param omMetadataReader
   * @param resType
   * @param storeType
   * @param aclType
   * @param vol
   * @param bucket
   * @param key
   * @param volOwner
   * @param bucketOwner
   * @throws IOException
   */
  @SuppressWarnings("parameternumber")
  public static void checkAllAcls(OmMetadataReader omMetadataReader,
      OzoneObj.ResourceType resType,
      OzoneObj.StoreType storeType, IAccessAuthorizer.ACLType aclType,
      String vol, String bucket, String key, String volOwner,
      String bucketOwner, UserGroupInformation user, InetAddress remoteAddress,
      String hostName) throws IOException {

    boolean isVolOwner = isOwner(user, volOwner);

    switch (resType) {
    //For Volume level access we only need to check {OWNER} equal
    // to Volume Owner.
    case VOLUME:
      omMetadataReader.checkAcls(resType, storeType, aclType, vol, bucket, key,
          user, remoteAddress, hostName, true,
          volOwner);
      break;
    case BUCKET:
    case KEY:
    //For Bucket/Key/Prefix level access, first we need to check {OWNER} equal
    // to volume owner on parent volume. Then we need to check {OWNER} equals
    // volume owner if current ugi user is volume owner else we need check
    //{OWNER} equals bucket owner for bucket/key/prefix.
    case PREFIX:
      if (isVolOwner) {
        omMetadataReader.checkAcls(resType, storeType,
            aclType, vol, bucket, key,
            user, remoteAddress, hostName, true,
            volOwner);
      } else {
        IAccessAuthorizer.ACLType parentAclRight =
            IAccessAuthorizer.ACLType.READ;
        // OzoneNativeAuthorizer differs from Ranger Authorizer as Ranger
        // requires only READ access on parent level access.
        // OzoneNativeAuthorizer has different parent level access based on the
        // child level access type.
        if (omMetadataReader.isNativeAuthorizerEnabled() && resType == BUCKET) {
          parentAclRight = getParentNativeAcl(aclType, resType);
        }

        omMetadataReader.checkAcls(OzoneObj.ResourceType.VOLUME, storeType,
            parentAclRight, vol, bucket, key, user,
            remoteAddress, hostName, true,
            volOwner);

        // Delete files owned by the current user
        // Delete files with _COPYING_ suffix

        // create 'key._COPYING_'
        // write 'key._COPYING_'
        // delete 'key._COPYING_'
        // create 'key'
        Logger LOG = LoggerFactory.getLogger(OzoneAclUtils.class);

        // This new code should be in a method that returns the username,
        // so that it can be cleanly reused in other places checking for FSO keys.
        LOG.info("xbis: here: OzoneAclUtils.checkAllAcls()");
        // If create or delete with _COPYING_ suffix.
        boolean ignoreACLs =
            (Objects.equals(aclType, IAccessAuthorizer.ACLType.CREATE) ||
             Objects.equals(aclType, IAccessAuthorizer.ACLType.WRITE)) ||
            (Objects.equals(aclType, IAccessAuthorizer.ACLType.DELETE) &&
            key.contains(OzoneConsts.FS_FILE_COPYING_TEMP_SUFFIX));

        String owner = "";

        if (Objects.equals(resType, OzoneObj.ResourceType.KEY)) {
          LOG.info("xbis: key: " + key);
          if (ignoreACLs) {
            owner = user.getUserName();
            LOG.info("xbis: ignoreACLs: resource: " + resType + " | owner: " + owner);
          } else {
            List<OzoneAcl> aclList = omMetadataReader.getKeyOzoneAcls(vol, bucket, key);
            for (OzoneAcl acl : aclList) {
              LOG.info("xbis: user: " + acl.getName() + " | ACLs: " + IAccessAuthorizer.ACLType.getACLString(acl.getAclBitSet()));
              // If ACL is for user (and not group or world) &&
              // acl user is the same as the current user &&
              // acl access is a
              if (Objects.equals(acl.getType(), IAccessAuthorizer.ACLIdentityType.USER) &&
                  Objects.equals(user.getUserName(), acl.getName()) &&
                  Objects.equals(IAccessAuthorizer.ACLType.getACLString(acl.getAclBitSet()), "a")) {
                owner = user.getUserName();
                LOG.info("xbis: don't ignoreACLs: resource: " + resType + " | owner: " + owner);
                break;
              }
            }
          }
        }

        if (owner.isEmpty()) {
          owner = bucketOwner;
        }

        omMetadataReader.checkAcls(resType, storeType,
            aclType, vol, bucket, key,
            user, remoteAddress, hostName, true,
//            bucketOwner);
            owner);
      }
      break;
    default:
      throw new OMException("Unexpected object type:" +
              resType, INVALID_REQUEST);
    }
  }

  /**
   * get the Parent ACL based on child ACL and resource type.
   * 
   * @param aclRight child acl as required
   * @param resType resource type
   * @return parent acl
   */
  public static IAccessAuthorizer.ACLType getParentNativeAcl(
      IAccessAuthorizer.ACLType aclRight, OzoneObj.ResourceType resType) {
    // For volume, parent access has no meaning and not used
    if (resType == VOLUME) {
      return IAccessAuthorizer.ACLType.NONE;
    }
    
    // Refined the parent for bucket, keys & prefix
    // OP         |CHILD       |PARENT

    // CREATE      NONE        WRITE
    // DELETE      DELETE      READ
    // WRITE       WRITE       WRITE     (For key/prefix, volume is READ)
    // WRITE_ACL   WRITE_ACL   READ      (V1 WRITE_ACL=>WRITE)

    // READ        READ        READ
    // LIST        LIST        READ      (V1 LIST=>READ)
    // READ_ACL    READ_ACL    READ      (V1 READ_ACL=>READ)

    // for bucket, except CREATE, all cases need READ for volume
    if (resType == BUCKET) {
      if (aclRight == IAccessAuthorizer.ACLType.CREATE) {
        return IAccessAuthorizer.ACLType.WRITE;
      }
      return IAccessAuthorizer.ACLType.READ;
    }
    
    // else for key and prefix, bucket permission will be read
    // except where key/prefix have CREATE and WRITE,
    // bucket will have WRITE
    IAccessAuthorizer.ACLType parentAclRight = aclRight;
    if (aclRight == IAccessAuthorizer.ACLType.CREATE) {
      parentAclRight = IAccessAuthorizer.ACLType.WRITE;
    } else if (aclRight == IAccessAuthorizer.ACLType.READ_ACL
        || aclRight == IAccessAuthorizer.ACLType.LIST
        || aclRight == IAccessAuthorizer.ACLType.WRITE_ACL
        || aclRight == IAccessAuthorizer.ACLType.DELETE) {
      parentAclRight = IAccessAuthorizer.ACLType.READ;
    }

    return parentAclRight;
  }

  private static boolean isOwner(UserGroupInformation callerUgi,
      String ownerName) {
    if (ownerName == null) {
      return false;
    }
    if (callerUgi.getShortUserName().equals(ownerName)) {
      return true;
    }
    return false;
  }
}
