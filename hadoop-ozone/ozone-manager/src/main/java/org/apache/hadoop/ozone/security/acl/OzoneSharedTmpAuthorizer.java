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
package org.apache.hadoop.ozone.security.acl;

import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;

import java.util.Objects;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;

/**
 * Implementation of {@link IAccessAuthorizer} to specifically
 * handle the case of the shareable tmp dir.
 */
public class OzoneSharedTmpAuthorizer implements IAccessAuthorizer { // Probably don't need to implement IAccessAuthorizer.

  private final OzoneManager ozoneManager;
  private static final String OFS_SHARED_TMP = "tmp";

  public OzoneSharedTmpAuthorizer(OzoneManager ozoneManager) {
    this.ozoneManager = ozoneManager;
  }

  @Override
  public boolean checkAccess(IOzoneObj ozoneObject, RequestContext context)
      throws OMException {
    OzoneObjInfo objInfo;

    if (ozoneObject instanceof OzoneObjInfo) {
      objInfo = (OzoneObjInfo) ozoneObject;
    } else {
      throw new OMException("Unexpected input received. OM native acls are " +
          "configured to work with OzoneObjInfo type only.", INVALID_REQUEST);
    }

    boolean isTmpVolume =
        Objects.equals(objInfo.getResourceType(),
                       OzoneObj.ResourceType.VOLUME) &&
        Objects.equals(objInfo.getVolumeName(),
                       // Reuse OFS_SHARED_TMP here?
                       // The OFSPath variable is more intuitive.
                       OFSPath.OFS_MOUNT_TMP_VOLUMENAME);

    boolean isOfsSharedTmpDir =
        Objects.equals(objInfo.getResourceType(),
                       OzoneObj.ResourceType.BUCKET) &&
        Objects.equals(objInfo.getVolumeName(),
                       OFSPath.OFS_MOUNT_TMP_VOLUMENAME) &&
        Objects.equals(objInfo.getVolumeName(), OFS_SHARED_TMP);

    IAccessAuthorizer authorizer =
        (isTmpVolume || isOfsSharedTmpDir) ?
            OzoneAuthorizerFactory.createNativeAuthorizer(ozoneManager) :
            OzoneAuthorizerFactory.forOM(ozoneManager);

    return authorizer.checkAccess(ozoneObject, context);
  }

  @Override
  public boolean isNative() {
    return true;
  }
}
