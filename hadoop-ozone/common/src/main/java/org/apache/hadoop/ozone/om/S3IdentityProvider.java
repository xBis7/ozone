package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ipc.IdentityProvider;
import org.apache.hadoop.ipc.Schedulable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import org.apache.hadoop.security.UserGroupInformation;

public class S3IdentityProvider implements IdentityProvider {

  public S3IdentityProvider() {
  }

  @Override
  public String makeIdentity(Schedulable schedulable) {
    OzoneManagerProtocolProtos.OMRequest omRequest =
        OzoneManagerProtocolProtos.OMRequest.getDefaultInstance();
    if (omRequest.hasS3Authentication()) {
      S3Authentication s3Auth = omRequest.getS3Authentication();
      return s3Auth.getAccessId();
    } else {
      UserGroupInformation ugi = schedulable.getUserGroupInformation();
      return ugi == null ? null : ugi.getShortUserName();
    }
  }
}
