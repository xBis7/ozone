package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ipc.IdentityProvider;
import org.apache.hadoop.ipc.Schedulable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.Objects;

public class S3IdentityProvider implements IdentityProvider {

  public S3IdentityProvider() {
  }

  @Override
  public String makeIdentity(Schedulable schedulable) {
    if (Objects.isNull(OzoneManager.getS3Auth())) {
      UserGroupInformation ugi = schedulable.getUserGroupInformation();
      return ugi == null ? null : ugi.getShortUserName();
    } else {
      S3Authentication s3Auth = OzoneManager.getS3Auth();
      return s3Auth.getAccessId();
    }
  }
}
