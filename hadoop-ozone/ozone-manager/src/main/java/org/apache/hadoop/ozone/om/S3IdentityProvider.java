package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ipc.IdentityProvider;
import org.apache.hadoop.ipc.Schedulable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ozone implementation of IdentityProvider used by
 * Hadoop DecayRpcScheduler.
 */
public class S3IdentityProvider implements IdentityProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3IdentityProvider.class);

  public S3IdentityProvider() {
  }

  @Override
  public String makeIdentity(Schedulable schedulable) {
    S3Authentication s3Authentication =
        OzoneManagerProtocolServerSideTranslatorPB.getS3Auth();
    if (s3Authentication != null) {
      return s3Authentication.getAccessId();
    } else {
      UserGroupInformation ugi = schedulable.getUserGroupInformation();
      return ugi == null ? null : ugi.getShortUserName();
    }
  }
}
