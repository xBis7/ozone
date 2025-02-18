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
package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Secret;

import java.util.Objects;

/**
 * S3Secret to be saved in database.
 */
public class S3SecretValue {
  private static final Codec<S3SecretValue> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(S3Secret.getDefaultInstance()),
      S3SecretValue::fromProtobuf,
      S3SecretValue::getProtobuf);

  public static Codec<S3SecretValue> getCodec() {
    return CODEC;
  }

  // TODO: This field should be renamed to accessId for generalization.
  private String kerberosID;
  private String awsSecret;
  private boolean isDeleted;
  private long transactionLogIndex;

  public S3SecretValue(String kerberosID, String awsSecret) {
    this(kerberosID, awsSecret, false, 0L);
  }

  public S3SecretValue(String kerberosID, String awsSecret, boolean isDeleted,
                       long transactionLogIndex) {
    this.kerberosID = kerberosID;
    this.awsSecret = awsSecret;
    this.isDeleted = isDeleted;
    this.transactionLogIndex = transactionLogIndex;
  }

  public String getKerberosID() {
    return kerberosID;
  }

  public void setKerberosID(String kerberosID) {
    this.kerberosID = kerberosID;
  }

  public String getAwsSecret() {
    return awsSecret;
  }

  public void setAwsSecret(String awsSecret) {
    this.awsSecret = awsSecret;
  }

  public boolean isDeleted() {
    return isDeleted;
  }

  public void setDeleted(boolean status) {
    this.isDeleted = status;
  }

  public String getAwsAccessKey() {
    return kerberosID;
  }

  public long getTransactionLogIndex() {
    return transactionLogIndex;
  }

  public void setTransactionLogIndex(long transactionLogIndex) {
    this.transactionLogIndex = transactionLogIndex;
  }

  public static S3SecretValue fromProtobuf(S3Secret s3Secret) {
    return new S3SecretValue(s3Secret.getKerberosID(), s3Secret.getAwsSecret());
  }

  public S3Secret getProtobuf() {
    return S3Secret.newBuilder()
        .setAwsSecret(this.awsSecret)
        .setKerberosID(this.kerberosID)
        .build();
  }

  @Override
  public String toString() {
    return "awsAccessKey=" + kerberosID + "\nawsSecret=" + awsSecret +
        "\nisDeleted=" + isDeleted + "\ntransactionLogIndex=" +
        transactionLogIndex;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    S3SecretValue that = (S3SecretValue) o;
    return kerberosID.equals(that.kerberosID) &&
        awsSecret.equals(that.awsSecret) && isDeleted == that.isDeleted &&
        transactionLogIndex == that.transactionLogIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(kerberosID, awsSecret, isDeleted, transactionLogIndex);
  }
}
