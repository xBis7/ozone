package org.apache.hadoop.ozone.debug;

public class ContainerKeyInfo {

  private final long containerID;
  private final String volumeName;
  private final String bucketName;
  private final String keyName;

  public ContainerKeyInfo(
      long containerID, String volumeName,
      String bucketName, String keyName) {
    this.containerID = containerID;
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.keyName = keyName;
  }

  public long getContainerID() {
    return containerID;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getKeyName() {
    return keyName;
  }
}
