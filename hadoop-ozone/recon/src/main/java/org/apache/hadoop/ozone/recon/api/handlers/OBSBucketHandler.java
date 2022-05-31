/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.recon.api.handlers;

import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * Class for handling OBS buckets.
 */
public class OBSBucketHandler extends BucketHandler {

  private OmBucketInfo omBucketInfo;
  private String vol;
  private String bucket;

  public OBSBucketHandler(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager omMetadataManager,
      OzoneStorageContainerManager reconSCM,
      OmBucketInfo bucketInfo) {
    super(reconNamespaceSummaryManager, omMetadataManager, reconSCM);
    this.omBucketInfo = bucketInfo;
    this.vol = omBucketInfo.getVolumeName();
    this.bucket = omBucketInfo.getBucketName();
  }

  /**
   * Helper function to check if a path is a directory, key, or invalid.
   * @param keyName key name
   * @return DIRECTORY, KEY, or UNKNOWN
   * @throws IOException
   */
  @Override
  public EntityType determineKeyPath(String keyName, long bucketObjectId)
      throws IOException {

    // For example, /vol1/buck1/a/b/c/d/e/file1.txt
    // For Object Store buckets, in the KeyTable
    // there are no directories
    // keys are only the files
    // /vol1/buck1/a/ or
    // /vol1/buck1/a/b/ or
    // /vol1/buck1/a/b/c/ or
    // /vol1/buck1/a/b/c/d/ or
    // /vol1/buck1/a/b/c/d/e/ aren't stored
    String key = OM_KEY_PREFIX + vol +
        OM_KEY_PREFIX + bucket +
        OM_KEY_PREFIX + keyName;
    OmKeyInfo omKeyInfo = getOmMetadataManager()
        .getKeyTable(getBucketLayout()).get(key);

    if (omKeyInfo != null) {
      return EntityType.KEY;
    } else {
      return EntityType.UNKNOWN;
    }
  }

  @Override
  public long calculateDUUnderObject(long parentId)
      throws IOException {
    Table keyTable = getOmMetadataManager().getKeyTable(getBucketLayout());

    TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
        iterator = keyTable.iterator();

    String seekPrefix =
        OM_KEY_PREFIX + vol + OM_KEY_PREFIX + bucket + OM_KEY_PREFIX;

    // handle nested keys (DFS)
    NSSummary nsSummary = getReconNamespaceSummaryManager()
        .getNSSummary(parentId);
    // empty bucket
    if (nsSummary == null) {
      return 0;
    }

    iterator.seek(seekPrefix);
    long totalDU = 0L;
    // handle direct keys
    while (iterator.hasNext()) {
      Table.KeyValue<String, OmKeyInfo> kv = iterator.next();
      String dbKey = kv.getKey();
      // since the RocksDB is ordered, seek until the prefix isn't matched
      if (!dbKey.startsWith(seekPrefix)) {
        break;
      }
      OmKeyInfo keyInfo = kv.getValue();
      if (keyInfo != null) {
        totalDU += getKeySizeWithReplication(keyInfo);
      }
    }

    return totalDU;
  }

  /**
   * This method handles disk usage of direct keys.
   * @param parentId parent bucket
   * @param withReplica if withReplica is enabled, set sizeWithReplica
   * for each direct key's DU
   * @param listFile if listFile is enabled, append key DU as a subpath
   * @param duData the current DU data
   * @param normalizedPath the normalized path request
   * @return the total DU of all direct keys
   * @throws IOException IOE
   */
  @Override
  public long handleDirectKeys(long parentId, boolean withReplica,
                               boolean listFile,
                               List<DUResponse.DiskUsage> duData,
                               String normalizedPath) throws IOException {

    Table keyTable = getOmMetadataManager().getKeyTable(getBucketLayout());
    TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
        iterator = keyTable.iterator();

    String seekPrefix =
        OM_KEY_PREFIX + vol + OM_KEY_PREFIX + bucket + OM_KEY_PREFIX;

    NSSummary nsSummary = getReconNamespaceSummaryManager()
        .getNSSummary(parentId);
    // empty bucket
    if (nsSummary == null) {
      return 0;
    }

    iterator.seek(seekPrefix);

    long keyDataSizeWithReplica = 0L;

    while (iterator.hasNext()) {
      Table.KeyValue<String, OmKeyInfo> kv = iterator.next();
      String dbKey = kv.getKey();

      if (!dbKey.startsWith(seekPrefix)) {
        break;
      }
      OmKeyInfo keyInfo = kv.getValue();
      if (keyInfo != null) {
        DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
        String subpath = buildSubpath(normalizedPath,
            keyInfo.getFileName());
        diskUsage.setSubpath(subpath);
        diskUsage.setKey(true);
        diskUsage.setSize(keyInfo.getDataSize());

        if (withReplica) {
          long keyDU = getKeySizeWithReplication(keyInfo);
          keyDataSizeWithReplica += keyDU;
          diskUsage.setSizeWithReplica(keyDU);
        }
        // list the key as a subpath
        if (listFile) {
          duData.add(diskUsage);
        }
      }
    }

    return keyDataSizeWithReplica;
  }

  @Override
  public long getDirObjectId(String[] names) throws IOException {
    return getDirObjectId(names, names.length);
  }

  /**
   * In an OBS bucket directories are not stored as keys.
   * Directories only exist in a key's name prefix.
   * There are no directory IDs in an OBS bucket.
   * @return 0
   */
  @Override
  public long getDirObjectId(String[] names, int cutoff) throws IOException {
    return 0;
  }

  public int getTotalDirCountUnderPrefix() throws IOException {
    Table keyTable = getOmMetadataManager().getKeyTable(getBucketLayout());
    TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
        iterator = keyTable.iterator();

    String seekPrefix =
        OM_KEY_PREFIX + vol + OM_KEY_PREFIX + bucket + OM_KEY_PREFIX;

    iterator.seek(seekPrefix);

    int totalDirCount = 0;

    ArrayList<String> dirList = new ArrayList<>();

    while (iterator.hasNext()) {
      Table.KeyValue<String, OmKeyInfo> kv = iterator.next();
      String dbKey = kv.getKey();

      if (!dbKey.startsWith(seekPrefix)) {
        break;
      }
      OmKeyInfo keyInfo = kv.getValue();
      if (keyInfo != null) {
        String key = keyInfo.getKeyName();

        // keyNames.length > 1, then there are dirs
        // the last element of the array is the key
        String[] keyNames = key.split(OM_KEY_PREFIX);

        for (int i = 0; i < (keyNames.length - 1); i++) {
          if (!dirList.contains(keyNames[i])) {
            dirList.add(keyNames[i]);
          }
        }
        totalDirCount = dirList.size();
      }
    }

    return totalDirCount;
  }

  private static BucketLayout getBucketLayout() {
    return BucketLayout.OBJECT_STORE;
  }

}
