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
package org.apache.hadoop.ozone.recon.tasks;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.WithParentObjectId;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Iterator;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;

/**
 * Class for handling OBS specific tasks.
 */
public class OBSNSSummaryTask extends NSSummaryTask {

  private BucketLayout bucketLayout;

  private ReconOMMetadataManager reconOMMetadataManager;

  private static final Logger LOG =
      LoggerFactory.getLogger(OBSNSSummaryTask.class);

  @Inject
  public OBSNSSummaryTask(ReconNamespaceSummaryManager
                                 reconNamespaceSummaryManager,
                             ReconOMMetadataManager
                                 reconOMMetadataManager) {
    super(reconNamespaceSummaryManager);
    this.reconOMMetadataManager = reconOMMetadataManager;
    this.bucketLayout = BucketLayout.OBJECT_STORE;
  }

  @Override
  public String getTaskName() {
    return "OBSNSSummaryTask";
  }

  @Override
  public Pair<String, Boolean> process(OMUpdateEventBatch events) {
    Iterator<OMDBUpdateEvent> eventIterator = events.getIterator();

    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, ? extends
          WithParentObjectId> omdbUpdateEvent = eventIterator.next();
      OMDBUpdateEvent.OMDBUpdateAction action = omdbUpdateEvent.getAction();

      // we only process updates on OM's KeyTable
      String table = omdbUpdateEvent.getTable();
      boolean updateOnKeyTable = table.equals(KEY_TABLE);
      if (!updateOnKeyTable) {
        continue;
      }

      String updatedKey = omdbUpdateEvent.getKey();

      try {
        OMDBUpdateEvent<String, OmKeyInfo> keyTableUpdateEvent =
            (OMDBUpdateEvent<String, OmKeyInfo>) omdbUpdateEvent;
        OmKeyInfo updatedKeyInfo = keyTableUpdateEvent.getValue();
        OmKeyInfo oldKeyInfo = keyTableUpdateEvent.getOldValue();

        if (updatedKeyInfo != null) {
          setKeyParentID(updatedKeyInfo);

          switch (action) {
          case PUT:
            writeOmKeyInfoOnNamespaceDB(updatedKeyInfo);
            break;

          case DELETE:
            deleteOmKeyInfoOnNamespaceDB(updatedKeyInfo);
            break;

          case UPDATE:
            if (oldKeyInfo != null) {
              // delete first, then put
              setKeyParentID(oldKeyInfo);
              deleteOmKeyInfoOnNamespaceDB(oldKeyInfo);
            } else {
              LOG.warn("Update event does not have the old keyInfo for {}.",
                  updatedKey);
            }
            writeOmKeyInfoOnNamespaceDB(updatedKeyInfo);
            break;

          default:
            LOG.debug("Skipping DB update event : {}",
                omdbUpdateEvent.getAction());
          }
        } else {
          LOG.error("UpdatedKeyInfo for OBSNSSummaryTask is null");
        }
      } catch (IOException ioEx) {
        LOG.error("Unable to process Namespace Summary data in Recon DB. ",
            ioEx);
        return new ImmutablePair<>(getTaskName(), false);
      }
    }
    LOG.info("Completed a process run of OBSNSSummaryTask");
    return new ImmutablePair<>(getTaskName(), true);
  }

  @Override
  public Pair<String, Boolean> reprocess(OMMetadataManager omMetadataManager) {
    try {
      // reinit Recon RocksDB's namespace CF.
      getReconNamespaceSummaryManager().clearNSSummaryTable();

      Table keyTable = omMetadataManager.getKeyTable(bucketLayout);

      TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
          keyTableIter = keyTable.iterator();

      while (keyTableIter.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = keyTableIter.next();
        OmKeyInfo keyInfo = kv.getValue();

        if (keyInfo != null) {
          setKeyParentID(keyInfo);
          writeOmKeyInfoOnNamespaceDB(keyInfo);
        } else {
          LOG.error("Reprocess KeyInfo for OBSNSSummaryTask is null");
        }
      }
    } catch (IOException ioEx) {
      LOG.error("Unable to reprocess Namespace Summary data in Recon DB. ",
          ioEx);
      return new ImmutablePair<>(getTaskName(), false);
    }

    LOG.info("Completed a reprocess run of OBSNSSummaryTask");
    return new ImmutablePair<>(getTaskName(), true);
  }

  /**
   * For an OBS key the parent object will always be the bucket.
   * @param keyInfo
   * @throws IOException
   */
  private void setKeyParentID(OmKeyInfo keyInfo) throws IOException {
    String bucketKey = reconOMMetadataManager
        .getBucketKey(keyInfo.getVolumeName(), keyInfo.getBucketName());
    OmBucketInfo parentBucketInfo =
        reconOMMetadataManager.getBucketTable().get(bucketKey);

    if (parentBucketInfo != null) {
      keyInfo.setParentObjectID(parentBucketInfo.getObjectID());
    } else {
      LOG.error("ParentBucketInfo for OBSNSSummaryTask is null");
    }
  }
}
