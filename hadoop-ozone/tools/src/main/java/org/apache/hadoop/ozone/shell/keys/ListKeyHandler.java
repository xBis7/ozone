/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.shell.keys;

import java.io.IOException;
import java.util.Iterator;

import com.google.common.base.Strings;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.shell.ListOptions;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.snapshot.SnapshotHandler;

import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Executes List Keys for a bucket or snapshot.
 */
@Command(name = "list",
    aliases = "ls",
    description = "list all keys in a given bucket or snapshot")
public class ListKeyHandler extends SnapshotHandler {

  @CommandLine.Mixin
  private ListOptions listOptions;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException, OzoneClientException {

    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    String snapshotNameWithIndicator = address.getSnapshotNameWithIndicator();
    String keyPrefix = "";

    if (!Strings.isNullOrEmpty(snapshotNameWithIndicator)) {
      keyPrefix += snapshotNameWithIndicator;

      if (!Strings.isNullOrEmpty(listOptions.getPrefix())) {
        keyPrefix += "/";
      }
    }

    if (!Strings.isNullOrEmpty(listOptions.getPrefix())) {
      keyPrefix += listOptions.getPrefix();
    }

    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = vol.getBucket(bucketName);
    Iterator<? extends OzoneKey> keyIterator = bucket.listKeys(
        keyPrefix, listOptions.getStartItem());

    int maxKeyLimit = listOptions.getLimit();
    int counter = printAsJsonArray(keyIterator, maxKeyLimit);

    // More keys were returned notify about max length
    if (keyIterator.hasNext()) {
      out().println("Listing first " + maxKeyLimit + " entries of the " +
          "result. Use --length (-l) to override max returned keys.");
    } else if (isVerbose()) {
      if (!Strings.isNullOrEmpty(snapshotNameWithIndicator)) {
        // snapshot[0] = .snapshot
        // snapshot[1] = snapshot name
        String[] snapshotValues = snapshotNameWithIndicator.split("/");

        out().printf("Found : %d keys for snapshot %s " +
                "under bucket %s in volume : %s ",
            counter, snapshotValues[1], bucketName, volumeName);
      } else {
        out().printf("Found : %d keys for bucket %s in volume : %s ",
            counter, bucketName, volumeName);
      }
    }
  }

}
