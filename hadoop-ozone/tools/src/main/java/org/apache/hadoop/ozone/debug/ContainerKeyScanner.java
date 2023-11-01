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

package org.apache.hadoop.ozone.debug;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.kohsuke.MetaInfServices;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Parser for a list of container IDs, to scan for keys.
 */
@CommandLine.Command(
    name = "ckscanner",
    description = "Parse a list of container IDs"
)
@MetaInfServices(SubcommandWithParent.class)
public class ContainerKeyScanner implements Callable<Void>,
    SubcommandWithParent {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerKeyScanner.class);

  @CommandLine.ParentCommand
  private RDBParser parent;

  @CommandLine.Option(names = {"-ids", "--container-ids"},
      split = ",",
      paramLabel = "containerIDs",
      required = true,
      description = "List of container IDs to be used for getting all " +
          "their keys. Example-usage: 1,11,2.(Separated by ',')")
  private List<Long> containerIdList;

  @Override
  public Void call() throws Exception {
    String dbPath = parent.getDbPath();

    ContainerKeyInfoWrapper containerKeyInfoWrapper =
        scanDBForContainerKeys(dbPath, containerIdList);

    jsonPrintList(containerKeyInfoWrapper);

    return null;
  }

  @Override
  public Class<?> getParentType() {
    return RDBParser.class;
  }

  private ContainerKeyInfoWrapper scanDBForContainerKeys(
      String dbPath, List<Long> containerIds)
      throws RocksDBException, IOException {
    List<ContainerKeyInfo> containerKeyInfos = new ArrayList<>();
    long keysProcessed = 0;

    List<ColumnFamilyDescriptor> columnFamilyDescriptors =
        RocksDBUtils.getColumnFamilyDescriptors(parent.getDbPath());
    final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

    try (ManagedRocksDB db = ManagedRocksDB.openReadOnly(parent.getDbPath(),
        columnFamilyDescriptors, columnFamilyHandles)) {
      dbPath = removeTrailingSlashIfNeeded(dbPath);
      DBDefinition dbDefinition = DBDefinitionFactory.getDefinition(
          Paths.get(dbPath), new OzoneConfiguration());
      if (dbDefinition == null) {
        throw new IllegalStateException("Incorrect DB Path");
      }

      DBColumnFamilyDefinition<?, ?> columnFamilyDefinition =
          dbDefinition.getColumnFamily("fileTable");
      if (columnFamilyDefinition == null) {
        throw new IllegalStateException("Table with name fileTable not found");
      }

      ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(
          columnFamilyDefinition.getName().getBytes(UTF_8),
          columnFamilyHandles);
      if (columnFamilyHandle == null) {
        throw new IllegalStateException("columnFamilyHandle is null");
      }

      try (ManagedRocksIterator iterator = new ManagedRocksIterator(
          db.get().newIterator(columnFamilyHandle))) {
        iterator.get().seekToFirst();
        while (iterator.get().isValid()) {
          OmKeyInfo value = ((OmKeyInfo) columnFamilyDefinition.getValueCodec()
              .fromPersistedFormat(iterator.get().value()));
          List<OmKeyLocationInfoGroup> keyLocationVersions =
              value.getKeyLocationVersions();
          if (Objects.isNull(keyLocationVersions)) {
            iterator.get().next();
            keysProcessed++;
            continue;
          }

          keyLocationVersions
              .forEach(omKeyLocationInfoGroup -> omKeyLocationInfoGroup
                  .getLocationVersionMap()
                  .values()
                  .forEach(omKeyLocationInfos -> omKeyLocationInfos
                      .forEach(
                          omKeyLocationInfo -> {
                            if (containerIds.contains(
                                omKeyLocationInfo.getContainerID())) {
                              containerKeyInfos.add(new ContainerKeyInfo(
                                  omKeyLocationInfo.getContainerID(),
                                  value.getVolumeName(),
                                  value.getBucketName(),
                                  value.getKeyName()));
                            }
                          })));
          iterator.get().next();
          keysProcessed++;
        }
      }
    }
    return new ContainerKeyInfoWrapper(keysProcessed, containerKeyInfos);
  }

  private ColumnFamilyHandle getColumnFamilyHandle(
      byte[] name, List<ColumnFamilyHandle> columnFamilyHandles) {
    return columnFamilyHandles
        .stream()
        .filter(
            handle -> {
              try {
                return Arrays.equals(handle.getName(), name);
              } catch (Exception ex) {
                throw new RuntimeException(ex);
              }
            })
        .findAny()
        .orElse(null);
  }

  private String removeTrailingSlashIfNeeded(String dbPath) {
    if (dbPath.endsWith(OzoneConsts.OZONE_URI_DELIMITER)) {
      dbPath = dbPath.substring(0, dbPath.length() - 1);
    }
    return dbPath;
  }

  private void jsonPrintList(ContainerKeyInfoWrapper containerKeyInfoWrapper) {
    List<ContainerKeyInfo> containerKeyInfos =
        containerKeyInfoWrapper.getContainerKeyInfos();
    if (containerKeyInfos.isEmpty()) {
      System.out.println("No keys were found for container IDs: " +
          containerIdList);
      System.out.println(
          "Keys processed: " + containerKeyInfoWrapper.getKeysProcessed());
      return;
    }

    Map<Long, List<ContainerKeyInfo>> infoMap = new HashMap<>();

    for (long id : containerIdList) {
      List<ContainerKeyInfo> tmpList = new ArrayList<>();

      for (ContainerKeyInfo info : containerKeyInfos) {
        if (id == info.getContainerID()) {
          tmpList.add(info);
        }
      }
      infoMap.put(id, tmpList);
    }

    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    String prettyJson = gson.toJson(
        new ContainerKeyInfoResponse(containerKeyInfoWrapper.getKeysProcessed(),
            infoMap));
    System.out.println(prettyJson);
  }
}
