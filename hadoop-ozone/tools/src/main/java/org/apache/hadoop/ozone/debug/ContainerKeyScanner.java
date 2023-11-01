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
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

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

    List<ContainerKeyInfo> containerKeyInfos =
        scanDBForContainerKeys(dbPath, containerIdList);

    jsonPrintList(containerKeyInfos);

    return null;
  }

  @Override
  public Class<?> getParentType() {
    return RDBParser.class;
  }

  private List<ContainerKeyInfo> scanDBForContainerKeys(String dbPath,
      List<Long> containerIdList) {
    List<ContainerKeyInfo> containerKeyInfos = new ArrayList<>();

    for (long id : containerIdList) {
      if (id != 2) {
        ContainerKeyInfo keyInfo = new ContainerKeyInfo(id,
            "vol" + id, "bucket" + id, "key" + id);
        containerKeyInfos.add(keyInfo);

        ContainerKeyInfo keyInfo2 = new ContainerKeyInfo(id,
            "v1ol" + id, "b1ucket" + id, "k1ey" + id);
        containerKeyInfos.add(keyInfo2);
      }
    }
    // TODO: replace testing dummy code with
    //  code logic for scanning the DB dump.

    return containerKeyInfos;
  }

  private void jsonPrintList(List<ContainerKeyInfo> containerKeyInfos) {
    if (containerKeyInfos.isEmpty()) {
      System.out.println("No keys were found for container IDs: " +
                         containerIdList);
      return;
    }

    HashMap<Long, List<ContainerKeyInfo>> infoMap = new HashMap<>();

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
    String prettyJson = gson.toJson(infoMap);
    System.out.println(prettyJson);
  }
}