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

import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

import java.util.ArrayList;
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
public class ContainerKeyScanner implements Callable<Void>, SubcommandWithParent {

  @CommandLine.ParentCommand
  private RDBParser parent;

  @CommandLine.Option(names = {"-ids", "--container-ids"},
      split = ",",
      paramLabel = "containerIDs",
      description = "List of container IDs to be used for getting all " +
                    "their keys. Example-usage: 1,11,2.(Separated by ',')")
  private List<Long> containerIdList;

  @Override
  public Void call() throws Exception {
    String dbPath = parent.getDbPath();

    List<ContainerKeyInfo> containerKeyInfos =
        scanDBForContainerKeys(dbPath, containerIdList);

    jsonPrettyPrintList(containerKeyInfos);

    return null;
  }

  @Override
  public Class<?> getParentType() {
    return RDBParser.class;
  }

  private List<ContainerKeyInfo> scanDBForContainerKeys(String dbPath,
      List<Long> containerIdList) {

    // TODO: code logic for scanning the DB bump.

    return new ArrayList<>();
  }

  private void jsonPrettyPrintList(List<ContainerKeyInfo> containerKeyInfos) {

  }
}