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

package org.apache.hadoop.ozone.om.snapshot;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.ozone.om.OmSnapshotManager;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.ozone.OzoneConsts.OM_CHECKPOINT_DIR;

/**
 * Ozone Manager Snapshot Utilities.
 */
public final class OmSnapshotUtils {

  public static final String DATA_PREFIX = "data";
  public static final String DATA_SUFFIX = "txt";

  private OmSnapshotUtils() { }

  /**
   * Get the filename without the introductory metadata directory.
   *
   * @param truncateLength Length to remove.
   * @param file           File to remove prefix from.
   * @return Truncated string.
   */
  public static String truncateFileName(int truncateLength, Path file) {
    return file.toString().substring(truncateLength);
  }

  /**
   * Get the INode for file.
   *
   * @param file File whose INode is to be retrieved.
   * @return INode for file.
   */
  @VisibleForTesting
  public static Object getINode(Path file) throws IOException {
    return Files.readAttributes(file, BasicFileAttributes.class).fileKey();
  }

  /**
   * Create file of links to add to tarball.
   * Format of entries are either:
   * dir1/fileTo fileFrom
   * for files in active db or:
   * dir1/fileTo dir2/fileFrom
   * for files in another directory, (either another snapshot dir or
   * sst compaction backup directory)
   *
   * @param truncateLength - Length of initial path to trim in file path.
   * @param hardLinkFiles  - Map of link->file paths.
   * @return Path to the file of links created.
   */
  public static Path createHardLinkList(int truncateLength,
                                        Map<Path, Path> hardLinkFiles)
      throws IOException {
    Path data = Files.createTempFile(DATA_PREFIX, DATA_SUFFIX);
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<Path, Path> entry : hardLinkFiles.entrySet()) {
      String fixedFile = truncateFileName(truncateLength, entry.getValue());
      // If this file is from the active db, strip the path.
      if (fixedFile.startsWith(OM_CHECKPOINT_DIR)) {
        Path f = Paths.get(fixedFile).getFileName();
        if (f != null) {
          fixedFile = f.toString();
        }
      }
      sb.append(truncateFileName(truncateLength, entry.getKey())).append("\t")
          .append(fixedFile).append("\n");
    }
    Files.write(data, sb.toString().getBytes(StandardCharsets.UTF_8));

    // Read all the data and print the thread.
    File hardLinkFile = data.toFile();
    // Read file.
    try (Stream<String> s = Files.lines(hardLinkFile.toPath())) {
      List<String> lines = s.collect(Collectors.toList());

      // Create a link for each line.
      for (String l : lines) {
        System.out.println("xbis: Thread: " + Thread.currentThread().getName() + " | create hardlink file, line: " + l);
      }
    }

    return data;
  }

  /**
   * Create hard links listed in OM_HARDLINK_FILE.
   *
   * @param dbPath Path to db to have links created.
   */
  public static void createHardLinks(Path dbPath) throws IOException {
    File hardLinkFile =
        new File(dbPath.toString(), OmSnapshotManager.OM_HARDLINK_FILE);
    if (hardLinkFile.exists()) {
      // Read file.
      try (Stream<String> s = Files.lines(hardLinkFile.toPath())) {
        List<String> lines = s.collect(Collectors.toList());

        long counter = 0;
        List<String> lines_partial_copy = lines.subList(3, lines.size() - 2);
        // Create a link for each line.
        for (String l : lines_partial_copy) {
          System.out.println("xbis: Thread: " + Thread.currentThread().getName() + " | create hardlinks, line: " + l);
          String from = l.split("\t")[1];
          String to = l.split("\t")[0];
          Path fullFromPath = Paths.get(dbPath.toString(), from);
          Path fullToPath = Paths.get(dbPath.toString(), to);
          // Make parent dir if it doesn't exist.
          Path parent = fullToPath.getParent();
          if ((parent != null) && (!parent.toFile().exists())) {
            if (!parent.toFile().mkdirs()) {
              throw new IOException(
                  "Failed to create directory: " + parent.toString());
            }
          }
          Files.createLink(fullToPath, fullFromPath);
          counter++;
        }
        System.out.println("xbis: counter: " + counter + " | file.lines.size: " + lines.size() +
            " | lines_partial_copy.size: " + lines_partial_copy.size());
        if (!hardLinkFile.delete()) {
          throw new IOException("Failed to delete: " + hardLinkFile);
        }
      }
    }
  }

  /**
   * Link each of the files in oldDir to newDir.
   *
   * @param oldDir The dir to create links from.
   * @param newDir The dir to create links to.
   */
  public static void linkFiles(File oldDir, File newDir, boolean beforeInstall) throws IOException {
    int truncateLength = oldDir.toString().length() + 1;
    List<String> oldDirList;
    try (Stream<Path> files = Files.walk(oldDir.toPath())) {
      oldDirList = files.map(Path::toString).
          // Don't copy the directory itself
          filter(s -> !s.equals(oldDir.toString())).
          // Remove the old path
          map(s -> s.substring(truncateLength)).
          sorted().
          collect(Collectors.toList());
    }
    long linkCounter = 0;
    long dirCounter = 0;
    long parentDirCounter = 0;

//    if (!beforeInstall) {
//      oldDirList.remove(oldDirList.size() / 5);
//      oldDirList.remove(oldDirList.size() / 4);
//      oldDirList.remove(oldDirList.size() / 3);
//      oldDirList.remove(oldDirList.size() / 2);
//    }
    for (String s: oldDirList) {
      File oldFile = new File(oldDir, s);
      File newFile = new File(newDir, s);
      File newParent = newFile.getParentFile();
      if (!newParent.exists()) {
        parentDirCounter++;
        if (!newParent.mkdirs()) {
          throw new IOException("Directory create fails: " + newParent);
        }
      }
      if (oldFile.isDirectory()) {
        dirCounter++;
        if (!newFile.mkdirs()) {
          throw new IOException("Directory create fails: " + newFile);
        }
      } else {
        Files.createLink(newFile.toPath(), oldFile.toPath());
        linkCounter++;
      }
    }
    if (!beforeInstall) {
      System.out.println("xbis: after checkpoint install: " +
          "\nlinkCounter: " + linkCounter +
          "\ndirCounter: " + dirCounter +
          "\nparentDirCounter: " + parentDirCounter +
          "\noldDirList.size: " + oldDirList.size());
    }
  }
}
