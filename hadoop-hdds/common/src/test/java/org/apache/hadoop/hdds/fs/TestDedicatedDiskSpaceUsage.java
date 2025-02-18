/*
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
package org.apache.hadoop.hdds.fs;

import org.apache.hadoop.fs.FileUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.apache.hadoop.hdds.fs.TestDU.createFile;
import static org.apache.ozone.test.GenericTestUtils.getTestDir;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link DedicatedDiskSpaceUsage}.
 */
class TestDedicatedDiskSpaceUsage {

  private static final File DIR =
      getTestDir(TestDedicatedDiskSpaceUsage.class.getSimpleName());

  private static final int FILE_SIZE = 1024;

  @BeforeEach
  void setUp() {
    FileUtil.fullyDelete(DIR);
    assertTrue(DIR.mkdirs());
  }

  @AfterEach
  void tearDown() {
    FileUtil.fullyDelete(DIR);
  }

  @Test
  void testGetUsed() throws IOException {
    File file = new File(DIR, "data");
    createFile(file, FILE_SIZE);
    SpaceUsageSource subject = new DedicatedDiskSpaceUsage(DIR);

    // condition comes from TestDFCachingGetSpaceUsed in Hadoop Common
    assertThat(subject.getUsedSpace()).isGreaterThanOrEqualTo(FILE_SIZE - 20);
  }

}
