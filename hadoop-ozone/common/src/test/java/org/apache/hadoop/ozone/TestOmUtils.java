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

package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.ozone.OmUtils.getOmHostsFromConfig;
import static org.apache.hadoop.ozone.OmUtils.getOzoneManagerServiceId;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_INTERNAL_SERVICE_ID;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODES_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Unit tests for {@link OmUtils}.
 */
@Timeout(60)
public class TestOmUtils {

  @TempDir
  private Path folder;

  @Test
  public void createOMDirCreatesDirectoryIfNecessary() throws IOException {
    File parent = folder.toFile();
    File omDir = new File(new File(parent, "sub"), "dir");
    assertFalse(omDir.exists());

    OmUtils.createOMDir(omDir.getAbsolutePath());

    assertTrue(omDir.exists());
  }

  @Test
  public void createOMDirDoesNotThrowIfAlreadyExists() throws IOException {
    File omDir = folder.toFile();
    assertTrue(omDir.exists());

    OmUtils.createOMDir(omDir.getAbsolutePath());

    assertTrue(omDir.exists());
  }

  @Test
  public void createOMDirThrowsIfCannotCreate() {
    assertThrows(IllegalArgumentException.class, () -> {
      File parent = folder.toFile();
      File omDir = new File(new File(parent, "sub"), "dir");
      assumeTrue(parent.setWritable(false, false));

      OmUtils.createOMDir(omDir.getAbsolutePath());
      // expecting exception
    });
  }

  @Test
  public void testGetOmHAAddressesById() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_OM_SERVICE_IDS_KEY, "ozone1");
    conf.set("ozone.om.nodes.ozone1", "node1,node2,node3");
    conf.set("ozone.om.address.ozone1.node1", "1.1.1.1");
    conf.set("ozone.om.address.ozone1.node2", "1.1.1.2");
    conf.set("ozone.om.address.ozone1.node3", "1.1.1.3");
    Map<String, List<InetSocketAddress>> addresses =
        OmUtils.getOmHAAddressesById(conf);
    assertFalse(addresses.isEmpty());
    List<InetSocketAddress> rpcAddrs = addresses.get("ozone1");
    assertFalse(rpcAddrs.isEmpty());
    assertTrue(rpcAddrs.stream().anyMatch(
        a -> a.getAddress().getHostAddress().equals("1.1.1.1")));
    assertTrue(rpcAddrs.stream().anyMatch(
        a -> a.getAddress().getHostAddress().equals("1.1.1.2")));
    assertTrue(rpcAddrs.stream().anyMatch(
        a -> a.getAddress().getHostAddress().equals("1.1.1.3")));
  }

  @Test
  public void testGetOzoneManagerServiceId() throws IOException {

    // If the above is not configured, look at 'ozone.om.service.ids'.
    // If no config is set, return null. (Non HA)
    OzoneConfiguration configuration = new OzoneConfiguration();
    assertNull(getOzoneManagerServiceId(configuration));

    // Verify 'ozone.om.internal.service.id' takes precedence
    configuration.set(OZONE_OM_INTERNAL_SERVICE_ID, "om1");
    configuration.set(OZONE_OM_SERVICE_IDS_KEY, "om2,om1");
    String id = getOzoneManagerServiceId(configuration);
    assertEquals("om1", id);

    configuration.set(OZONE_OM_SERVICE_IDS_KEY, "om2,om3");
    try {
      getOzoneManagerServiceId(configuration);
      fail();
    } catch (IOException ioEx) {
      assertTrue(ioEx.getMessage()
          .contains("Cannot find the internal service id om1 in [om2, om3]"));
    }

    // When internal service ID is not defined.
    // Verify if count(ozone.om.service.ids) == 1, return that id.
    configuration = new OzoneConfiguration();
    configuration.set(OZONE_OM_SERVICE_IDS_KEY, "om2");
    id = getOzoneManagerServiceId(configuration);
    assertEquals("om2", id);

    // Verify if more than count(ozone.om.service.ids) > 1 and internal
    // service id is not defined, throw exception
    configuration.set(OZONE_OM_SERVICE_IDS_KEY, "om2,om1");
    try {
      getOzoneManagerServiceId(configuration);
      fail();
    } catch (IOException ioEx) {
      assertTrue(ioEx.getMessage()
          .contains("More than 1 OzoneManager ServiceID (ozone.om.service" +
              ".ids) configured"));
    }
  }

  @Test
  public void checkMaxTransactionID() {
    assertEquals((long) (Math.pow(2, 54) - 2), OmUtils.MAX_TRXN_ID);
  }

  @Test
  public void testGetOmHostsFromConfig() {
    OzoneConfiguration conf = new OzoneConfiguration();
    String serviceId = "myOmId";

    conf.set(OZONE_OM_NODES_KEY  + "." + serviceId, "omA,omB,omC");
    conf.set(OZONE_OM_ADDRESS_KEY + "." + serviceId + ".omA", "omA-host:9861");
    conf.set(OZONE_OM_ADDRESS_KEY + "." + serviceId + ".omB", "omB-host:9861");
    conf.set(OZONE_OM_ADDRESS_KEY + "." + serviceId + ".omC", "omC-host:9861");

    String serviceId2 = "myOmId2";
    conf.set(OZONE_OM_NODES_KEY  + "." + serviceId2, "om1");
    conf.set(OZONE_OM_ADDRESS_KEY + "." + serviceId2 + ".om1", "om1-host");

    Set<String> hosts = getOmHostsFromConfig(conf, serviceId);
    assertEquals(3, hosts.size());
    assertTrue(hosts.contains("omA-host"));
    assertTrue(hosts.contains("omB-host"));
    assertTrue(hosts.contains("omC-host"));

    hosts = getOmHostsFromConfig(conf, serviceId2);
    assertEquals(1, hosts.size());
    assertTrue(hosts.contains("om1-host"));

    assertTrue(getOmHostsFromConfig(conf, "newId").isEmpty());
  }

  @Test
  public void testgetOmSocketAddress() {
    final OzoneConfiguration conf = new OzoneConfiguration();

    // First try a client address with just a host name. Verify it falls
    // back to the default port.
    conf.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY, "1.2.3.4");
    InetSocketAddress addr = OmUtils.getOmAddress(conf);
    assertThat(addr.getHostString(), is("1.2.3.4"));
    assertThat(addr.getPort(), is(OMConfigKeys.OZONE_OM_PORT_DEFAULT));

    // Next try a client address with just a host name and port. Verify the port
    // is ignored and the default OM port is used.
    conf.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY, "1.2.3.4:100");
    addr = OmUtils.getOmAddress(conf);
    assertThat(addr.getHostString(), is("1.2.3.4"));
    assertThat(addr.getPort(), is(100));

    // Assert the we are able to use default configs if no value is specified.
    conf.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY, "");
    addr = OmUtils.getOmAddress(conf);
    assertThat(addr.getHostString(), is("0.0.0.0"));
    assertThat(addr.getPort(), is(OMConfigKeys.OZONE_OM_PORT_DEFAULT));
  }
}

