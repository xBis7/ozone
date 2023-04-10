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
package org.apache.hadoop.hdds.scm.cli.container;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;

import javax.security.sasl.AuthenticationException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTPS_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTP_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdds.server.http.HttpConfig.getHttpPolicy;
import static org.apache.hadoop.http.HttpServer2.HTTPS_SCHEME;
import static org.apache.hadoop.http.HttpServer2.HTTP_SCHEME;

/**
 * Utility class for retrieving data from Recon endpoints.
 */
public final class ReconEndpointUtils {

  private ReconEndpointUtils() {
  }

  public static String makeHttpCall(StringBuilder url,
                                    boolean isSpnegoEnabled,
                                    ConfigurationSource conf)
      throws Exception {

    System.out.println("Connecting to Recon: " + url + " ...");
    final URLConnectionFactory connectionFactory =
        URLConnectionFactory.newDefaultURLConnectionFactory(
            (Configuration) conf);

    HttpURLConnection httpURLConnection;

    try {
      httpURLConnection = (HttpURLConnection)
          connectionFactory.openConnection(new URL(url.toString()),
              isSpnegoEnabled);
      httpURLConnection.connect();
      int errorCode = httpURLConnection.getResponseCode();
      InputStream inputStream = httpURLConnection.getInputStream();

      if ((errorCode == HTTP_OK) || (errorCode == HTTP_CREATED)) {
        return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
      }

      if (httpURLConnection.getErrorStream() != null) {
        System.out.println("Recon is being initialized. Please wait a moment");
        return null;
      } else {
        System.out.println("Unexpected null in http payload," +
            " while processing request");
      }
      return null;
    } catch (ConnectException ex) {
      System.err.println("Connection Refused. Please make sure the " +
          "Recon Server has been started.");
      return null;
    } catch (AuthenticationException authEx) {
      System.err.println("Authentication Failed. Please make sure you " +
          "have login or disable Ozone security settings.");
      return null;
    }
  }

  public static HashMap<String, Object> getResponseMap(String response) {
    return new Gson().fromJson(response, HashMap.class);
  }

  public static void printNewLines(int cnt) {
    for (int i = 0; i < cnt; ++i) {
      System.out.println();
    }
  }

  public static String getReconWebAddress(OzoneConfiguration conf) {
    final String protocol;
    final HttpConfig.Policy webPolicy = getHttpPolicy(conf);

    final boolean isHostDefault;
    String host;

    if (webPolicy.isHttpsEnabled()) {
      protocol = HTTPS_SCHEME;
      host = conf.get(OZONE_RECON_HTTPS_ADDRESS_KEY,
          OZONE_RECON_HTTPS_ADDRESS_DEFAULT);
      isHostDefault = getHostOnly(host).equals(
          getHostOnly(OZONE_RECON_HTTPS_ADDRESS_DEFAULT));
    } else {
      protocol = HTTP_SCHEME;
      host = conf.get(OZONE_RECON_HTTP_ADDRESS_KEY,
          OZONE_RECON_HTTP_ADDRESS_DEFAULT);
      isHostDefault = getHostOnly(host).equals(
          getHostOnly(OZONE_RECON_HTTP_ADDRESS_DEFAULT));
    }

    if (isHostDefault) {
      // Fallback to <Recon RPC host name>:<Recon http(s) address port>
      final String rpcHost =
          conf.get(OZONE_RECON_ADDRESS_KEY, OZONE_RECON_ADDRESS_DEFAULT);
      host = getHostOnly(rpcHost) + ":" + getPort(host);
    }

    return protocol + "://" + host;
  }

  private static String getHostOnly(String host) {
    return host.split(":", 2)[0];
  }

  private static String getPort(String host) {
    return host.split(":", 2)[1];
  }

  public static boolean isHTTPSEnabled(OzoneConfiguration conf) {
    return getHttpPolicy(conf) == HttpConfig.Policy.HTTPS_ONLY;
  }


  /**
   * Nested class to map the missing containers
   * json array from endpoint
   * /api/v1/containers/unhealthy/MISSING.
   */
  public static class MissingContainerJson {

    private long containerID;
    private String containerState;
    private long unhealthySince;
    private long expectedReplicaCount;
    private long actualReplicaCount;
    private long replicaDeltaCount;
    private String reason;
    private long keys;
    private String pipelineID;
    private ArrayList<ContainerReplicasJson> replicas;

    @JsonCreator
    public MissingContainerJson(
        @JsonProperty("containerID") long containerID,
        @JsonProperty("containerState") String containerState,
        @JsonProperty("unhealthySince") long unhealthySince,
        @JsonProperty("expectedReplicaCount") long expectedReplicaCount,
        @JsonProperty("actualReplicaCount") long actualReplicaCount,
        @JsonProperty("replicaDeltaCount") long replicaDeltaCount,
        @JsonProperty("reason") String reason,
        @JsonProperty("keys") long keys,
        @JsonProperty("pipelineID") String pipelineID,
        @JsonProperty("replicas") ArrayList<ContainerReplicasJson> replicas) {
      this.containerID = containerID;
      this.containerState = containerState;
      this.unhealthySince = unhealthySince;
      this.expectedReplicaCount = expectedReplicaCount;
      this.replicaDeltaCount = replicaDeltaCount;
      this.reason = reason;
      this.keys = keys;
      this.pipelineID = pipelineID;
      this.replicas = replicas;
    }

  }

  /**
   * Nested class to map missing container replicas
   * json array from endpoint
   * /api/v1/containers/unhealthy/MISSING.
   */
  public static class ContainerReplicasJson {

    private long containerId;
    private String datanodeUuid;
    private String datanodeHost;
    private long firstSeenTime;
    private long lastSeenTime;
    private long lastBcsId;

    @JsonCreator
    public ContainerReplicasJson(
        @JsonProperty("containerId") long containerId,
        @JsonProperty("datanodeUuid") String datanodeUuid,
        @JsonProperty("datanodeHost") String datanodeHost,
        @JsonProperty("firstSeenTime") long firstSeenTime,
        @JsonProperty("lastSeenTime") long lastSeenTime,
        @JsonProperty("lastBcsId") long lastBcsId) {
      this.containerId = containerId;
      this.datanodeUuid = datanodeUuid;
      this.datanodeHost = datanodeHost;
      this.firstSeenTime = firstSeenTime;
      this.lastSeenTime = lastSeenTime;
      this.lastBcsId = lastBcsId;
    }
  }

  /**
   * Nested class to map the container keys json array
   * from endpoint /api/v1/containers/ID/keys.
   */
  public static class ContainerKeyJson {

    private String volume;
    private String bucket;
    private String key;

    // Maybe redundant
    private long dataSize;
    private ArrayList<Integer> versions;
    private HashMap<Long, Object> blocks;
    private String creationTime;
    private String modificationTime;

    @JsonCreator
    private ContainerKeyJson(
        @JsonProperty("Volume") String volume,
        @JsonProperty("Bucket") String bucket,
        @JsonProperty("Key") String key,
        @JsonProperty("DataSize") long dataSize,
        @JsonProperty("Versions") ArrayList<Integer> versions,
        @JsonProperty("Blocks") HashMap<Long, Object> blocks,
        @JsonProperty("CreationTime") String creationTime,
        @JsonProperty("ModificationTime") String modificationTime) {
      this.volume = volume;
      this.bucket = bucket;
      this.key = key;
      this.dataSize = dataSize;
      this.versions = versions;
      this.blocks = blocks;
      this.creationTime = creationTime;
      this.modificationTime = modificationTime;
    }

    public String getVolume() {
      return volume;
    }

    public void setVolume(String volume) {
      this.volume = volume;
    }

    public String getBucket() {
      return bucket;
    }

    public void setBucket(String bucket) {
      this.bucket = bucket;
    }

    public String getKey() {
      return key;
    }

    public void setKey(String key) {
      this.key = key;
    }
  }
}
