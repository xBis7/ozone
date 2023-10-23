package org.apache.hadoop.ozone.admin.om;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.cli.container.CreateSubcommand;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.InputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

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
 * Delete all container keys.
 */
@CommandLine.Command(
    name = "deleteKeys",
    description = "Delete all container keys",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DeleteKeysSubCommand implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(CreateSubcommand.class);

  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Option(names = {"-id", "--service-id"},
      description = "OM Service ID",
      defaultValue = "omservice",
      required = false)
  private String omServiceId;

  @CommandLine.Parameters(description = "Decimal id of the container.")
  private long containerID;

  private OzoneManagerProtocol ozoneManagerClient;

  @Override
  public Void call() throws Exception {
    OzoneConfiguration configuration = parent.getParent().getOzoneConf();
    ozoneManagerClient = parent.createOmClient(omServiceId);

    List<ContainerKey> containerKeys =
        getContainerKeysFromRecon(containerID, configuration);
    for (ContainerKey key : containerKeys) {
      OmKeyArgs keyArgs = new OmKeyArgs.Builder()
                              .setVolumeName(key.getVolume())
                              .setBucketName(key.getBucket())
                              .setKeyName(key.getKey())
                              .setDataSize(key.getDataSize())
                              .build();
      ozoneManagerClient.deleteKey(keyArgs);
    }
    return null;
  }

  private List<ContainerKey> getContainerKeysFromRecon(long containerId,
      OzoneConfiguration conf)
      throws JsonProcessingException {
    StringBuilder urlBuilder = new StringBuilder();
    urlBuilder.append(getReconWebAddress(conf))
        .append("/api/v1/containers/")
        .append(containerId)
        .append("/keys");
    String containerKeysResponse = "";
    try {
      containerKeysResponse = makeHttpCall(urlBuilder, conf);
    } catch (Exception e) {
      LOG.error("Error getting container keys response from Recon");
    }

    if (Strings.isNullOrEmpty(containerKeysResponse)) {
      LOG.info("Container keys response from Recon is empty");
      // Return empty list
      return new LinkedList<>();
    }

    // Get the keys JSON array
    String keysJsonArray = containerKeysResponse.substring(
        containerKeysResponse.indexOf("keys\":") + 6,
        containerKeysResponse.length() - 1);

    final ObjectMapper objectMapper = new ObjectMapper();

    return objectMapper.readValue(keysJsonArray,
        new TypeReference<List<ContainerKey>>() { });
  }

  private String makeHttpCall(StringBuilder url, OzoneConfiguration conf)
      throws Exception {

    System.out.println("Connecting to Recon: " + url + " ...");
    final URLConnectionFactory connectionFactory =
        URLConnectionFactory.newDefaultURLConnectionFactory(conf);

    boolean isSpnegoEnabled = isHTTPSEnabled(conf);
    HttpURLConnection httpURLConnection;

    try {
      httpURLConnection = (HttpURLConnection)
                              connectionFactory
                                  .openConnection(new URL(url.toString()),
                                      isSpnegoEnabled);
      httpURLConnection.connect();
      int errorCode = httpURLConnection.getResponseCode();
      InputStream inputStream = httpURLConnection.getInputStream();

      if ((errorCode == HTTP_OK) || (errorCode == HTTP_CREATED)) {
        return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
      }

      if (httpURLConnection.getErrorStream() != null) {
        System.out.println("Recon is being initialized. " +
                           "Please wait a moment");
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
    }
  }

  private String getReconWebAddress(OzoneConfiguration conf) {
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

  private String getHostOnly(String host) {
    return host.split(":", 2)[0];
  }

  private String getPort(String host) {
    return host.split(":", 2)[1];
  }

  private boolean isHTTPSEnabled(OzoneConfiguration conf) {
    return getHttpPolicy(conf) == HttpConfig.Policy.HTTPS_ONLY;
  }
}
