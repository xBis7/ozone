package org.apache.hadoop.hdds.scm.cli.container;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.util.List;

@CommandLine.Command(
    name = "read",
    description = "Get information directly from the state map",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ReadSubcommand extends ScmSubcommand {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReadSubcommand.class);

  @CommandLine.Parameters(description = "Decimal id of the container.")
  private long containerID;

  @Override
  protected void execute(ScmClient client) throws IOException {
    ContainerInfo info = client.getContainer(containerID);
    LOG.info("xbis: ID: " + info.getContainerID() + " | keyNum: " +
             info.getNumberOfKeys() + " | state: " +
             info.getState() + " | usedBytes: " +
             info.getUsedBytes());

    List<ContainerReplicaInfo> containerReplicas =
        client.getContainerReplicas(containerID);

    for (ContainerReplicaInfo replicaInfo : containerReplicas) {
      LOG.info("xbis: replica: " + replicaInfo.toString());
    }
  }
}
