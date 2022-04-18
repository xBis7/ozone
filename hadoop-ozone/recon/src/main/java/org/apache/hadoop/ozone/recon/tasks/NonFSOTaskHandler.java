package org.apache.hadoop.ozone.recon.tasks;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

import java.util.Arrays;
import java.util.Collection;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.*;

public class NonFSOTaskHandler extends NSSummaryTask {

  public NonFSOTaskHandler(ReconNamespaceSummaryManager
                            reconNamespaceSummaryManager) {
    super(reconNamespaceSummaryManager);
  }

  public Collection<String> getTaskTables() {
    return Arrays.asList(new String[]{KEY_TABLE});
  }

  @Override
  public String getTaskName() {
    return "NonFSOTaskHandler";
  }

  @Override
  public Pair<String, Boolean> process(OMUpdateEventBatch events) {

  }

  @Override
  public Pair<String, Boolean> reprocess(OMMetadataManager omMetadataManager) {

  }

}
