package org.apache.hadoop.ozone.recon;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.om.helpers.*;
import org.apache.hadoop.ozone.recon.api.NSSummaryEndpoint;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.helpers.OzoneFSUtils.removeTrailingSlashIfNeeded;

public abstract class BucketHandler {

    private static final Logger LOG = LoggerFactory.getLogger(
            NSSummaryEndpoint.class);

    private ContainerManager containerManager;

    private OzoneStorageContainerManager reconSCM;

    @Inject
    private ReconOMMetadataManager omMetadataManager;

    public BucketHandler(OzoneStorageContainerManager reconSCM,
                        ReconOMMetadataManager omMetadataManager) {
        this.reconSCM = reconSCM;
        this.omMetadataManager = omMetadataManager;
        containerManager = reconSCM.getContainerManager();
    }

    public abstract EntityType determineKeyPath(String keyName, long bucketObjectId, BucketLayout bucketLayout) throws IOException;

    public abstract long calculateDUUnderObject(long parentId, BucketLayout bucketLayout) throws IOException;

    public abstract long handleDirectKeys(long parentId, boolean withReplica,
                                  boolean listFile,
                                  List<DUResponse.DiskUsage> duData,
                                  String normalizedPath, BucketLayout bucketLayout) throws IOException;

    public abstract long getDirObjectId(String[] names, BucketLayout bucketLayout) throws IOException;

    public abstract long getDirObjectId(String[] names, int cutoff, BucketLayout bucketLayout) throws IOException;

    public long getKeySizeWithReplication(OmKeyInfo keyInfo) {
        OmKeyLocationInfoGroup locationGroup = keyInfo.getLatestVersionLocations();
        List<OmKeyLocationInfo> keyLocations =
                locationGroup.getBlocksLatestVersionOnly();
        long du = 0L;
        // a key could be too large to fit in one single container
        for (OmKeyLocationInfo location: keyLocations) {
            BlockID block = location.getBlockID();
            ContainerID containerId = new ContainerID(block.getContainerID());
            try {
                int replicationFactor =
                        containerManager.getContainerReplicas(containerId).size();
                long blockSize = location.getLength() * replicationFactor;
                du += blockSize;
            } catch (ContainerNotFoundException cnfe) {
                LOG.warn("Cannot find container {}", block.getContainerID(), cnfe);
            }
        }
        return du;
    }

    /**
     *
     * @param path
     * @param nextLevel
     * @return
     */
    public static String buildSubpath(String path, String nextLevel) {
        String subpath = path;
        if (!subpath.startsWith(OM_KEY_PREFIX)) {
            subpath = OM_KEY_PREFIX + subpath;
        }
        subpath = removeTrailingSlashIfNeeded(subpath);
        if (nextLevel != null) {
            subpath = subpath + OM_KEY_PREFIX + nextLevel;
        }
        return subpath;
    }

    /**
     * Given a existent path, get the bucket object ID.
     * @param names valid path request
     * @return bucket objectID
     * @throws IOException
     */
    public long getBucketObjectId(String[] names) throws IOException {
        String bucketKey = omMetadataManager.getBucketKey(names[0], names[1]);
        OmBucketInfo bucketInfo = omMetadataManager
                .getBucketTable().getSkipCache(bucketKey);
        return bucketInfo.getObjectID();
    }
}
