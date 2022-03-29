package org.apache.hadoop.ozone.recon;

import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class KeyTableHandler extends TableHandler {

    @Inject
    private ReconOMMetadataManager omMetadataManager;

    @Inject
    private ReconNamespaceSummaryManager reconNamespaceSummaryManager;

    public KeyTableHandler(ReconNamespaceSummaryManager reconNamespaceSummaryManager,
                           ReconOMMetadataManager omMetadataManager,
                           OzoneStorageContainerManager reconSCM) {
        super(reconSCM, omMetadataManager);
        this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
        this.omMetadataManager = omMetadataManager;
    }

    @Override
    public EntityType determineKeyPath(String keyName, long bucketObjectId,
                                       BucketLayout bucketLayout) throws IOException {

        java.nio.file.Path keyPath = Paths.get(keyName);
        Iterator<Path> elements = keyPath.iterator();

        long lastKnownParentId = bucketObjectId;
        OmKeyInfo omKeyInfo;
        while (elements.hasNext()) {
            String fileName = elements.next().toString();

            // For example, /vol1/buck1/a/b/c/d/e/file1.txt
            // Look in the KeyTable, if there is a result, check the keyName
            // if it ends with '/' then we have directory
            // else we have a key
            // otherwise we return null, UNKNOWN
            String dbNodeName = omMetadataManager.getOzonePathKey(
                    lastKnownParentId, fileName);

            omKeyInfo = omMetadataManager.getKeyTable(bucketLayout)
                    .getSkipCache(dbNodeName);

            if (omKeyInfo != null) {
                omKeyInfo.setKeyName(keyName);
                if(keyName.endsWith("/")){
                    return EntityType.DIRECTORY;
                }
                else{
                    return EntityType.KEY;
                }
            }
            else{
                return EntityType.UNKNOWN;
            }
        }

        return EntityType.UNKNOWN;
    }

    // KeyTable's key is in the format of "path/fileName"
    // Make use of RocksDB's order to seek to the prefix and avoid full iteration
    @Override
    public long calculateDUUnderObject(long parentId, BucketLayout bucketLayout)
            throws IOException {
        Table keyTable = omMetadataManager.getKeyTable(bucketLayout);

        TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
                iterator = keyTable.iterator();

        // returns "parentId.path" + "/" + "", path to parent object
        String seekPrefix = omMetadataManager.getOzonePathKey(parentId, "");
        iterator.seek(seekPrefix);
        long totalDU = 0L;
        // handle direct keys
        while (iterator.hasNext()) {
            Table.KeyValue<String, OmKeyInfo> kv = iterator.next();
            String dbKey = kv.getKey();
            // since the RocksDB is ordered, seek until the prefix isn't matched
            if (!dbKey.startsWith(seekPrefix)) {
                break;
            }
            OmKeyInfo keyInfo = kv.getValue();
            if (keyInfo != null) {
                totalDU += getKeySizeWithReplication(keyInfo);
            }
        }

        // handle nested keys (DFS)
        NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(parentId);
        // empty bucket
        if (nsSummary == null) {
            return 0;
        }

        Set<Long> subDirIds = nsSummary.getChildDir();
        for (long subDirId: subDirIds) {
            totalDU += calculateDUUnderObject(subDirId, bucketLayout);
        }
        return totalDU;
    }

    /**
     * This method handles disk usage of direct keys.
     * @param parentId parent directory/bucket
     * @param withReplica if withReplica is enabled, set sizeWithReplica
     * for each direct key's DU
     * @param listFile if listFile is enabled, append key DU as a subpath
     * @param duData the current DU data
     * @param normalizedPath the normalized path request
     * @return the total DU of all direct keys
     * @throws IOException IOE
     */
    @Override
    public long handleDirectKeys(long parentId, boolean withReplica,
                                          boolean listFile,
                                          List<DUResponse.DiskUsage> duData,
                                          String normalizedPath, BucketLayout bucketLayout) throws IOException {

        Table keyTable = omMetadataManager.getKeyTable(bucketLayout);
        TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
                iterator = keyTable.iterator();

        String seekPrefix = omMetadataManager.getOzonePathKey(parentId, "");
        iterator.seek(seekPrefix);

        long keyDataSizeWithReplica = 0L;

        while (iterator.hasNext()) {
            Table.KeyValue<String, OmKeyInfo> kv = iterator.next();
            String dbKey = kv.getKey();

            if (!dbKey.startsWith(seekPrefix)) {
                break;
            }
            OmKeyInfo keyInfo = kv.getValue();
            if (keyInfo != null) {
                DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
                String subpath = buildSubpath(normalizedPath,
                        keyInfo.getFileName());
                diskUsage.setSubpath(subpath);
                diskUsage.setKey(true);
                diskUsage.setSize(keyInfo.getDataSize());

                if (withReplica) {
                    long keyDU = getKeySizeWithReplication(keyInfo);
                    keyDataSizeWithReplica += keyDU;
                    diskUsage.setSizeWithReplica(keyDU);
                }
                // list the key as a subpath
                if (listFile) {
                    duData.add(diskUsage);
                }
            }
        }
        return keyDataSizeWithReplica;
    }

    /**
     * Given a valid path request for a directory,
     * return the directory object ID.
     * @param names parsed path request in a list of names
     * @return directory object ID
     */
    @Override
    public long getDirObjectId(String[] names, BucketLayout bucketLayout) throws IOException {
        return getDirObjectId(names, names.length, bucketLayout);
    }

    /**
     * Given a valid path request and a cutoff length where should be iterated
     * up to.
     * return the directory object ID for the object at the cutoff length
     * @param names parsed path request in a list of names
     * @param cutoff cannot be larger than the names' length. If equals,
     *               return the directory object id for the whole path
     * @return directory object ID
     */
    @Override
    public long getDirObjectId(String[] names, int cutoff, BucketLayout bucketLayout) throws IOException {
        long dirObjectId = getBucketObjectId(names);
        String dirKey;
        for (int i = 2; i < cutoff; ++i) {
            dirKey = omMetadataManager.getOzonePathKey(dirObjectId, names[i]);
            OmKeyInfo dirInfo =
                    omMetadataManager.getKeyTable(bucketLayout).getSkipCache(dirKey);
            if(!dirInfo.getKeyName().endsWith("/")){
                dirObjectId = dirInfo.getObjectID();
            }
        }
        return dirObjectId;
    }
}
