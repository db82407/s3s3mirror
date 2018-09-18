package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;


@Slf4j
public class CopyMaster extends KeyMaster {
    private final MirrorOptions options;
    private Storage gs_storage;

    public CopyMaster(AmazonS3Client client, MirrorContext context, BlockingQueue<Runnable> workQueue, ThreadPoolExecutor executorService) {
        super(client, context, workQueue, executorService);

        options = context.getOptions();
        String bucket = options.getSourceBucket();

        try {
            String status = client.getBucketVersioningConfiguration(bucket).getStatus();
            log.info(String.format("Bucket(%s) versioning = %s", bucket, status));
        } catch (Exception e) {
            log.error("Failed to get bucket versioning for bucket: " + bucket);
            throw e;
        }

        if (!options.fixEtags()) {
            String dest = options.getDestinationBucket();
            if (dest.startsWith("gs://")) {
                gs_storage = StorageOptions.getDefaultInstance().getService();
            }
        }

    }

    protected String getPrefix(MirrorOptions options) {
        return options.getPrefix();
    }

    protected String getBucket(MirrorOptions options) {
        return options.getSourceBucket();
    }

    protected Runnable getTask(S3ObjectSummary summary) {
        if (options.fixEtags()) {
            if (summary.getETag().contains("-")) {
                return new S3FixEtagJob(client, context, summary, notifyLock);
            }

            if (options.hasPrefixFile()) {
                // prefix is dirname of current file
                String prefix = summary.getKey().replaceFirst("/[^/]*$", "");
                context.getStats().decPrefixCount(prefix);
            }
            return null;
        }

        if (gs_storage != null) {
            return new GsKeyCopyJob(gs_storage, client, context, summary, notifyLock);
        }

        if (summary.getSize() > MirrorOptions.MAX_SINGLE_REQUEST_UPLOAD_FILE_SIZE) {
            return new MultipartS3KeyCopyJob(client, context, summary, notifyLock);
        }

        return new S3KeyCopyJob(client, context, summary, notifyLock);
    }
}
