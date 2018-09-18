package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

/**
 * Fix Etag.
 */
@Slf4j
public class S3FixEtagJob extends KeyJob {

    private final String key;
    private final String prefix;
    private final MirrorOptions options;
    private final MirrorStats stats;
    private final boolean verbose;

    public S3FixEtagJob(AmazonS3Client s3Client, MirrorContext context, S3ObjectSummary summary, Object notifyLock) {
        super(s3Client, context, summary, notifyLock);

        key = summary.getKey();
        options = context.getOptions();
        stats = context.getStats();
        verbose = options.isVerbose();

        if (options.hasPrefixFile()) {
            // prefix is dirname of current file
            prefix = key.replaceFirst("/[^/]*$", "");
        } else {
            prefix = null;
        }
    }

    @Override
    public Logger getLog() {
        return log;
    }

    @Override
    public void run() {
        try {
            if (!summary.getETag().contains("-")) {
                stats.decPrefixCount(prefix);
                return;
            }

            final String sourceBucket = options.getSourceBucket();
            final ObjectMetadata sourceMetadata = getObjectMetadata(sourceBucket, key, options);
            final String md5 = sourceMetadata.getUserMetaDataOf("md5");

            if (md5 != null) {
                stats.decPrefixCount(prefix);
                return;
            }

            if (options.isDryRun()) {
                log.info("Would have fixed Etag for " + key);
                stats.s3copyCount.incrementAndGet();
                return;
            }

            if (keyUpdated(sourceMetadata)) {
                stats.objectsCopied.incrementAndGet();
                stats.decPrefixCount(prefix);
            } else {
                stats.copyErrors.incrementAndGet();
            }
        } catch (Exception e) {
            log.error("error updating key: " + key + ": " + e);
        } finally {
            synchronized (notifyLock) {
                notifyLock.notifyAll();
            }
            if (verbose) log.info("done with " + key);
        }
    }

    private boolean keyUpdated(ObjectMetadata sourceMetadata) {
        int maxRetries = options.getMaxRetries();

        for (int tries = 1; tries <= maxRetries; tries++) {
            if (verbose) log.info("update (try #" + tries + "): " + key);

            // To update existing metadata, you have to copy the object to itself.
            // If bucket versioning is enabled, this creates a new version of the object otherwise it will replace it.
            // https://stackoverflow.com/questions/32646646/how-do-i-update-metadata-for-an-existing-amazon-s3-file
            final CopyObjectRequest request = new CopyObjectRequest(options.getSourceBucket(), key, options.getSourceBucket(), key);

            // We don't have to explicitly set the fixed md5 checksum,
            //  as this is NOT a multipart copy, the new Etag will be the MD5 checksum.
            //      sourceMetadata.setContentMD5(md5Base64);
            //      sourceMetadata.addUserMetadata("md5", md5Base64);

            // However, we do need to set some metadata to avoid this error:
            //  AmazonS3Exception: This copy request is illegal because it is trying to copy an object to itself
            //      without changing the object's metadata
            request.setNewObjectMetadata(sourceMetadata);

            try {
                stats.s3copyCount.incrementAndGet();

                CopyObjectResult result = client.copyObject(request);

                stats.bytesCopied.addAndGet(sourceMetadata.getContentLength());
                if (verbose) log.info("successfully updated (on try #" + tries + "): " + key);
                return true;
            } catch (AmazonS3Exception s3e) {
                if (s3e.getStatusCode() == 403) {
                    log.error("Access denied updating " + key + ": " + s3e);
                    break;
                }
                log.error("s3 exception updating (try #" + tries + ") " + key + ": " + s3e);
            } catch (Exception e) {
                log.error("unexpected exception updating (try #" + tries + ") " + key + ": " + e);
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                log.error("interrupted while waiting to retry key: " + key);
                break;
            }
        }
        return false;
    }

}
