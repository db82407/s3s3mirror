package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import javax.xml.bind.DatatypeConverter;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles a single key. Determines if it should be copied, and if so, performs the copy operation.
 */
@Slf4j
public class S3KeyCopyJob extends KeyJob {

    protected String keydest;
    private final String key;
    private final String prefix;
    private final MirrorOptions options;
    private ObjectMetadata srcMetadata;

    public S3KeyCopyJob(AmazonS3Client client, MirrorContext context, S3ObjectSummary summary, Object notifyLock) {
        super(client, context, summary, notifyLock);

        key = summary.getKey();
        options = context.getOptions();

        if (options.hasPrefixFile()) {
            // prefix is dirname of current file
            prefix = keydest.replaceFirst("/[^/]*$", "");
        } else {
            prefix = null;
        }

        keydest = key;

        if (options.hasDestPrefix()) {
            String destPrefix = options.getDestPrefix();
            if (!destPrefix.endsWith("/")) {
                destPrefix += "/";
            }
            keydest = keydest.substring(options.getPrefixLength());
            keydest = destPrefix + keydest;
        }
    }

    @Override
    public Logger getLog() {
        return log;
    }

    @Override
    public void run() {
        try {
            if (summary.getETag().contains("-")) {
                srcMetadata = getObjectMetadata(options.getSourceBucket(), key, options);
            }

            if (!shouldTransfer()) {
                context.getStats().decPrefixCount(prefix);
                return;
            }

            if (options.isDryRun()) {
                log.info("Would have copied " + key + " to destination: " + keydest);
                context.getStats().s3copyCount.incrementAndGet();
            } else {
                final AccessControlList objectAcl = getAccessControlList(options, key);
                if (srcMetadata == null) {
                    srcMetadata = getObjectMetadata(options.getSourceBucket(), key, options);
                }

                if (keyCopied(srcMetadata, objectAcl)) {
                    context.getStats().objectsCopied.incrementAndGet();
                    context.getStats().decPrefixCount(prefix);
                } else {
                    context.getStats().copyErrors.incrementAndGet();
                }
            }
        } catch (Exception e) {
            log.error("error copying key: " + key + ": " + e);
            context.getStats().copyErrors.incrementAndGet();
        } finally {
            synchronized (notifyLock) {
                notifyLock.notifyAll();
            }
            if (options.isVerbose()) log.info("done with " + key);
        }
    }

    protected boolean keyCopied(ObjectMetadata sourceMetadata, AccessControlList objectAcl) {
        boolean verbose = options.isVerbose();
        int maxRetries = options.getMaxRetries();
        MirrorStats stats = context.getStats();
        for (int tries = 0; tries < maxRetries; tries++) {
            if (verbose) log.info("copying (try #" + tries + "): " + key + " to: " + keydest);
            final CopyObjectRequest request = new CopyObjectRequest(options.getSourceBucket(), key, options.getDestinationBucket(), keydest);

            String storageName = options.getStorageClass();
            if (storageName != null) {
                StorageClass storageClass = StorageClass.valueOf(storageName);
                request.setStorageClass(storageClass);
            }

            if (options.isEncrypt()) {
                request.putCustomRequestHeader("x-amz-server-side-encryption", "AES256");
            }

            request.setNewObjectMetadata(sourceMetadata);
            if (options.isCrossAccountCopy()) {
                request.setCannedAccessControlList(CannedAccessControlList.BucketOwnerFullControl);
            } else {
                request.setAccessControlList(objectAcl);
            }
            try {
                stats.s3copyCount.incrementAndGet();
                client.copyObject(request);
                stats.bytesCopied.addAndGet(sourceMetadata.getContentLength());
                if (verbose) log.info("successfully copied (on try #" + tries + "): " + key + " to: " + keydest);
                return true;
            } catch (AmazonS3Exception s3e) {
                if (s3e.getStatusCode() == 403) {
                    log.error("Access denied copying " + key + ": " + s3e);
                    break;
                }
                log.error("s3 exception copying (try #" + tries + ") " + key + " to: " + keydest + ": " + s3e);
            } catch (Exception e) {
                log.error("unexpected exception copying (try #" + tries + ") " + key + " to: " + keydest + ": " + e);
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

    private boolean shouldTransfer() {
        final boolean verbose = options.isVerbose();

        if (options.hasCtime()) {
            final Date lastModified = summary.getLastModified();
            if (lastModified == null) {
                if (verbose) log.info("No Last-Modified header for key: " + key);

            } else {
                if (lastModified.getTime() < options.getMaxAge()) {
                    if (verbose)
                        log.info("key " + key + " (lastmod=" + lastModified + ") is older than " + options.getCtime() + " (cutoff=" + options.getMaxAgeDate() + "), not copying");
                    return false;
                }
            }
        }

        final ObjectMetadata dsnMetadata;
        try {
            dsnMetadata = getObjectMetadata(options.getDestinationBucket(), keydest, options);
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                if (verbose) log.info("Key not found in destination bucket (will copy): " + keydest);
                return true;
            } else {
                log.warn("Error getting metadata for " + options.getDestinationBucket() + "/" + keydest + " (not copying): " + e);
                return false;
            }
        } catch (Exception e) {
            log.warn("Error getting metadata for " + options.getDestinationBucket() + "/" + keydest + " (not copying): " + e);
            return false;
        }

        final boolean objectChanged = objectChanged(dsnMetadata);
        if (verbose && !objectChanged) log.info("Destination file is same as source, not copying: " + key);

        return objectChanged;
    }


    protected boolean objectChanged(ObjectMetadata dsnMetadata) {
        final boolean verbose = options.isVerbose();
        final KeyFingerprint sourceFingerprint;
        final KeyFingerprint destFingerprint;

        if (options.isSizeOnly()) {
            sourceFingerprint = new KeyFingerprint(summary.getSize());
            destFingerprint = new KeyFingerprint(dsnMetadata.getContentLength());
        } else {
            String srcEtag = summary.getETag();
            String dsnEtag = dsnMetadata.getETag();

            if (srcEtag.contains("-") && !srcEtag.equals(dsnEtag)) {
                String md5 = srcMetadata.getUserMetaDataOf("md5");
                if (md5 != null) {
                    srcEtag = base64ToHex(md5);

                    md5 = dsnMetadata.getUserMetaDataOf("md5");

                    if (md5 != null)
                        dsnEtag = base64ToHex(md5);
                } else {
                    log.warn("Non-MD5 Etag (only comparing size): " + key);
                    srcEtag = dsnEtag;
                }
            }

            if (options.checkMeta()) {
                Map<String, String> dsnUserMetadata = new HashMap<>(dsnMetadata.getUserMetadata());
                Map<String, String> srcUserMetadata = new HashMap<>(srcMetadata.getUserMetadata());

                srcUserMetadata.put("Content-Type", srcMetadata.getContentType());
                srcUserMetadata.put("Cache-Control", srcMetadata.getCacheControl());
                dsnUserMetadata.put("Content-Type", dsnMetadata.getContentType());
                dsnUserMetadata.put("Cache-Control", dsnMetadata.getCacheControl());

                boolean match = true;

                for (Map.Entry<String, String> e : srcUserMetadata.entrySet()) {
                    String s = dsnUserMetadata.get(e.getKey());
                    if (!e.getValue().equals(s)) {
                        if (verbose)
                            log.info(String.format("Metadata different (%s != %s) for key: %s", e.getKey(), e.getValue(), key));
                        match = false;
                        break;
                    }
                }

                if (!match) {
                    dsnEtag = "metadata-mismatch";
                }
            }

            sourceFingerprint = new KeyFingerprint(summary.getSize(), srcEtag);
            destFingerprint = new KeyFingerprint(dsnMetadata.getContentLength(), dsnEtag);
        }

        return !sourceFingerprint.equals(destFingerprint);
    }


    private static String base64ToHex(String base64) {
        byte[] bytes = Base64.getDecoder().decode(base64);
        return bytesToHex(bytes);
    }

    private static String bytesToHex(byte[] bytes) {
        return DatatypeConverter.printHexBinary(bytes).toLowerCase();
    }
}
