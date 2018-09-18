package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Copy to Google Cloud Platform destination.
 * Handles a single key. Determines if it should be copied, and if so, performs the copy operation.
 */
@Slf4j
public class GsKeyCopyJob extends KeyJob {

    private final Storage gs_storage;
    protected String keydest;
    private final String key;
    private final String prefix;
    private final MirrorOptions options;
    private final String destBucket;
    private ObjectMetadata srcMetadata;

    public GsKeyCopyJob(Storage storage, AmazonS3Client s3Client, MirrorContext context, S3ObjectSummary summary, Object notifyLock) {
        super(s3Client, context, summary, notifyLock);

        gs_storage = storage;
        key = summary.getKey();
        options = context.getOptions();
        destBucket = options.getDestinationBucket().substring("gs://".length());


        if (options.hasPrefixFile()) {
            // prefix is dirname of current file
            prefix = key.replaceFirst("/[^/]*$", "");
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
            if (summary.getETag().contains("-") || options.checkMeta()) {
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
                if (srcMetadata == null) {
                    srcMetadata = getObjectMetadata(options.getSourceBucket(), key, options);
                }
                if (keyCopied(srcMetadata)) {
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

    boolean keyCopied(ObjectMetadata sourceMetadata) {
        boolean verbose = options.isVerbose();
        int maxRetries = options.getMaxRetries();
        MirrorStats stats = context.getStats();

        for (int tries = 1; tries <= maxRetries; tries++) {
            if (verbose) log.info("copying (try #" + tries + "): " + key + " to: " + keydest);

            try {
                stats.s3copyCount.incrementAndGet();

                S3Object s3obj = client.getObject(options.getSourceBucket(), key);
                ObjectMetadata s3Meta = s3obj.getObjectMetadata();

                String md5hex = summary.getETag();
                String md5base64 = null;

                if (md5hex.contains("-")) {
                    md5base64 = s3Meta.getUserMetaDataOf("md5");
                } else {
                    md5base64 = hexToBase64(md5hex);
                }

                BlobInfo blobInfo = BlobInfo.newBuilder(destBucket, keydest)
                        .setContentType(s3Meta.getContentType())
                        .setCacheControl(s3Meta.getCacheControl())
                        .setMetadata(s3Meta.getUserMetadata()).build();

                // Creates a new blob with no content.
                Blob blob = gs_storage.create(blobInfo);
                Storage.BlobWriteOption[] options = {};

                if (md5base64 != null) {
                    // enable server-side checksum verification
                    // Blob does NOT inherit Md5 from BlobInfo, so set it here
                    blob = blob.toBuilder().setMd5(md5base64).build();
                    options = new Storage.BlobWriteOption[]{Storage.BlobWriteOption.md5Match()};
                }

                // gs_storage.writer() starts a resumable upload
                // recommended for large (>5Mb) files or unreliable connections
                // also when metadata is set, as here.
                try (S3ObjectInputStream s3stream = s3obj.getObjectContent();
                     ReadableByteChannel reader = Channels.newChannel(s3stream);
                     WriteChannel writer = gs_storage.writer(blob, options)) {
                    copy(reader, writer);
                }

                if (md5base64 == null) {
                    // client-side checksum verification
                    // this will only check size, but also log warning about broken etag.
                    if (shouldTransfer()) {
                        throw new Exception("Checksum doesn't match: " + keydest);
                    }
                }

                stats.bytesCopied.addAndGet(sourceMetadata.getContentLength());

                if (tries > 1 || verbose) {
                    log.info("successfully copied (on try #" + tries + "): " + key + " to: " + keydest);
                }

                return true;
            } catch (AmazonS3Exception s3e) {
                if (s3e.getStatusCode() == 403) {
                    log.error("Access denied (no retry) copying " + key + ": " + s3e);
                    break;
                }
                log.error("s3 exception copying (try #" + tries + ") " + key + " to: " + keydest + ": " + s3e);
            } catch (StorageException se) {
                if (se.getCode() == 400) {
                    log.error("Bad Request (no retry) " + key + ": " + se);
                    break;
                }
            } catch (Exception e) {
                if (tries == maxRetries) {
                    log.error("FATAL copying (try #" + tries + ") " + key + " to: " + keydest + ": " + e);
                } else {
                    log.warn("copying (try #" + tries + ") " + key + " to: " + keydest + ": " + e);
                }
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
        boolean verbose = options.isVerbose();

        if (options.hasCtime()) {
            final Date lastModified = summary.getLastModified();
            if (lastModified == null) {
                if (verbose)
                    log.info("No Last-Modified header for key: " + key);
            } else {
                if (lastModified.getTime() < options.getMaxAge()) {
                    if (verbose)
                        log.info("key " + key + " (lastmod=" + lastModified + ") is older than " + options.getCtime() + " (cutoff=" + options.getMaxAgeDate() + "), not copying");
                    return false;
                }
            }
        }

        final Blob blob;
        try {
            blob = getBlob(destBucket, keydest, options);
            if (blob == null || !blob.exists()) {
                if (verbose) log.info("Key not found in destination bucket (will copy): " + keydest);
                return true;
            }
        } catch (Exception e) {
            log.warn("Error getting blob for " + options.getDestinationBucket() + "/" + keydest + " (not copying): " + e);
            return false;
        }

        final boolean objectChanged = objectChanged(blob);
        if (verbose && !objectChanged) log.info("Destination file is same as source, not copying: " + key);

        return objectChanged;
    }

    private Blob getBlob(String bucket, String key, MirrorOptions options) throws Exception {
        Exception ex = null;
        for (int tries = 0; tries < options.getMaxRetries(); tries++) {
            try {
                context.getStats().s3getCount.incrementAndGet();
                return gs_storage.get(bucket, key);
            } catch (Exception e) {
                ex = e;
                if (options.isVerbose()) {
                    if (tries >= options.getMaxRetries()) {
                        getLog().error("getBlob(" + key + ") failed (try #" + tries + "), giving up");
                        break;
                    } else {
                        getLog().warn("getBlob(" + key + ") failed (try #" + tries + "), retrying...");
                    }
                }
            }
        }
        throw ex;
    }

    boolean objectChanged(Blob blob) {
        boolean verbose = options.isVerbose();
        final KeyFingerprint sourceFingerprint;
        final KeyFingerprint destFingerprint;

        if (options.isSizeOnly()) {
            sourceFingerprint = new KeyFingerprint(summary.getSize());
            destFingerprint = new KeyFingerprint(blob.getSize());
        } else {
            String srcMd5 = summary.getETag();
            String dsnMd5 = base64ToHex(blob.getMd5());

            if (srcMd5.contains("-")) {
                String md5 = srcMetadata.getUserMetaDataOf("md5");
                if (md5 != null) {
                    srcMd5 = base64ToHex(md5);
                } else {
                    log.warn("Non-MD5 Etag (only comparing size): " + key);
                    srcMd5 = dsnMd5;
                }
            }

            if (options.checkMeta()) {
                Map<String, String> userMetadata = new HashMap<>(srcMetadata.getUserMetadata());
                Map<String, String> dsnMetadata = new HashMap<>();

                if (blob.getMetadata() != null)
                    dsnMetadata.putAll(blob.getMetadata());

                userMetadata.put("Content-Type", srcMetadata.getContentType());
                userMetadata.put("Cache-Control", srcMetadata.getCacheControl());

                dsnMetadata.put("Content-Type", blob.getContentType());
                dsnMetadata.put("Cache-Control", blob.getCacheControl());

                boolean match = true;

                for (Map.Entry<String, String> e : userMetadata.entrySet()) {
                    String s = dsnMetadata.get(e.getKey());
                    if (!e.getValue().equals(s)) {
                        if (verbose)
                            log.info(String.format("Metadata different (%s != %s) for key: %s", e.getKey(), e.getValue(), key));
                        match = false;
                        break;
                    }
                }

                if (!match) {
                    dsnMd5 = "metadata-mismatch";
                }
            }

            sourceFingerprint = new KeyFingerprint(summary.getSize(), srcMd5);
            destFingerprint = new KeyFingerprint(blob.getSize(), dsnMd5);
        }

        return !sourceFingerprint.equals(destFingerprint);
    }


    /**
     * copy channels
     *
     * @param src
     * @param dest
     * @return MD5 checksum
     * @throws IOException
     * @throws NoSuchAlgorithmException
     */
    private static String copy(final ReadableByteChannel src, final WritableByteChannel dest)
            throws IOException, NoSuchAlgorithmException {
        final MessageDigest digest = MessageDigest.getInstance("MD5");
        final ByteBuffer buffer = ByteBuffer.allocateDirect(256 * 1024);

        while (src.read(buffer) != -1) {
            // prepare the buffer to be drained
            buffer.flip();

            // write to the channel, may block
            int n = dest.write(buffer);
            int r = buffer.remaining();

            buffer.rewind();

            if (r == 0) {
                digest.update(buffer);
            } else {
                byte[] x = new byte[n];
                buffer.get(x, 0, n);
                digest.update(x);
            }

            // If partial transfer, shift remainder down
            // If buffer is empty, same as doing clear()
            buffer.compact();
        }

        // EOF will leave buffer in fill state
        buffer.flip();

        // make sure the buffer is fully drained.
        while (buffer.hasRemaining()) {
            int n = dest.write(buffer);
            buffer.rewind();
            byte[] x = new byte[n];
            buffer.get(x, 0, n);
            digest.update(x);
        }

        return bytesToHex(digest.digest());
    }

    private static String base64ToHex(String base64) {
        byte[] bytes = Base64.getDecoder().decode(base64);
        return bytesToHex(bytes);
    }

    private static String hexToBase64(String hex) {
        byte[] bytes = DatatypeConverter.parseHexBinary(hex);
        byte[] encoded = Base64.getEncoder().encode(bytes);
        return new String(encoded, StandardCharsets.ISO_8859_1);
    }

    private static String bytesToHex(byte[] bytes) {
        return DatatypeConverter.printHexBinary(bytes).toLowerCase();
    }
}
