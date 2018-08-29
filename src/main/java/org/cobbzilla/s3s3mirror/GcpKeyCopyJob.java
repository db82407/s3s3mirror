package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

/**
 * Copy to Google Cloud Platform destination.
 * Handles a single key. Determines if it should be copied, and if so, performs the copy operation.
 */
@Slf4j
public class GcpKeyCopyJob extends KeyCopyJob {

    protected String keydest;
    private String prefix;
    private final Storage gs_storage;
    private final String destBucket;

    public GcpKeyCopyJob(Storage storage, AmazonS3Client client, MirrorContext context, S3ObjectSummary summary, Object notifyLock) {
        super(client, context, summary, notifyLock);
        gs_storage = storage;

        keydest = summary.getKey();
        final MirrorOptions options = context.getOptions();

        destBucket = options.getDestinationBucket().substring("gs://".length());

		if (options.getPrefix().startsWith("file:")) {
			prefix = keydest.replaceFirst("/[^/]*$", ""); // remove last path element
		}
		else {
			keydest = keydest.substring(options.getPrefixLength());
		}

        if (options.hasDestPrefix()) {
            String destPrefix = options.getDestPrefix();
            if (!destPrefix.endsWith("/")) {
            	destPrefix += "/";
            }
            keydest = destPrefix + keydest;
        }
    }

    @Override
    public void run() {
        final MirrorOptions options = context.getOptions();
        final String key = summary.getKey();
        try {
            final ObjectMetadata sourceMetadata = getObjectMetadata(options.getSourceBucket(), key, options);
            if (!shouldTransfer(sourceMetadata, null, options.isVerbose())) {
				if (prefix != null) {
					context.getStats().prefix2count.get(prefix).decrementAndGet();
				}
            	return;
            }
            final AccessControlList objectAcl = getAccessControlList(options, key);

            if (options.isDryRun()) {
                log.info("Would have copied " + key + " to destination: " + keydest);
            } else {
                if (keyCopied(sourceMetadata, objectAcl)) {
                    context.getStats().objectsCopied.incrementAndGet();
                    if (prefix != null) {
						context.getStats().prefix2count.get(prefix).decrementAndGet();
                    }
                } else {
                    context.getStats().copyErrors.incrementAndGet();
                }
            }
        } catch (Exception e) {
            log.error("error copying key: " + key + ": " + e);
        } finally {
            synchronized (notifyLock) {
                notifyLock.notifyAll();
            }
            if (options.isVerbose()) log.info("done with " + key);
        }
    }

    boolean keyCopied(ObjectMetadata sourceMetadata, AccessControlList objectAcl) {
        String key = summary.getKey();
        MirrorOptions options = context.getOptions();
        boolean verbose = options.isVerbose();
        int maxRetries= options.getMaxRetries();
        MirrorStats stats = context.getStats();

        for (int tries = 1; tries <= maxRetries; tries++) {
            if (verbose) log.info("copying (try #" + tries + "): " + key + " to: " + keydest);

            try {
                stats.s3copyCount.incrementAndGet();

                S3Object s3obj = client.getObject(options.getSourceBucket(), key);
                Map<String, String> metadata = s3obj.getObjectMetadata().getUserMetadata();

                BlobInfo blobInfo = BlobInfo.newBuilder(destBucket, keydest)
                        .setMetadata(metadata)
                        .build();

                Blob blob = gs_storage.create(blobInfo);
                WriteChannel writer = gs_storage.writer(blob);
                String md5 = null;

                try (S3ObjectInputStream stream = s3obj.getObjectContent()) {
                    ReadableByteChannel reader = Channels.newChannel(stream);
                    md5 = copy(reader, writer);
                    reader.close();
                }
                finally {
                    writer.close();
                }

                if (shouldTransfer(sourceMetadata, md5,  verbose)) {
                    throw new Exception("checksum doesn't match: " + keydest);
                }

                stats.bytesCopied.addAndGet(sourceMetadata.getContentLength());

                if (tries > 1 || verbose) {
                    log.info("successfully copied (on try #" + tries + "): " + key + " to: " + keydest);
                }

                return true;

            } catch (Exception e) {
                if (tries == maxRetries) {
                    log.error("FATAL copying (try #" + tries + ") " + key + " to: " + keydest + ": " + e);
                }
                else {
                    log.warn("copying (try #" + tries + ") " + key + " to: " + keydest + ": " + e);
                }
//                e.printStackTrace();
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                log.error("interrupted while waiting to retry key: " + key);
                return false;
            }
        }
        return false;
    }

    private boolean shouldTransfer(ObjectMetadata sourceMetadata, String clientMd5, boolean verbose) {
        final MirrorOptions options = context.getOptions();
        final String key = summary.getKey();

        if (options.hasCtime()) {
            final Date lastModified = summary.getLastModified();
            if (lastModified == null) {
                if (verbose) log.info("No Last-Modified header for key: " + key);

            } else {
                if (lastModified.getTime() < options.getMaxAge()) {
                    if (verbose) log.info("key "+key+" (lastmod="+lastModified+") is older than "+options.getCtime()+" (cutoff="+options.getMaxAgeDate()+"), not copying");
                    return false;
                }
            }
        }

        final Blob blob;
        try {
            blob = getBlob(destBucket, keydest, options);
            if (blob == null || !blob.exists()) {
                if (verbose) log.info("Key not found in destination bucket (will copy): "+ keydest);
                return true;
            }
        } catch (Exception e) {
            log.warn("Error getting blob for " + options.getDestinationBucket() + "/" + keydest + " (not copying): " + e);
            return false;
        }

        final boolean objectChanged = objectChanged(sourceMetadata, blob, clientMd5, verbose);
        if (verbose && !objectChanged) log.info("Destination file is same as source, not copying: "+ key);

        return objectChanged;
    }

    private Blob getBlob(String bucket, String key, MirrorOptions options) throws Exception {
        Exception ex = null;
        for (int tries=0; tries<options.getMaxRetries(); tries++) {
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
                        getLog().warn("getBlob("+key+") failed (try #"+tries+"), retrying...");
                    }
                }
            }
        }
        throw ex;
    }

    boolean objectChanged(ObjectMetadata sourceMetadata, Blob blob, String clientMd5, boolean verbose) {
        final MirrorOptions options = context.getOptions();
        final Properties props = context.getProperties();
        final KeyFingerprint sourceFingerprint;
        final KeyFingerprint destFingerprint;

        if (options.isSizeOnly()) {
            sourceFingerprint = new KeyFingerprint(summary.getSize());
            destFingerprint = new KeyFingerprint(blob.getSize());
        } else {
            byte[] bytes = Base64.getDecoder().decode(blob.getMd5());
            String gs_md5 = bytesToHex(bytes);
            String s3_md5 = sourceMetadata.getContentMD5();
            if (s3_md5 == null) {
               s3_md5 = summary.getETag();
            }

            if (!gs_md5.equals(s3_md5)) {
                if (clientMd5 == null) {
                    String md5 = props.getProperty(summary.getKey());
                    if (md5 != null) {
                        s3_md5 = md5;
                    }
                }
                else if (gs_md5.equals(clientMd5)) {
                    // s3 checksum is not md5, so record correct checksum to aid re-run
                    s3_md5 = gs_md5;
                    props.put(summary.getKey(), gs_md5);
                }
            }

            sourceFingerprint = new KeyFingerprint(summary.getSize(), s3_md5);
            destFingerprint = new KeyFingerprint(blob.getSize(), gs_md5);
        }

        return !sourceFingerprint.equals(destFingerprint);
    }

    private static String bytesToHex(byte[] in) {
        final StringBuilder builder = new StringBuilder();
        for(byte b : in) {
            builder.append(String.format("%02x", b));
        }
        return builder.toString();
    }

    private static String copy(final ReadableByteChannel src, final WritableByteChannel dest) throws IOException, NoSuchAlgorithmException {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(256 * 1024);
        MessageDigest m = MessageDigest.getInstance("MD5");

        while (src.read(buffer) != -1) {
            // prepare the buffer to be drained
            buffer.flip();

            // write to the channel, may block
            int n = dest.write(buffer);
            int r = buffer.remaining();

            buffer.rewind();

            if (r == 0) {
                m.update(buffer);
            }
            else {
                byte[] x = new byte[n];
                buffer.get(x, 0, n);
                m.update(x);
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
            m.update(x);
        }

        BigInteger md5 = new BigInteger(1, m.digest());
        return String.format("%032x", md5);
    }

}
