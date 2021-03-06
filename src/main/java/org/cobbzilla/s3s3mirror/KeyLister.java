package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class KeyLister implements Runnable {

    private AmazonS3Client client;
    private MirrorContext context;
    private int maxQueueCapacity;

    private final List<S3ObjectSummary> summaries;
    private final AtomicBoolean done = new AtomicBoolean(false);
    private ObjectListing listing;

    private final String bucket;
    private final MirrorOptions options;
    private final int fetchSize;
    private String prefix;

    private BufferedReader reader;
    private long count = 0;
    private final long total;

    public boolean isDone() {
        return done.get();
    }

    public KeyLister(AmazonS3Client client, MirrorContext context, int maxQueueCapacity, String bucket, String prefix) {
        this.bucket = bucket;
        this.client = client;
        this.context = context;
        this.maxQueueCapacity = maxQueueCapacity;

        this.options = this.context.getOptions();
        this.fetchSize = options.getMaxThreads();
        this.summaries = new ArrayList<S3ObjectSummary>(10 * fetchSize);

        if (options.hasPrefixFile()) {
            String prefixFile = options.getPrefixFile();
            try {
                total = Files.lines(Paths.get(prefixFile)).count();
                reader = new BufferedReader(new FileReader(prefixFile));

                while ((prefix = reader.readLine()) != null) {
                    if (newListing(prefix))
                        break;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            total = 0;
            newListing(prefix);
        }
    }

    private void abort(String msg) {
        System.err.println("\nABORT: " + msg);
        System.exit(2);
    }

    private boolean newListing(String prefix) {
        System.err.print(String.format("\r%d/%d %s/%s", ++count, total, bucket, prefix));

        final ListObjectsRequest request = new ListObjectsRequest(bucket, prefix, null, null, fetchSize);

        listing = s3getFirstBatch(client, request);

        synchronized (summaries) {
            final List<S3ObjectSummary> objectSummaries = listing.getObjectSummaries();
            summaries.addAll(objectSummaries);
            context.getStats().objectsRead.addAndGet(objectSummaries.size());
            if (prefix != null) {
                context.getStats().prefix2count.put(prefix, new AtomicInteger(objectSummaries.size()));
            }

            if (options.isVerbose()) log.info("\nAdded initial set of " + objectSummaries.size() + " keys");

            if (objectSummaries.isEmpty()) {
                log.warn("\nEmpty directory: " + prefix);
                // throw new IllegalArgumentException("ERROR: prefix empty: " + prefix);
                this.prefix = null;
                return false;
            }
        }
        this.prefix = prefix;
        return true;
    }

    @Override
    public void run() {
        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();
        int counter = 0;
        log.info("starting...");
        try {
            while (true) {
                while (getSize() < maxQueueCapacity) {
                    if (listing.isTruncated()) {
                        listing = s3getNextBatch();
                        if (++counter % 100 == 0) context.getStats().logStats();
                        synchronized (summaries) {
                            final List<S3ObjectSummary> objectSummaries = listing.getObjectSummaries();
                            summaries.addAll(objectSummaries);
                            context.getStats().objectsRead.addAndGet(objectSummaries.size());
                            if (prefix != null) {
                                context.getStats().prefix2count.get(prefix).addAndGet(objectSummaries.size());
                            }
                            if (verbose)
                                log.info("queued next set of " + objectSummaries.size() + " keys (total now=" + getSize() + ")");
                        }

                    } else {
                        while (reader != null) {
                            String prefix = reader.readLine();
                            if (prefix == null) {
                                reader.close();
                                reader = null;
                            } else {
                                if (newListing(prefix))
                                    break;
                            }
                        }
                        if (reader == null) {
                            log.info("No more keys found in source bucket, KeyLister thread exiting");
                            String progress = MirrorMain.progress(context);
                            if (!progress.isEmpty()) {
                                log.info("The following jobs are still running: \n" + progress);
                            }
                            return;
                        }
                    }
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    log.error("interrupted!");
                    return;
                }
            }
        } catch (Exception e) {
            log.error("Error in run loop, KeyLister thread now exiting: " + e);
            e.printStackTrace();
            abort(e.toString());
        } finally {
            if (verbose) log.info("KeyLister run loop finished");
            done.set(true);
        }
    }

    private ObjectListing s3getFirstBatch(AmazonS3Client client, ListObjectsRequest request) {

        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();
        final int maxRetries = options.getMaxRetries();

        Exception lastException = null;
        for (int tries = 0; tries < maxRetries; tries++) {
            try {
                context.getStats().s3getCount.incrementAndGet();
                ObjectListing listing = client.listObjects(request);
                if (verbose) log.info("successfully got first batch of objects (on try #" + tries + ")");
                return listing;

            } catch (Exception e) {
                lastException = e;
                log.warn("s3getFirstBatch: error listing (try #" + tries + "): " + e);
                if (Sleep.sleep(50)) {
                    log.info("s3getFirstBatch: interrupted while waiting for next try");
                    break;
                }
            }
        }
        throw new IllegalStateException("s3getFirstBatch: error listing: " + lastException, lastException);
    }

    private ObjectListing s3getNextBatch() {
        final MirrorOptions options = context.getOptions();
        final boolean verbose = options.isVerbose();
        final int maxRetries = options.getMaxRetries();

        for (int tries = 0; tries < maxRetries; tries++) {
            try {
                context.getStats().s3getCount.incrementAndGet();
                ObjectListing next = client.listNextBatchOfObjects(listing);
                if (verbose) log.info("successfully got next batch of objects (on try #" + tries + ")");
                return next;

            } catch (AmazonS3Exception s3e) {
                log.error("s3 exception listing objects (try #" + tries + "): " + s3e);

            } catch (Exception e) {
                log.error("unexpected exception listing objects (try #" + tries + "): " + e);
            }
            if (Sleep.sleep(50)) {
                log.info("s3getNextBatch: interrupted while waiting for next try");
                break;
            }
        }
        throw new IllegalStateException("Too many errors trying to list objects (maxRetries=" + maxRetries + ")");
    }

    private int getSize() {
        synchronized (summaries) {
            return summaries.size();
        }
    }

    public List<S3ObjectSummary> getNextBatch() {
        List<S3ObjectSummary> copy;
        synchronized (summaries) {
            copy = new ArrayList<S3ObjectSummary>(summaries);
            summaries.clear();
        }
        return copy;
    }
}
