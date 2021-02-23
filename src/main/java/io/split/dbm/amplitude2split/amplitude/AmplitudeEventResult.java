package io.split.dbm.amplitude2split.amplitude;

import io.split.dbm.amplitude2split.Configuration;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;

public
class AmplitudeEventResult implements Iterator<AmplitudeEvent> {
    private final Configuration config;
    private final ZipArchiveInputStream eventsArchive;
    private Iterator<AmplitudeEvent> fileIterator;

    public AmplitudeEventResult(Configuration config, InputStream inputStream) {
        this.config = config;
        this.eventsArchive = new ZipArchiveInputStream(inputStream, "UTF-8", false, true);
    }

    public Stream<AmplitudeEvent> stream() {
        Spliterator<AmplitudeEvent> spliterator = Spliterators.spliteratorUnknownSize(this, Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, false);
    }

    @Override
    public boolean hasNext() {
        return getFileIterator().hasNext();
    }

    @Override
    public AmplitudeEvent next() {
        return getFileIterator().next();
    }

    public Iterator<AmplitudeEvent> getFileIterator() {
        // Is current iterator active
        if(fileIterator == null || !fileIterator.hasNext()) {
            try {
                // Load next file from archive
                ZipArchiveEntry entry = eventsArchive.getNextZipEntry();
                if(entry == null) {
                    return Collections.emptyIterator();
                }
                System.out.printf("INFO - Processing file: %s %n", entry.getName());

                // Read Events From File
                BufferedReader gzipFile = new BufferedReader(new InputStreamReader(new GZIPInputStream(eventsArchive)));

                // Set File Iterator
                fileIterator = gzipFile.lines()
                        .map(line -> new AmplitudeEvent(line, config))
                        .iterator();
            } catch (IOException exception) {
                System.err.println("ERROR - Error processing events archive");
                exception.printStackTrace(System.err);
                return Collections.emptyIterator();
            }
        }
        return fileIterator;
    }
}
