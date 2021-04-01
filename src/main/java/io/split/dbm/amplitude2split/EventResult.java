package io.split.dbm.amplitude2split;

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

public class EventResult implements Iterator<Event> {
    private final Configuration config;
    private final ZipArchiveInputStream eventsArchive;
    private Iterator<Event> fileIterator;

    public EventResult(Configuration config, InputStream inputStream) {
        this.config = config;
        this.eventsArchive = new ZipArchiveInputStream(inputStream, "UTF-8", false, true);
    }

    public Stream<Event> stream() {
        Spliterator<Event> spliterator = Spliterators.spliteratorUnknownSize(this, Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, false);
    }

    @Override
    public boolean hasNext() {
    	boolean result = false;
        try {
        	result = getFileIterator().hasNext();
        } catch (Exception e) {
    		System.err.println("WARN - exception reading JSON: " + e.getMessage() + ".  Beginning retry...");
    		e.printStackTrace(System.err);
    		
        	int retries = 0;
        	do {
        		try {
        			result = getFileIterator().hasNext(); // try again
        		} catch (Exception ex) {
            		System.err.println("WARN - exception reading JSON [" + (retries+1) + "]: " + e.getMessage());
            		e.printStackTrace(System.err);
        			result = false;
        		}
        	} while(retries++ < 10 && !result);
        	if(!result) {
        		System.err.println("ERROR - failed repeatedly while parsing JSON. Closing parse...");
        	}
        }
        return result;
    }

    @Override
    public Event next() {
        return getFileIterator().next();
    }

    public Iterator<Event> getFileIterator() {
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
                        .flatMap(line -> Event.fromJson(line, config).stream())
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
