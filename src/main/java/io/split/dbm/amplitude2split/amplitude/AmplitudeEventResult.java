package io.split.dbm.amplitude2split.amplitude;

import io.split.dbm.amplitude2split.Configuration;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;

public
class AmplitudeEventResult implements Iterator<AmplitudeEvent> {
    private final Configuration config;
    private final ZipArchiveInputStream zis;
    private Iterator<AmplitudeEvent> fileIterator;

    public AmplitudeEventResult(Configuration config, InputStream inputStream) throws IOException {
        this.config = config;

        // Handle the wrapper ZIP
        BufferedInputStream bis = new BufferedInputStream(inputStream);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copy(bis, baos);

        this.zis = new ZipArchiveInputStream(
                new BufferedInputStream(new ByteArrayInputStream(baos.toByteArray())),
                "UTF-8", false, true);
    }

    public Stream<AmplitudeEvent> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(this, Spliterator.NONNULL), false);
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
        if(fileIterator == null || !fileIterator.hasNext()) {
            Optional<Iterator<AmplitudeEvent>> nextFileIterator = nextFileIterator();
            if(nextFileIterator.isEmpty()) {
                return Collections.emptyIterator();
            }
            fileIterator = nextFileIterator.get();
        }
        return fileIterator;
    }

    private Optional<Iterator<AmplitudeEvent>> nextFileIterator() {
        try {
            ZipArchiveEntry entry = zis.getNextZipEntry();
            if(entry == null) {
                return Optional.empty();
            }

            // Handle the Gzipd JSON
            System.out.println("INFO - " + entry.getName());

            // Load GZipped file from Archive
            ByteArrayOutputStream zipBaos = toOutputStream(zis);
            IOUtils.copy(zis, zipBaos);

            GZIPInputStream gzis = new GZIPInputStream(new ByteArrayInputStream(zipBaos.toByteArray()));
            ByteArrayOutputStream jsonBaos = new ByteArrayOutputStream();
            IOUtils.copy(gzis, jsonBaos);

            // Read Events From File
            BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(jsonBaos.toByteArray())));
            return Optional.of(reader.lines()
                    .map(line -> new AmplitudeEvent(line, config))
                    .iterator());
        } catch (IOException exception) {
            System.err.println("ERROR - Cannot read from response");
            return Optional.empty();
        }
    }

    private ByteArrayOutputStream toOutputStream(InputStream inputStream) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        IOUtils.copy(inputStream, outputStream);
        return outputStream;
    }
}
