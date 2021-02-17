package it.uniroma2.faas.openwhisk.scheduler.util;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toCollection;

public class LineReader {

    private final String filename;

    private final Queue<String> queue;

    public LineReader(String filename) throws IOException {
        this.filename = filename;
        try (Stream<String> stream = Files.lines(Paths.get(filename))) {
            this.queue = stream.collect(toCollection(ArrayDeque::new));
        }
    }

    public @Nullable String poll() {
        return queue.poll();
    }

    public String getFilename() {
        return filename;
    }

    public Queue<String> getQueue() {
        return new ArrayDeque<>(queue);
    }

}