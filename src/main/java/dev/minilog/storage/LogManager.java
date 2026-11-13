package dev.minilog.storage;

import dev.minilog.common.Logger;
import dev.minilog.common.Time;
import dev.minilog.common.TopicPartition;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

/**
 * Owns every partition {@link Log} in a broker's data directory and runs the background
 * flush and retention tasks.
 *
 * <p>Clean-shutdown detection works like Kafka's: a {@code .clean_shutdown} marker file is written
 * as the very last step of a clean close and deleted on startup. If it is missing at startup, the
 * previous process crashed and every log's active segment is recovered.
 */
public final class LogManager implements Closeable {
    private static final Logger LOG = Logger.get("log-manager");
    private static final String CLEAN_SHUTDOWN_FILE = ".clean_shutdown";

    private final Path baseDir;
    private final LogConfig config;
    private final boolean hadCleanShutdown;
    private final Map<TopicPartition, Log> logs = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;

    public LogManager(Path baseDir, LogConfig config) throws IOException {
        this.baseDir = baseDir;
        this.config = config;
        Files.createDirectories(baseDir);
        Path marker = baseDir.resolve(CLEAN_SHUTDOWN_FILE);
        boolean anyData;
        try (Stream<Path> s = Files.list(baseDir)) {
            anyData = s.anyMatch(Files::isDirectory);
        }
        this.hadCleanShutdown = Files.exists(marker) || !anyData;
        Files.deleteIfExists(marker);
        loadExistingLogs();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "log-manager-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    private void loadExistingLogs() throws IOException {
        try (Stream<Path> dirs = Files.list(baseDir)) {
            for (Path dir : dirs.filter(Files::isDirectory).toList()) {
                TopicPartition tp = TopicPartition.fromDirName(dir.getFileName().toString());
                logs.put(tp, Log.open(dir, config, hadCleanShutdown));
            }
        }
        LOG.info("Loaded %d logs from %s (clean shutdown: %s)", logs.size(), baseDir, hadCleanShutdown);
    }

    /**
     * Starts background tasks.
     *
     * @param maxDeletableOffset for each partition, the offset below which retention may delete
     *                           (the broker passes the high watermark)
     */
    public void startBackgroundTasks(ToLongFunction<TopicPartition> maxDeletableOffset) {
        if (config.flushIntervalMs() > 0) {
            long period = Math.max(1, config.flushIntervalMs() / 2);
            scheduler.scheduleAtFixedRate(() -> {
                long now = Time.nowMs();
                logs.values().forEach(l -> safely(() -> l.maybeFlushByTime(now)));
            }, period, period, TimeUnit.MILLISECONDS);
        }
        scheduler.scheduleAtFixedRate(() -> runRetention(maxDeletableOffset),
                config.retentionCheckIntervalMs(), config.retentionCheckIntervalMs(), TimeUnit.MILLISECONDS);
    }

    public int runRetention(ToLongFunction<TopicPartition> maxDeletableOffset) {
        int deleted = 0;
        long now = Time.nowMs();
        for (Map.Entry<TopicPartition, Log> e : logs.entrySet()) {
            try {
                deleted += e.getValue().deleteOldSegments(now, maxDeletableOffset.applyAsLong(e.getKey()));
            } catch (RuntimeException ex) {
                LOG.warn("Retention failed for %s: %s", e.getKey(), ex);
            }
        }
        return deleted;
    }

    private static void safely(Runnable r) {
        try {
            r.run();
        } catch (RuntimeException e) {
            LOG.warn("Background task failed: %s", e);
        }
    }

    public Log getOrCreate(TopicPartition tp) {
        return logs.computeIfAbsent(tp, k -> {
            try {
                return Log.open(baseDir.resolve(k.dirName()), config, true);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    public Optional<Log> get(TopicPartition tp) {
        return Optional.ofNullable(logs.get(tp));
    }

    public Map<TopicPartition, Log> all() {
        return Map.copyOf(logs);
    }

    public boolean hadCleanShutdown() {
        return hadCleanShutdown;
    }

    /** Clean shutdown: flush and close every log, then write the marker file. */
    @Override
    public void close() throws IOException {
        scheduler.shutdownNow();
        for (Log log : logs.values()) {
            log.close();
        }
        Files.writeString(baseDir.resolve(CLEAN_SHUTDOWN_FILE), "");
    }

    /** Crash simulation for tests: no flush, no marker. */
    public void closeWithoutFlush() {
        scheduler.shutdownNow();
        for (Log log : logs.values()) {
            try {
                log.closeWithoutFlush();
            } catch (IOException ignored) {
                // best effort
            }
        }
    }
}
