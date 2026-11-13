package dev.minilog.broker;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Wakes up long-polling fetch requests. Any append (log end moved) or high-watermark change calls
 * {@link #signal()}; waiting fetches re-check whether they now have data to return.
 * A single broker-wide condition is simple and good enough for a handful of partitions.
 */
final class DataNotifier {
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private long version;

    long version() {
        lock.lock();
        try {
            return version;
        } finally {
            lock.unlock();
        }
    }

    void signal() {
        lock.lock();
        try {
            version++;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Waits until the version moves past {@code seenVersion} or the timeout elapses. */
    void awaitChange(long seenVersion, long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        lock.lock();
        try {
            while (version == seenVersion) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return;
                }
                changed.awaitNanos(remaining);
            }
        } finally {
            lock.unlock();
        }
    }
}
