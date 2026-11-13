package dev.minilog.common;

import java.util.concurrent.TimeUnit;

/** Small helpers around time so the rest of the code reads clearly. */
public final class Time {
    private Time() {
    }

    public static long nowMs() {
        return System.currentTimeMillis();
    }

    /** Sleeps without forcing callers to handle InterruptedException; restores the interrupt flag. */
    public static void sleepQuietly(long ms) {
        try {
            TimeUnit.MILLISECONDS.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
