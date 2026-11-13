package dev.minilog.bench;

import java.util.Arrays;

/** Percentiles over a set of samples (nearest-rank method on the sorted samples). */
public final class Stats {
    private final long[] sorted;

    public Stats(long[] samples) {
        this.sorted = samples.clone();
        Arrays.sort(sorted);
    }

    public long percentile(double p) {
        if (sorted.length == 0) {
            return -1;
        }
        int rank = (int) Math.ceil(p / 100.0 * sorted.length);
        return sorted[Math.max(0, Math.min(sorted.length - 1, rank - 1))];
    }

    public long max() {
        return sorted.length == 0 ? -1 : sorted[sorted.length - 1];
    }

    public long min() {
        return sorted.length == 0 ? -1 : sorted[0];
    }

    public int count() {
        return sorted.length;
    }
}
