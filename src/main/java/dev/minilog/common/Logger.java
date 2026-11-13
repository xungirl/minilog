package dev.minilog.common;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * A deliberately tiny logger (stderr, one line per event) so the project has no logging dependency.
 * Set the system property {@code minilog.log=debug} to see debug lines.
 */
public final class Logger {
    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final boolean DEBUG = "debug".equalsIgnoreCase(System.getProperty("minilog.log", "info"));
    private static final boolean QUIET = "quiet".equalsIgnoreCase(System.getProperty("minilog.log", "info"));

    private final String name;

    private Logger(String name) {
        this.name = name;
    }

    public static Logger get(String name) {
        return new Logger(name);
    }

    public void info(String fmt, Object... args) {
        if (!QUIET) {
            print("INFO ", fmt, args);
        }
    }

    public void warn(String fmt, Object... args) {
        print("WARN ", fmt, args);
    }

    public void debug(String fmt, Object... args) {
        if (DEBUG) {
            print("DEBUG", fmt, args);
        }
    }

    private void print(String level, String fmt, Object... args) {
        String msg = args.length == 0 ? fmt : String.format(fmt, args);
        System.err.println(LocalTime.now().format(FMT) + " " + level + " [" + name + "] " + msg);
    }
}
