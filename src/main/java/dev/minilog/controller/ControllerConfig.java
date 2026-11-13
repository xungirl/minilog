package dev.minilog.controller;

import java.nio.file.Path;
import java.util.Properties;

/**
 * @param brokerSessionTimeoutMs a broker that has not heartbeated for this long is declared dead
 *                               and loses leadership of its partitions
 */
public record ControllerConfig(String host, int port, Path dataDir, long brokerSessionTimeoutMs) {

    public static ControllerConfig fromProperties(Properties p) {
        return new ControllerConfig(
                p.getProperty("host", "localhost"),
                Integer.parseInt(p.getProperty("port", "9090")),
                Path.of(p.getProperty("data.dir", "data/controller")),
                Long.parseLong(p.getProperty("broker.session.timeout.ms", "1500")));
    }
}
