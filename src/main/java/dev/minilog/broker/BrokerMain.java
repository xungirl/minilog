package dev.minilog.broker;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/** Entry point: {@code java -cp minilog.jar dev.minilog.broker.BrokerMain config/broker-1.properties [key=value ...]}. */
public final class BrokerMain {
    private BrokerMain() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties props = new Properties();
        for (String arg : args) {
            if (arg.contains("=")) {
                String[] kv = arg.split("=", 2);
                props.setProperty(kv[0], kv[1]);
            } else {
                try (InputStream in = Files.newInputStream(Path.of(arg))) {
                    Properties fromFile = new Properties();
                    fromFile.load(in);
                    fromFile.forEach(props::putIfAbsent);
                }
            }
        }
        Broker broker = new Broker(BrokerConfig.fromProperties(props)).start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                broker.close();
            } catch (IOException ignored) {
                // exiting
            }
        }));
        Thread.currentThread().join();
    }
}
