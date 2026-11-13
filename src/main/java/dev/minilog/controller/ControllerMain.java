package dev.minilog.controller;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/** Entry point: {@code java -cp minilog.jar dev.minilog.controller.ControllerMain config/controller.properties}. */
public final class ControllerMain {
    private ControllerMain() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties props = new Properties();
        if (args.length > 0) {
            try (InputStream in = Files.newInputStream(Path.of(args[0]))) {
                props.load(in);
            }
        }
        ControllerServer server = new ControllerServer(ControllerConfig.fromProperties(props)).start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.close();
            } catch (IOException ignored) {
                // exiting
            }
        }));
        Thread.currentThread().join();
    }
}
