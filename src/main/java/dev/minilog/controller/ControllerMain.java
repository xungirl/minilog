package dev.minilog.controller;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/** Entry point: {@code java -cp minilog.jar dev.minilog.controller.ControllerMain config/controller.properties [key=value ...]}. */
public final class ControllerMain {
    private ControllerMain() {
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
