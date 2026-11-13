package dev.minilog.tools;

import dev.minilog.client.Admin;
import dev.minilog.client.Consumer;
import dev.minilog.client.ConsumerConfig;
import dev.minilog.client.ConsumerRecord;
import dev.minilog.client.Producer;
import dev.minilog.client.ProducerConfig;
import dev.minilog.client.RecordMetadata;
import dev.minilog.protocol.ClusterMetadata;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

/**
 * Small command-line client.
 * <pre>
 * Cli BOOTSTRAP create-topic NAME PARTITIONS RF
 * Cli BOOTSTRAP describe
 * Cli BOOTSTRAP produce TOPIC          (one record per stdin line; "key:value" sets a key)
 * Cli BOOTSTRAP consume TOPIC GROUP    (prints partition/offset/key/value until Ctrl-C)
 * </pre>
 */
public final class Cli {
    private Cli() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: Cli BOOTSTRAP (create-topic NAME P RF | describe | produce TOPIC | consume TOPIC GROUP)");
            System.exit(2);
        }
        String bootstrap = args[0];
        switch (args[1]) {
            case "create-topic" -> {
                try (Admin admin = new Admin(bootstrap)) {
                    admin.createTopic(args[2], Integer.parseInt(args[3]), Integer.parseInt(args[4]));
                    System.out.println("created " + args[2]);
                }
            }
            case "describe" -> {
                try (Admin admin = new Admin(bootstrap)) {
                    ClusterMetadata md = admin.describe();
                    System.out.println("metadata version " + md.version() + ", controller " + md.controller());
                    md.brokers().forEach(b -> System.out.printf("broker %d %s:%d%n", b.id(), b.host(), b.port()));
                    for (ClusterMetadata.TopicMetadata t : md.topics()) {
                        for (ClusterMetadata.PartitionMetadata p : t.partitions()) {
                            System.out.printf("%s-%d leader=%d epoch=%d replicas=%s isr=%s%n", t.name(), p.partition(),
                                    p.leader(), p.leaderEpoch(), p.replicas(), p.isr());
                        }
                    }
                }
            }
            case "produce" -> {
                try (Producer producer = new Producer(ProducerConfig.defaults(bootstrap));
                     BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = in.readLine()) != null) {
                        int colon = line.indexOf(':');
                        byte[] key = colon > 0 ? line.substring(0, colon).getBytes(StandardCharsets.UTF_8) : null;
                        byte[] value = (colon > 0 ? line.substring(colon + 1) : line).getBytes(StandardCharsets.UTF_8);
                        RecordMetadata md = producer.send(args[2], key, value).get();
                        System.out.println("-> " + md.partition() + " @ " + md.offset());
                    }
                }
            }
            case "consume" -> {
                try (Consumer consumer = new Consumer(ConsumerConfig.defaults(bootstrap, args[3]))) {
                    consumer.subscribe(List.of(args[2]));
                    while (true) {
                        for (ConsumerRecord r : consumer.poll(Duration.ofMillis(500))) {
                            System.out.printf("%s @ %d key=%s value=%s%n", r.partition(), r.offset(),
                                    r.key() == null ? "null" : new String(r.key(), StandardCharsets.UTF_8),
                                    new String(r.value(), StandardCharsets.UTF_8));
                        }
                    }
                }
            }
            default -> System.err.println("unknown command " + args[1]);
        }
    }
}
