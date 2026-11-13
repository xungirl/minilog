package dev.minilog.network;

import dev.minilog.common.Logger;
import dev.minilog.protocol.Codec;
import dev.minilog.protocol.Frame;
import dev.minilog.protocol.Message;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 * A blocking TCP server with one <b>virtual thread per connection</b>.
 *
 * <p>Why not NIO selectors or Netty? With Java 21 virtual threads, plain blocking code
 * ({@code read frame -> handle -> write response}) scales to many thousands of connections
 * because a blocked virtual thread does not hold an OS thread. The code stays linear and easy to
 * read, and handlers can block (long-poll fetch, waiting for replication) without callbacks.
 *
 * <p>Requests on one connection are processed strictly in order, which gives clients the same
 * per-connection ordering guarantee Kafka gives.
 */
public final class SocketServer implements Closeable {
    private final Logger log;
    private final String host;
    private final int requestedPort;
    private final RequestHandler handler;
    private final Set<Socket> connections = ConcurrentHashMap.newKeySet();
    private volatile ServerSocket serverSocket;
    private volatile boolean running;
    /** Test hook: when it returns true for a request, the server closes the connection instead of replying. */
    private volatile Predicate<Message> dropResponseIf = m -> false;

    public SocketServer(String name, String host, int port, RequestHandler handler) {
        this.log = Logger.get(name);
        this.host = host;
        this.requestedPort = port;
        this.handler = handler;
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(host, requestedPort));
        running = true;
        Thread.ofVirtual().name("acceptor-" + port()).start(this::acceptLoop);
    }

    public int port() {
        return serverSocket.getLocalPort();
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                socket.setTcpNoDelay(true); // small request/response messages: don't wait for Nagle
                connections.add(socket);
                Thread.ofVirtual().name("conn-" + socket.getPort()).start(() -> serve(socket));
            } catch (IOException e) {
                if (running) {
                    log.warn("accept failed: %s", e);
                }
            }
        }
    }

    private void serve(Socket socket) {
        try (socket;
             DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 64 * 1024));
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 64 * 1024))) {
            while (running) {
                Codec.Request request = Codec.decodeRequest(Frame.read(in));
                Message response = handler.handle(request.api(), request.body());
                if (dropResponseIf.test(request.body())) {
                    return; // simulate a lost response (the request WAS processed)
                }
                if (response != null) {
                    Frame.write(out, Codec.encodeResponse(request.correlationId(), response));
                }
            }
        } catch (EOFException | SocketException e) {
            // client went away or server is closing: normal
        } catch (Exception e) {
            if (running) {
                log.warn("connection %s failed: %s", socket.getRemoteSocketAddress(), e);
            }
        } finally {
            connections.remove(socket);
        }
    }

    public void setDropResponseIf(Predicate<Message> predicate) {
        this.dropResponseIf = predicate;
    }

    @Override
    public void close() {
        running = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
            // closing
        }
        for (Socket s : connections) {
            try {
                s.close();
            } catch (IOException ignored) {
                // closing
            }
        }
    }
}
