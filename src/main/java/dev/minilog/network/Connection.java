package dev.minilog.network;

import dev.minilog.protocol.ApiKey;
import dev.minilog.protocol.Codec;
import dev.minilog.protocol.Frame;
import dev.minilog.protocol.Message;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Client side of one TCP connection: send a request, block for its response.
 *
 * <p>One request is in flight per connection at a time (guarded by a {@link ReentrantLock};
 * {@code synchronized} would pin a virtual thread to its carrier while it blocks on the socket in
 * Java 21). Any IOException leaves the connection unusable; callers close it and reconnect.
 */
public final class Connection implements Closeable {
    private final String host;
    private final int port;
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private final ReentrantLock lock = new ReentrantLock();
    private int nextCorrelationId;
    private volatile boolean broken;

    public Connection(String host, int port, int timeoutMs) throws IOException {
        this.host = host;
        this.port = port;
        this.socket = new Socket();
        socket.setTcpNoDelay(true);
        socket.connect(new InetSocketAddress(host, port), Math.min(timeoutMs, 3000));
        socket.setSoTimeout(timeoutMs);
        this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 64 * 1024));
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 64 * 1024));
    }

    /** Sends a request and waits for the response. */
    public Message send(ApiKey api, Message request) throws IOException {
        lock.lock();
        try {
            int correlationId = nextCorrelationId++;
            Frame.write(out, Codec.encodeRequest(api, correlationId, request));
            return Codec.decodeResponse(api, correlationId, Frame.read(in));
        } catch (IOException | RuntimeException e) {
            broken = true;
            close();
            throw e instanceof IOException io ? io : new IOException(e);
        } finally {
            lock.unlock();
        }
    }

    /** Fire-and-forget (produce with acks=0): the server sends no response. */
    public void sendNoResponse(ApiKey api, Message request) throws IOException {
        lock.lock();
        try {
            Frame.write(out, Codec.encodeRequest(api, nextCorrelationId++, request));
        } catch (IOException e) {
            broken = true;
            close();
            throw e;
        } finally {
            lock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends Message> T call(ApiKey api, Message request) throws IOException {
        return (T) send(api, request);
    }

    public boolean isBroken() {
        return broken || socket.isClosed();
    }

    @Override
    public void close() {
        broken = true;
        try {
            socket.close();
        } catch (IOException ignored) {
            // closing
        }
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}
