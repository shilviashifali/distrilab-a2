package common;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Reuses one gRPC channel per host:port. Channels are expensive to create
 * and can carry many concurrent calls, so each program keeps one cache.
 */
public final class Channels {

    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    public ManagedChannel get(String host, int port) {
        return channels.computeIfAbsent(host + ":" + port,
                k -> ManagedChannelBuilder.forAddress(host, port).usePlaintext().build());
    }

    /** Drops a channel to a node that has failed, so a restarted node gets a fresh connection. */
    public void close(String host, int port) {
        ManagedChannel ch = channels.remove(host + ":" + port);
        if (ch != null) {
            ch.shutdownNow();
        }
    }

    public void closeAll() {
        for (ManagedChannel ch : channels.values()) {
            ch.shutdownNow();
            try {
                ch.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        channels.clear();
    }
}
