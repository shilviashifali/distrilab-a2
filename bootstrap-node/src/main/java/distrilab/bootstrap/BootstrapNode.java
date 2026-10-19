package distrilab.bootstrap;

import distrilab.common.Channels;
import distrilab.common.Defaults;
import distrilab.common.Log;
import distrilab.common.Settings;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;

/**
 * Bootstrap Node: assigns worker IDs, keeps the list of active workers,
 * removes failed workers, and records the current coordinator.
 *
 * Usage: java -jar bootstrap-node.jar [--port=50050]
 */
public class BootstrapNode {

    private static final String NAME = "Bootstrap";

    private final int port;
    private final Channels channels = new Channels();
    private final WorkerRegistry registry = new WorkerRegistry();
    private final HealthChecker healthChecker;
    private Server server;

    public BootstrapNode(Settings settings) {
        this.port = settings.getInt("port", Defaults.BOOTSTRAP_PORT);
        this.healthChecker = new HealthChecker(registry, channels,
                settings.getInt("pingInterval", Defaults.PING_INTERVAL_SECONDS),
                settings.getInt("missedPings", Defaults.MISSED_PINGS_FOR_FAILURE),
                settings.getInt("rpcTimeoutMs", Defaults.RPC_TIMEOUT_MS));
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new BootstrapServiceImpl(registry))
                .build()
                .start();
        healthChecker.start();
        Log.info(NAME, "Running on port " + port);
    }

    public void stop() {
        healthChecker.stop();
        channels.closeAll();
        if (server != null) {
            server.shutdownNow();
        }
        Log.info(NAME, "Stopped");
    }

    public static void main(String[] args) {
        try {
            BootstrapNode node = new BootstrapNode(Settings.load(args));
            node.start();
            Runtime.getRuntime().addShutdownHook(new Thread(node::stop));
            node.server.awaitTermination();
        } catch (IllegalArgumentException e) {
            Log.error(NAME, e.getMessage());
        } catch (IOException e) {
            Log.error(NAME, "Could not start server (port in use?): " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
