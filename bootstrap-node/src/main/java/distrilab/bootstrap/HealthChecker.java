package distrilab.bootstrap;

import distrilab.common.Channels;
import distrilab.common.Log;
import distrilab.grpc.Empty;
import distrilab.grpc.WorkerInfo;
import distrilab.grpc.WorkerServiceGrpc;
import io.grpc.StatusRuntimeException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Pings every active worker on a fixed interval.
 * One failed ping only marks a worker as suspected (it may just be slow or busy);
 * it is removed after {@code missedLimit} failures in a row. This avoids removing
 * a slow worker because of a single delayed reply.
 */
public class HealthChecker {

    private static final String NAME = "Bootstrap";

    private final WorkerRegistry registry;
    private final Channels channels;
    private final int intervalSeconds;
    private final int missedLimit;
    private final int timeoutMs;
    private final Map<Integer, Integer> missed = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public HealthChecker(WorkerRegistry registry, Channels channels,
                         int intervalSeconds, int missedLimit, int timeoutMs) {
        this.registry = registry;
        this.channels = channels;
        this.intervalSeconds = intervalSeconds;
        this.missedLimit = missedLimit;
        this.timeoutMs = timeoutMs;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::checkAll, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    private void checkAll() {
        try {
            for (WorkerInfo w : registry.active()) {
                check(w);
            }
        } catch (RuntimeException e) {
            Log.error(NAME, "Health check failed: " + e.getMessage()); // keep the scheduler alive
        }
    }

    private void check(WorkerInfo w) {
        try {
            WorkerServiceGrpc.newBlockingStub(channels.get(w.getHost(), w.getPort()))
                    .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                    .ping(Empty.getDefaultInstance());
            if (missed.remove(w.getId()) != null) {
                Log.info(NAME, "Worker-" + w.getId() + " is responding again");
            }
        } catch (StatusRuntimeException e) {
            int count = missed.merge(w.getId(), 1, Integer::sum);
            if (count < missedLimit) {
                Log.info(NAME, "Worker-" + w.getId() + " missed ping " + count + "/" + missedLimit
                        + " (" + e.getStatus().getCode() + ") - suspected");
                return;
            }
            missed.remove(w.getId());
            boolean wasCoordinator = registry.remove(w.getId());
            channels.close(w.getHost(), w.getPort());
            Log.info(NAME, "Worker-" + w.getId() + " FAILED (" + missedLimit + " missed pings) - removed");
            if (wasCoordinator) {
                Log.info(NAME, "Failed worker was the coordinator - waiting for a new election");
            }
        }
    }
}
