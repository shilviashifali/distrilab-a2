package distrilab.worker;

import distrilab.common.Channels;
import distrilab.common.Log;
import distrilab.grpc.BootstrapServiceGrpc;
import distrilab.grpc.Empty;
import distrilab.grpc.RandomWorkersRequest;
import distrilab.grpc.WorkerId;
import distrilab.grpc.WorkerInfo;
import distrilab.grpc.WorkerServiceGrpc;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Keeps this worker's neighbours in the unstructured network.
 * A joining worker connects to a few random active workers (not just one, so a
 * single failure cannot split the network). Neighbours are pinged regularly;
 * one missed ping = suspected, {@code missedLimit} in a row = failed and removed,
 * and the worker then connects to a new random worker to repair the network.
 */
public class NeighbourManager {

    private final BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrap;
    private final Channels channels;
    private final int target;
    private final int missedLimit;
    private final int timeoutMs;

    private final Map<Integer, WorkerInfo> neighbours = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> missed = new ConcurrentHashMap<>();
    private volatile WorkerInfo self;

    public NeighbourManager(BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrap, Channels channels,
                            int target, int missedLimit, int timeoutMs) {
        this.bootstrap = bootstrap;
        this.channels = channels;
        this.target = target;
        this.missedLimit = missedLimit;
        this.timeoutMs = timeoutMs;
    }

    /** Called once this worker has its ID from the Bootstrap Node. */
    public void start(WorkerInfo self) {
        this.self = self;
    }

    // ------------------------------------------------------------------
    // Joining and repairing the network
    // ------------------------------------------------------------------

    /** Connects to random active workers until this worker has {@code target} neighbours. */
    public void connectToRandom() {
        if (neighbours.size() >= target) {
            return;
        }
        List<WorkerInfo> candidates;
        try {
            candidates = bootstrap.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                    .getRandomActiveWorkers(RandomWorkersRequest.newBuilder()
                            .setExcludeId(self.getId())
                            .setCount(target + neighbours.size())
                            .build())
                    .getWorkersList();
        } catch (StatusRuntimeException e) {
            Log.error(name(), "Bootstrap unreachable while looking for neighbours: " + e.getStatus().getCode());
            return;
        }
        if (candidates.isEmpty() && neighbours.isEmpty()) {
            Log.info(name(), "First worker in the network");
            return;
        }
        for (WorkerInfo w : candidates) {
            if (neighbours.size() >= target) {
                break;
            }
            if (neighbours.containsKey(w.getId())) {
                continue;
            }
            try {
                stub(w).addNeighbour(self);
                neighbours.put(w.getId(), w);
                Log.info(name(), "Connected to Worker-" + w.getId() + ". Neighbours: " + ids());
            } catch (StatusRuntimeException e) {
                Log.error(name(), "Could not connect to Worker-" + w.getId() + ": " + e.getStatus().getCode());
            }
        }
    }

    /** Another worker connected to this one. */
    public void add(WorkerInfo w) {
        if (w.getId() == self.getId()) {
            return;
        }
        neighbours.put(w.getId(), w);
        missed.remove(w.getId());
        Log.info(name(), "Worker-" + w.getId() + " connected. Neighbours: " + ids());
    }

    /** A neighbour left the network. */
    public void remove(int id) {
        if (neighbours.remove(id) != null) {
            missed.remove(id);
            Log.info(name(), "Worker-" + id + " disconnected. Neighbours: " + ids());
        }
    }

    // ------------------------------------------------------------------
    // Failure detection between neighbours
    // ------------------------------------------------------------------

    /** Runs on a schedule: pings every neighbour, removes failed ones, then repairs the network. */
    public void check() {
        try {
            for (WorkerInfo w : new ArrayList<>(neighbours.values())) {
                ping(w);
            }
            connectToRandom();
        } catch (RuntimeException e) {
            Log.error(name(), "Neighbour check failed: " + e.getMessage()); // keep the scheduler alive
        }
    }

    private void ping(WorkerInfo w) {
        try {
            stub(w).ping(Empty.getDefaultInstance());
            missed.remove(w.getId());
        } catch (StatusRuntimeException e) {
            int count = missed.merge(w.getId(), 1, Integer::sum);
            if (count < missedLimit) {
                Log.info(name(), "Neighbour Worker-" + w.getId() + " missed ping " + count + "/" + missedLimit);
                return;
            }
            neighbours.remove(w.getId());
            missed.remove(w.getId());
            channels.close(w.getHost(), w.getPort());
            Log.info(name(), "Neighbour Worker-" + w.getId() + " FAILED - removed. Neighbours: " + ids());
        }
    }

    /** Tells every neighbour this worker is leaving. */
    public void leave() {
        for (WorkerInfo w : neighbours.values()) {
            try {
                stub(w).removeNeighbour(WorkerId.newBuilder().setId(self.getId()).build());
            } catch (StatusRuntimeException ignored) {
                // neighbour already gone
            }
        }
        neighbours.clear();
    }

    // ------------------------------------------------------------------
    // Accessors (also used by election and job code)
    // ------------------------------------------------------------------

    public List<WorkerInfo> all() {
        return new ArrayList<>(neighbours.values());
    }

    public List<Integer> ids() {
        return neighbours.keySet().stream().sorted().toList();
    }

    /** Blocking stub to any worker, with the standard deadline. */
    public WorkerServiceGrpc.WorkerServiceBlockingStub stub(WorkerInfo w) {
        return WorkerServiceGrpc.newBlockingStub(channels.get(w.getHost(), w.getPort()))
                .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);
    }

    private String name() {
        return self == null ? "Worker" : "Worker-" + self.getId();
    }
}
