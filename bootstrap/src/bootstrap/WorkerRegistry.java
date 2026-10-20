package bootstrap;

import grpc.WorkerInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe record of active workers and the current coordinator.
 * gRPC calls and the health checker run on different threads, so all state
 * is in concurrent collections and the coordinator fields are updated together under a lock.
 */
public class WorkerRegistry {

    private final Map<Integer, WorkerInfo> workers = new ConcurrentHashMap<>();
    private final AtomicInteger nextId = new AtomicInteger(1);

    private final Object coordinatorLock = new Object();
    private int coordinatorId = -1;
    private long term = 0;

    /** Assigns the next unique ID and stores the worker's address. */
    public WorkerInfo register(String host, int port) {
        int id = nextId.getAndIncrement();
        WorkerInfo info = WorkerInfo.newBuilder().setId(id).setHost(host).setPort(port).build();
        workers.put(id, info);
        return info;
    }

    /** Removes a worker. Returns true if it was the coordinator. */
    public boolean remove(int id) {
        if (workers.remove(id) == null) {
            return false;
        }
        synchronized (coordinatorLock) {
            if (coordinatorId == id) {
                coordinatorId = -1;
                return true;
            }
        }
        return false;
    }

    public List<WorkerInfo> active() {
        return new ArrayList<>(workers.values());
    }

    public Optional<WorkerInfo> find(int id) {
        return Optional.ofNullable(workers.get(id));
    }

    /** Up to {@code count} random active workers, excluding one ID. */
    public List<WorkerInfo> random(int excludeId, int count) {
        List<WorkerInfo> candidates = new ArrayList<>(workers.values());
        candidates.removeIf(w -> w.getId() == excludeId);
        Collections.shuffle(candidates, ThreadLocalRandom.current());
        return candidates.subList(0, Math.min(count, candidates.size()));
    }

    /**
     * Records an election winner. A result from an older term is rejected, so a
     * coordinator that was cut off and comes back cannot overwrite the new one.
     */
    public boolean setCoordinator(int id, long newTerm) {
        synchronized (coordinatorLock) {
            if (newTerm < term || !workers.containsKey(id)) {
                return false;
            }
            coordinatorId = id;
            term = newTerm;
            return true;
        }
    }

    /** Current coordinator, if it is still active. */
    public Optional<WorkerInfo> coordinator() {
        synchronized (coordinatorLock) {
            return coordinatorId == -1 ? Optional.empty() : find(coordinatorId);
        }
    }

    public long term() {
        synchronized (coordinatorLock) {
            return term;
        }
    }
}
