package bootstrap;

import common.*;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class BootstrapNode extends UnicastRemoteObject implements BootstrapService {
    private static final long serialVersionUID = 1L;
    private static final String NAME = "Bootstrap";

    private final Map<Integer, WorkerService> workers = new ConcurrentHashMap<>();
    private final AtomicInteger nextId = new AtomicInteger(1);
    private final Random random = new Random();
    private volatile int coordinatorId = -1;
    private final ScheduledExecutorService healthChecker =
            Executors.newSingleThreadScheduledExecutor();

    public BootstrapNode() throws RemoteException {
        super();
    }

    @Override
    public int register(WorkerService worker) {
        int id = nextId.getAndIncrement();
        workers.put(id, worker);
        Log.info(NAME, "Worker-" + id + " registered. Active: " + workers.keySet());
        return id;
    }

    @Override
    public void unregister(int workerId) {
        if (workers.remove(workerId) != null) {
            Log.info(NAME, "Worker-" + workerId + " removed. Active: " + workers.keySet());
        }
        if (workerId == coordinatorId) {
            coordinatorId = -1;
            Log.info(NAME, "Coordinator left - no active coordinator");
        }
    }

    @Override
    public WorkerInfo getRandomActiveWorker(int excludeId) {
        List<Integer> ids = new ArrayList<>(workers.keySet());
        ids.remove(Integer.valueOf(excludeId));
        if (ids.isEmpty()) {
            return null;
        }
        int chosen = ids.get(random.nextInt(ids.size()));
        WorkerService stub = workers.get(chosen);
        return stub == null ? null : new WorkerInfo(chosen, stub);
    }

    @Override
    public Map<Integer, WorkerService> getActiveWorkers() {
        return new HashMap<>(workers);
    }

    @Override
    public void setCoordinator(int workerId) {
        coordinatorId = workerId;
        Log.info(NAME, "Coordinator is now Worker-" + workerId);
    }

    @Override
    public WorkerInfo getCoordinator() {
        int id = coordinatorId;
        WorkerService stub = workers.get(id);
        return stub == null ? null : new WorkerInfo(id, stub);
    }

    private void startHealthChecks() {
        healthChecker.scheduleAtFixedRate(this::checkHealth,
                Config.HEARTBEAT_SECONDS, Config.HEARTBEAT_SECONDS, TimeUnit.SECONDS);
    }

    private void checkHealth() {
        try {
            for (Map.Entry<Integer, WorkerService> e : workers.entrySet()) {
                try {
                    e.getValue().ping();
                } catch (RemoteException ex) {
                    Log.info(NAME, "Worker-" + e.getKey() + " unreachable");
                    unregister(e.getKey());
                }
            }
        } catch (Exception e) {
            Log.error(NAME, "Health check failed: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        int port = Config.DEFAULT_BOOTSTRAP_PORT;
        String host = Config.DEFAULT_BOOTSTRAP_HOST;
        try {
            if (args.length > 0) port = Integer.parseInt(args[0]);
            if (args.length > 1) host = args[1];
        } catch (NumberFormatException e) {
            Log.error(NAME, "Invalid port: " + args[0]);
            return;
        }

        System.setProperty("java.rmi.server.hostname", host);
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", Config.RMI_TIMEOUT_MS);

        try {
            Registry registry = LocateRegistry.createRegistry(port);
            BootstrapNode node = new BootstrapNode();
            registry.rebind(Config.BOOTSTRAP_NAME, node);
            node.startHealthChecks();
            Log.info(NAME, "Running on " + host + ":" + port);
        } catch (RemoteException e) {
            Log.error(NAME, "Failed to start: " + e.getMessage());
        }
    }
}