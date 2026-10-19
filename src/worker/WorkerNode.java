package worker;

import common.*;
import jobs.TaskExecutor;
import java.io.Serializable;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A worker node in the DistriLab network.
 * Handles RMI, neighbour management and failure detection, and delegates
 * leader election to ElectionManager and job coordination to JobCoordinator.
 */
public class WorkerNode extends UnicastRemoteObject implements WorkerService {
    private static final long serialVersionUID = 1L;

    private final String leaderman = "cs324"; // required by the assignment

    private final BootstrapService bootstrap;
    private final Map<Integer, WorkerService> neighbours = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final AtomicBoolean left = new AtomicBoolean(false);
    private volatile int id = -1;

    private final TaskExecutor taskExecutor;
    private final ElectionManager election;
    private final JobCoordinator jobCoordinator;

    public WorkerNode(BootstrapService bootstrap) throws RemoteException {
        super(0);
        this.bootstrap = bootstrap;
        this.taskExecutor = new TaskExecutor(Config.WORKER_THREADS);
        this.election = new ElectionManager(this);
        this.jobCoordinator = new JobCoordinator(this, election);
    }

    // =====================================================================
    // Network (remote)
    // =====================================================================

    @Override
    public int getId() {
        return id;
    }

    @Override
    public boolean ping() {
        return true;
    }

    @Override
    public void addNeighbour(int neighbourId, WorkerService neighbour) {
        neighbours.put(neighbourId, neighbour);
        Log.info(displayName(), "Worker-" + neighbourId + " connected. Neighbours: " + neighbours.keySet());
    }

    @Override
    public void removeNeighbour(int neighbourId) {
        if (neighbours.remove(neighbourId) != null) {
            Log.info(displayName(), "Worker-" + neighbourId + " disconnected. Neighbours: " + neighbours.keySet());
        }
    }

    @Override
    public Set<Integer> getNeighbourIds() {
        return new HashSet<>(neighbours.keySet());
    }

    // =====================================================================
    // Leader election (remote) - delegated to ElectionManager
    // =====================================================================

    @Override
    public int getJac() {
        return election.getJac();
    }

    @Override
    public int getCoordinatorId() {
        return election.getCoordinatorId();
    }

    @Override
    public ElectionCandidate receiveElection(String electionId, int senderId) {
        return election.receiveElection(electionId, senderId);
    }

    @Override
    public void receiveCoordinator(String electionId, int coordinatorId) {
        election.receiveCoordinator(electionId, coordinatorId);
    }

    @Override
    public void startElection() {
        election.startElection();
    }

    // =====================================================================
    // Distributed jobs (remote) - delegated to TaskExecutor / JobCoordinator
    // =====================================================================

    @Override
    public <R extends Serializable> R executeTask(Job<R> task) throws RemoteException {
        return taskExecutor.run(task);
    }

    @Override
    public JobResult submitJob(Job<?> job) throws RemoteException {
        return jobCoordinator.submit(job);
    }

    // =====================================================================
    // Local accessors used by ElectionManager and JobCoordinator
    // =====================================================================

    public Map<Integer, WorkerService> getNeighbours() {
        return Collections.unmodifiableMap(neighbours);
    }

    public BootstrapService getBootstrap() {
        return bootstrap;
    }

    public String displayName() {
        return "Worker-" + id;
    }

    // =====================================================================
    // Lifecycle
    // =====================================================================

    public void join() throws RemoteException {
        id = bootstrap.register(this);
        Log.info(displayName(), "Registered with bootstrap");
        if (!connectToRandomWorker()) {
            Log.info(displayName(), "First worker in the network");
        }
        scheduler.scheduleAtFixedRate(this::checkNeighbours,
                Config.HEARTBEAT_SECONDS, Config.HEARTBEAT_SECONDS, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(election::checkCoordinator,
                Config.ELECTION_START_DELAY_SECONDS, Config.HEARTBEAT_SECONDS, TimeUnit.SECONDS);
    }

    private boolean connectToRandomWorker() {
        try {
            WorkerInfo peer = bootstrap.getRandomActiveWorker(id);
            if (peer == null) {
                return false;
            }
            peer.getStub().addNeighbour(id, this);
            neighbours.put(peer.getId(), peer.getStub());
            Log.info(displayName(), "Connected to Worker-" + peer.getId());
            return true;
        } catch (RemoteException e) {
            Log.error(displayName(), "Could not connect to peer: " + e.getMessage());
            return false;
        }
    }

    private void checkNeighbours() {
        try {
            boolean lostNeighbour = false;
            for (Map.Entry<Integer, WorkerService> e : neighbours.entrySet()) {
                try {
                    e.getValue().ping();
                } catch (RemoteException ex) {
                    neighbours.remove(e.getKey());
                    lostNeighbour = true;
                    Log.info(displayName(), "Neighbour Worker-" + e.getKey() + " unreachable, removed");
                }
            }
            if (lostNeighbour || neighbours.isEmpty()) {
                connectToRandomWorker(); // repair the network
            }
        } catch (Exception e) {
            Log.error(displayName(), "Heartbeat failed: " + e.getMessage());
        }
    }

    public void leave() {
        if (!left.compareAndSet(false, true)) {
            return;
        }
        scheduler.shutdownNow();
        election.shutdown();
        jobCoordinator.shutdown();
        taskExecutor.shutdown();
        for (WorkerService n : neighbours.values()) {
            try {
                n.removeNeighbour(id);
            } catch (RemoteException ignored) {
                // neighbour already gone
            }
        }
        try {
            bootstrap.unregister(id);
        } catch (RemoteException e) {
            Log.error(displayName(), "Could not unregister: " + e.getMessage());
        }
        try {
            UnicastRemoteObject.unexportObject(this, true);
        } catch (NoSuchObjectException ignored) {
            // already unexported
        }
        Log.info(displayName(), "Left the network");
    }

    private void consoleLoop() {
        Scanner in = new Scanner(System.in);
        Log.info(displayName(), "Commands: e = start election, s = status, n = neighbours, w = all workers, q = quit");
        while (in.hasNextLine()) {
            switch (in.nextLine().trim().toLowerCase()) {
                case "e":
                    election.startElection();
                    break;
                case "s":
                    Log.info(displayName(), "Coordinator: Worker-" + election.getCoordinatorId()
                            + ", my JAC: " + election.getJac());
                    break;
                case "n":
                    Log.info(displayName(), "Neighbours: " + neighbours.keySet());
                    break;
                case "w":
                    try {
                        Log.info(displayName(), "Active workers: " + bootstrap.getActiveWorkers().keySet());
                    } catch (RemoteException e) {
                        Log.error(displayName(), "Bootstrap unreachable: " + e.getMessage());
                    }
                    break;
                case "q":
                    leave();
                    System.exit(0);
                    break;
                default:
                    break;
            }
        }
    }

    public static void main(String[] args) {
        String bsHost = args.length > 0 ? args[0] : Config.DEFAULT_BOOTSTRAP_HOST;
        String myHost = args.length > 2 ? args[2] : "localhost";
        int bsPort;
        try {
            bsPort = args.length > 1 ? Integer.parseInt(args[1]) : Config.DEFAULT_BOOTSTRAP_PORT;
        } catch (NumberFormatException e) {
            Log.error("Worker", "Invalid port: " + args[1]);
            return;
        }

        System.setProperty("java.rmi.server.hostname", myHost);
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", Config.RMI_TIMEOUT_MS);

        try {
            Registry registry = LocateRegistry.getRegistry(bsHost, bsPort);
            BootstrapService bootstrap = (BootstrapService) registry.lookup(Config.BOOTSTRAP_NAME);
            WorkerNode worker = new WorkerNode(bootstrap);
            worker.join();
            Runtime.getRuntime().addShutdownHook(new Thread(worker::leave));
            worker.consoleLoop();
        } catch (RemoteException | NotBoundException e) {
            Log.error("Worker", "Cannot reach bootstrap at " + bsHost + ":" + bsPort
                    + " - " + e.getMessage());
        }
    }
}