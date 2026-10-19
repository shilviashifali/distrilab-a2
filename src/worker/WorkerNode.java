package worker;

import common.Channels;
import common.Defaults;
import common.Log;
import common.Settings;
import grpc.BootstrapServiceGrpc;
import grpc.Empty;
import grpc.JobRequest;
import grpc.JobType;
import grpc.RegisterRequest;
import grpc.WorkerId;
import grpc.WorkerInfo;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A worker node: starts its gRPC server, registers with the Bootstrap Node,
 * joins the unstructured network, keeps its neighbours healthy, computes tasks,
 * and (as coordinator) distributes jobs.
 *
 * Usage: WorkerNode [--bootstrapHost=localhost] [--bootstrapPort=50050]
 *                   [--host=localhost] [--port=0 (any free port)]
 */
public class WorkerNode {

    private final String host;
    private final int requestedPort;
    private final int pingInterval;
    private final int timeoutMs;
    private final String bootstrapAddress;

    private final Channels channels = new Channels();
    private final BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrap;
    private final NeighbourManager neighbours;
    private final TaskWorker taskWorker;
    private final JobManager jobManager;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final AtomicBoolean left = new AtomicBoolean(false);

    private Server server;
    private volatile WorkerInfo self;

    public WorkerNode(Settings s) {
        this.host = s.get("host", "localhost");
        this.requestedPort = s.getInt("port", Defaults.WORKER_PORT);
        this.pingInterval = s.getInt("pingInterval", Defaults.PING_INTERVAL_SECONDS);
        this.timeoutMs = s.getInt("rpcTimeoutMs", Defaults.RPC_TIMEOUT_MS);
        String bsHost = s.get("bootstrapHost", Defaults.BOOTSTRAP_HOST);
        int bsPort = s.getInt("bootstrapPort", Defaults.BOOTSTRAP_PORT);
        this.bootstrapAddress = bsHost + ":" + bsPort;
        this.bootstrap = BootstrapServiceGrpc.newBlockingStub(channels.get(bsHost, bsPort));
        this.neighbours = new NeighbourManager(bootstrap, channels,
                s.getInt("neighbours", Defaults.NEIGHBOURS),
                s.getInt("missedPings", Defaults.MISSED_PINGS_FOR_FAILURE),
                timeoutMs);
        this.taskWorker = new TaskWorker(this, s.getInt("workerThreads", Defaults.WORKER_THREADS));
        this.jobManager = new JobManager(this,
                s.getInt("tasksPerWorker", Defaults.TASKS_PER_WORKER),
                s.getInt("slowAfterSeconds", Defaults.SLOW_AFTER_SECONDS));
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    /** Starts the server first (so the Bootstrap can ping us), then registers and joins the network. */
    public void start() throws IOException {
        server = ServerBuilder.forPort(requestedPort)
                .addService(new WorkerServiceImpl(neighbours, taskWorker))
                .addService(new JobServiceImpl(jobManager, this))
                // Person 2: .addService(new ElectionServiceImpl(...))
                .build()
                .start();
        try {
            self = bootstrapStub().register(RegisterRequest.newBuilder()
                    .setHost(host).setPort(server.getPort()).build());
        } catch (StatusRuntimeException e) {
            server.shutdownNow();
            throw new IllegalStateException("Cannot reach Bootstrap at " + bootstrapAddress
                    + " (" + e.getStatus().getCode() + "). Is it running?");
        }
        Log.info(name(), "Registered with Bootstrap, listening on " + host + ":" + server.getPort());

        neighbours.start(self);
        neighbours.connectToRandom();
        scheduler.scheduleAtFixedRate(neighbours::check, pingInterval, pingInterval, TimeUnit.SECONDS);
        jobManager.start();
        // Person 2: schedule the coordinator check / election start here
    }

    public void leave() {
        if (!left.compareAndSet(false, true)) {
            return;
        }
        scheduler.shutdownNow();
        jobManager.stop();
        taskWorker.shutdown();
        neighbours.leave();
        try {
            bootstrapStub().unregister(WorkerId.newBuilder().setId(self.getId()).build());
        } catch (StatusRuntimeException e) {
            Log.error(name(), "Could not unregister: " + e.getStatus().getCode());
        }
        server.shutdownNow();
        channels.closeAll();
        Log.info(name(), "Left the network");
    }

    // ------------------------------------------------------------------
    // Accessors for election and job code
    // ------------------------------------------------------------------

    public WorkerInfo self() {
        return self;
    }

    public NeighbourManager neighbours() {
        return neighbours;
    }

    public JobManager jobManager() {
        return jobManager;
    }

    public Channels channels() {
        return channels;
    }

    /** Bootstrap stub with a fresh deadline - call this for every request. */
    public BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrapStub() {
        return bootstrap.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public String name() {
        return self == null ? "Worker" : "Worker-" + self.getId();
    }

    // ------------------------------------------------------------------
    // Console
    // ------------------------------------------------------------------

    private static final String JOB_USAGE = "Usage: job PRIMESUM 1 20000000 | job MAX 4,8,15,16 | job PRIMECOUNT 2,3,4,5,7";

    private void consoleLoop() {
        Log.info(name(), "Commands: n = neighbours, w = all workers, q = quit, job ... = submit a test job");
        Log.info(name(), JOB_USAGE);
        Scanner in = new Scanner(System.in);
        while (in.hasNextLine()) {
            String line = in.nextLine().trim();
            if (line.toLowerCase().startsWith("job ")) {
                submitFromConsole(line.substring(4).trim());
                continue;
            }
            switch (line.toLowerCase()) {
                case "n" -> Log.info(name(), "Neighbours: " + neighbours.ids());
                case "w" -> showAllWorkers();
                case "q" -> {
                    leave();
                    System.exit(0);
                }
                // Person 2: "e" (start election) and "s" (coordinator, term, JAC)
                default -> { }
            }
        }
    }

    /** Test helper until the client GUI exists: this worker acts as coordinator for the job. */
    private void submitFromConsole(String args) {
        try {
            String[] parts = args.split("\\s+", 2);
            JobType type = JobType.valueOf(parts[0].toUpperCase());
            JobRequest.Builder request = JobRequest.newBuilder().setClientId("console-" + self.getId()).setType(type);
            if (type == JobType.PRIMESUM) {
                String[] range = parts[1].trim().split("[\\s,]+");
                request.setStart(Long.parseLong(range[0])).setEnd(Long.parseLong(range[1]));
            } else {
                for (String n : parts[1].split(",")) {
                    if (!n.isBlank()) {
                        request.addNumbers(Long.parseLong(n.trim()));
                    }
                }
            }
            String jobId = jobManager.submit(request.build());
            jobManager.watch(jobId, new ConsoleProgress(name()));
        } catch (IllegalArgumentException | ArrayIndexOutOfBoundsException e) {
            Log.error(name(), "Invalid job (" + e.getMessage() + "). " + JOB_USAGE);
        }
    }

    private void showAllWorkers() {
        try {
            Log.info(name(), "Active workers: " + bootstrapStub().getActiveWorkers(Empty.getDefaultInstance())
                    .getWorkersList().stream().map(WorkerInfo::getId).sorted().toList());
        } catch (StatusRuntimeException e) {
            Log.error(name(), "Bootstrap unreachable: " + e.getStatus().getCode());
        }
    }

    public static void main(String[] args) {
        WorkerNode node;
        try {
            node = new WorkerNode(Settings.load(args));
            node.start();
        } catch (IllegalArgumentException | IllegalStateException e) {
            Log.error("Worker", e.getMessage());
            System.exit(1);
            return;
        } catch (IOException e) {
            Log.error("Worker", "Could not start server (port in use?): " + e.getMessage());
            System.exit(1);
            return;
        }
        Runtime.getRuntime().addShutdownHook(new Thread(node::leave));
        node.consoleLoop();
    }
}
