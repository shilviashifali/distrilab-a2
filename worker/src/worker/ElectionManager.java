package worker;

import common.Log;
import grpc.Ack;
import grpc.Candidate;
import grpc.ClusterState;
import grpc.CoordinatorMessage;
import grpc.CoordinatorReply;
import grpc.CoordinatorUpdate;
import grpc.ElectionMessage;
import grpc.ElectionReply;
import grpc.ElectionServiceGrpc;
import grpc.Empty;
import grpc.WorkerInfo;
import io.grpc.StatusRuntimeException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Leader election, coordinator failure detection and state recovery for one worker.
 *
 * ELECTION  - echo algorithm over the unstructured network (same idea as A1, now over gRPC).
 *             Best candidate = lowest JAC (Job Allocation Counter); highest worker ID breaks a tie.
 * TERMS     - a coordinator accepts jobsPerTerm jobs (A1: 5); when they have all finished it hands
 *             over by starting a new election. Each election produces a higher term number and
 *             anything carrying an older term is ignored.
 * DETECTION - every worker pings the coordinator each interval. A node that answers pings is
 *             alive (even if busy = slow). {@code missedLimit} misses in a row = FAILED.
 *             The Bootstrap Node's own health check is a second, independent detector.
 * RECOVERY  - the coordinator copies its job state (ClusterState) to every worker after each
 *             change. The new coordinator restores the newest copy and carries on.
 */
public class ElectionManager {

    private final WorkerNode node;
    private final int timeoutMs;          // short calls (pings, bootstrap)
    private final int electionTimeoutMs;  // election calls wait for a whole branch of the network
    private final int missedLimit;
    private final long electionDelayMs;   // how long with no coordinator before electing
    private final int jobsPerTerm;        // jobs per coordinator term (0 = never rotate)

    // ---- election state ----
    private final AtomicInteger jac = new AtomicInteger(0);   // Job Allocation Counter
    private final AtomicLong term = new AtomicLong(0);        // highest term seen
    private volatile int coordinatorId = -1;                  // -1 = unknown
    private volatile WorkerInfo coordinator;                  // address of the coordinator (to ping it)
    private volatile int deadCoordinatorId = -1;              // coordinator we declared failed
    private final Set<String> processedElections = ConcurrentHashMap.newKeySet();
    private final Set<String> processedCoordinators = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean electing = new AtomicBoolean(false);
    private final AtomicInteger termJobs = new AtomicInteger(0);          // jobs accepted this term
    private final AtomicBoolean termEnding = new AtomicBoolean(false);    // hand-over already started

    // ---- failure detection state ----
    private int missedCoordinatorPings = 0;
    private long noCoordinatorSince = -1;
    private final Random random = new Random();

    // ---- replicated state ----
    private volatile ClusterState latestState = ClusterState.getDefaultInstance();
    private final AtomicLong version = new AtomicLong(0);
    private final AtomicBoolean replicatePending = new AtomicBoolean(false);
    private final ExecutorService replicator = Executors.newSingleThreadExecutor(daemon("replicator"));
    private final ExecutorService flooder = Executors.newCachedThreadPool(daemon("coordinator-flood"));
    private final ExecutorService termElection = Executors.newSingleThreadExecutor(daemon("term-election"));

    public ElectionManager(WorkerNode node, int missedLimit, int electionDelaySeconds, int jobsPerTerm, int timeoutMs) {
        this.node = node;
        this.missedLimit = missedLimit;
        this.electionDelayMs = electionDelaySeconds * 1000L;
        this.jobsPerTerm = jobsPerTerm;
        this.timeoutMs = timeoutMs;
        this.electionTimeoutMs = Math.max(timeoutMs * 4, 5000);
    }

    // =====================================================================
    // Read-only views (console command "s", JobServiceImpl)
    // =====================================================================

    public int jac() {
        return jac.get();
    }

    public long term() {
        return term.get();
    }

    public int coordinatorId() {
        return coordinatorId;
    }

    public boolean isCoordinator() {
        return node.self() != null && coordinatorId == node.self().getId();
    }

    public String status() {
        String who = coordinatorId == -1 ? "unknown (no coordinator)"
                : "Worker-" + coordinatorId + (isCoordinator() ? " (me)" : "");
        return "Coordinator: " + who + " | term " + term.get() + " | my JAC " + jac.get()
                + (isCoordinator() ? " | term jobs " + termJobs.get() + "/" + (jobsPerTerm > 0 ? String.valueOf(jobsPerTerm) : "unlimited") : "");
    }

    /** JAC goes up each time this coordinator hands a task to ANOTHER worker. */
    public void recordAssignment(int workerId) {
        if (node.self() != null && workerId != node.self().getId()) {
            jac.incrementAndGet();
        }
    }

    // =====================================================================
    // Coordinator terms (A1: a coordinator serves jobsPerTerm jobs, then a new election)
    // =====================================================================

    /** Called for every NEW job. Only the coordinator accepts jobs, and only jobsPerTerm of them per term. */
    public void reserveJobSlot() {
        if (!isCoordinator()) {
            throw new IllegalStateException("Not the coordinator (current coordinator: Worker-" + coordinatorId + ")");
        }
        int n = termJobs.incrementAndGet();
        if (jobsPerTerm > 0 && n > jobsPerTerm) {
            termJobs.decrementAndGet();
            throw new IllegalStateException("This coordinator's term of " + jobsPerTerm
                    + " jobs is over - a new election is starting. Please retry in a few seconds.");
        }
    }

    /** Called by the JobManager whenever a job completes. When the term's last job is done, hand over. */
    public void jobFinished() {
        if (jobsPerTerm <= 0 || !isCoordinator() || termJobs.get() < jobsPerTerm
                || node.jobManager().hasUnfinishedJobs()
                || !termEnding.compareAndSet(false, true)) {
            return;
        }
        Log.info(node.name(), "Term complete (" + jobsPerTerm + " jobs) - starting new election");
        termElection.execute(() -> {
            replicate();        // every worker holds the final state before the hand-over
            startElection();
        });
    }

    // =====================================================================
    // Failure detection - runs on a timer in WorkerNode
    // =====================================================================

    /**
     * One watchdog round. Learns who the coordinator is (Bootstrap record, or the last one this
     * worker knew if Bootstrap is down), pings it, and starts an election if it has failed
     * or if there is no coordinator at all.
     */
    public void watchdog() {
        if (node.self() == null || electing.get()) {
            return;
        }
        try {
            try {
                CoordinatorReply reply = node.bootstrapStub().getCoordinator(Empty.getDefaultInstance());
                if (reply.getFound()) {
                    adopt(reply.getCoordinator(), reply.getTerm());
                }
            } catch (StatusRuntimeException e) {
                // Bootstrap unreachable: carry on with what this worker already knows
            }

            WorkerInfo c = coordinator;
            if (isCoordinator()) {
                missedCoordinatorPings = 0;
                noCoordinatorSince = -1;
                return;
            }
            if (coordinatorId != -1 && c != null) {
                noCoordinatorSince = -1;
                checkCoordinatorAlive(c);
                return;
            }

            // No coordinator known: give workers time to join, then elect.
            long now = System.currentTimeMillis();
            if (noCoordinatorSince < 0) {
                noCoordinatorSince = now;
            }
            if (now - noCoordinatorSince >= electionDelayMs) {
                Log.info(node.name(), "No active coordinator - starting election");
                noCoordinatorSince = -1;
                electAfterJitter();
            }
        } catch (RuntimeException e) {
            Log.error(node.name(), "Watchdog error: " + e.getMessage());
        }
    }

    private void checkCoordinatorAlive(WorkerInfo c) {
        try {
            node.neighbours().stub(c).ping(Empty.getDefaultInstance());
            missedCoordinatorPings = 0;
        } catch (StatusRuntimeException e) {
            missedCoordinatorPings++;
            Log.info(node.name(), "Coordinator Worker-" + c.getId() + " missed ping "
                    + missedCoordinatorPings + "/" + missedLimit);
            if (missedCoordinatorPings >= missedLimit) {
                Log.info(node.name(), "Coordinator Worker-" + c.getId() + " FAILED - starting election");
                synchronized (this) {
                    deadCoordinatorId = c.getId();
                    coordinatorId = -1;
                    coordinator = null;
                }
                missedCoordinatorPings = 0;
                electAfterJitter();
            }
        }
    }

    /** Workers notice a failure at nearly the same time; a random pause stops them all electing at once. */
    private void electAfterJitter() {
        try {
            Thread.sleep(random.nextInt(1000));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        if (coordinatorId == -1) {   // nobody announced a winner while we waited
            startElection();
        }
    }

    // =====================================================================
    // Election (echo algorithm)
    // =====================================================================

    /** Starts an election from this worker: collect the best candidate, then announce the winner. */
    public void startElection() {
        if (node.self() == null || !electing.compareAndSet(false, true)) {
            return;
        }
        try {
            String electionId = node.self().getId() + "-" + System.currentTimeMillis();
            processedElections.add(electionId);
            Log.info(node.name(), "Starting ELECTION " + electionId);
            Candidate winner = collectBest(electionId, -1);
            Log.info(node.name(), "Election " + electionId + " winner: Worker-" + winner.getId()
                    + " (JAC " + winner.getJac() + ")");

            CoordinatorMessage announcement = CoordinatorMessage.newBuilder()
                    .setElectionId(electionId)
                    .setCoordinatorId(winner.getId())
                    .setTerm(Math.max(term.get(), bootstrapTerm()) + 1)
                    .build();
            receiveCoordinator(announcement);   // announces to ourselves and floods to the neighbours
        } finally {
            electing.set(false);
        }
    }

    /** ELECTION message from a neighbour. Replies with the best candidate in this worker's part of the network. */
    public ElectionReply receiveElection(ElectionMessage msg) {
        if (!processedElections.add(msg.getElectionId())) {
            return ElectionReply.newBuilder().setDuplicate(true).build();   // already took part
        }
        Candidate best = collectBest(msg.getElectionId(), msg.getSenderId());
        return ElectionReply.newBuilder().setDuplicate(false).setBest(best).build();
    }

    /** Forwards the election to every neighbour except the sender, keeping the best candidate (echo). */
    private Candidate collectBest(String electionId, int senderId) {
        int selfId = node.self().getId();
        Candidate best = Candidate.newBuilder().setId(selfId).setJac(jac.get()).build();
        ElectionMessage forward = ElectionMessage.newBuilder()
                .setElectionId(electionId).setSenderId(selfId).build();
        for (WorkerInfo w : node.neighbours().all()) {
            if (w.getId() == senderId) {
                continue;
            }
            try {
                ElectionReply reply = electionStub(w).election(forward);
                if (!reply.getDuplicate() && isBetter(reply.getBest(), best)) {
                    best = reply.getBest();
                }
            } catch (StatusRuntimeException e) {
                Log.info(node.name(), "Election: Worker-" + w.getId() + " unreachable (" + e.getStatus().getCode() + ")");
            }
        }
        return best;
    }

    /** Lowest JAC wins; on a tie the highest ID wins. */
    private static boolean isBetter(Candidate a, Candidate b) {
        return a.getJac() < b.getJac() || (a.getJac() == b.getJac() && a.getId() > b.getId());
    }

    // =====================================================================
    // Coordinator announcement
    // =====================================================================

    /** COORDINATOR message: the winner has been decided. Accepts it if its term is newer, then floods it on. */
    public void receiveCoordinator(CoordinatorMessage msg) {
        if (!processedCoordinators.add(msg.getElectionId())) {
            return;   // duplicate - already handled
        }
        synchronized (this) {
            boolean newer = msg.getTerm() > term.get()
                    || (msg.getTerm() == term.get() && coordinatorId == -1);
            if (!newer) {
                Log.info(node.name(), "Ignored stale COORDINATOR (term " + msg.getTerm()
                        + ", current term " + term.get() + ")");
                return;
            }
            boolean wasCoordinator = isCoordinator();
            term.set(msg.getTerm());
            coordinatorId = msg.getCoordinatorId();
            coordinator = null;   // address is looked up below / by the watchdog
            deadCoordinatorId = -1;
            missedCoordinatorPings = 0;
            if (wasCoordinator && coordinatorId != node.self().getId()) {
                stepDown();
            }
        }

        flooder.execute(() -> {
            for (WorkerInfo w : node.neighbours().all()) {
                try {
                    electionStub(w).coordinator(msg);
                } catch (StatusRuntimeException ignored) {
                    // neighbour unreachable - the watchdog will catch anyone who misses this
                }
            }
        });

        if (msg.getCoordinatorId() == node.self().getId()) {
            becomeCoordinator(msg.getTerm());
        } else {
            Log.info(node.name(), "Coordinator is Worker-" + msg.getCoordinatorId() + " (term " + msg.getTerm() + ")");
            resolveCoordinatorAddress(msg.getCoordinatorId());
        }
    }

    /** This worker won: record it at Bootstrap, recover the replicated state, start replicating. */
    private void becomeCoordinator(long newTerm) {
        synchronized (this) {
            coordinator = node.self();
        }
        termJobs.set(0);          // a new term starts with a fresh job allowance
        termEnding.set(false);
        Log.info(node.name(), "I am the COORDINATOR (term " + newTerm + ", JAC " + jac.get() + ")");
        try {
            Ack ack = node.bootstrapStub().setCoordinator(CoordinatorUpdate.newBuilder()
                    .setCoordinatorId(node.self().getId()).setTerm(newTerm).build());
            if (!ack.getOk()) {
                Log.error(node.name(), "Bootstrap refused the coordinator record: " + ack.getMessage());
            }
        } catch (StatusRuntimeException e) {
            Log.error(node.name(), "Could not tell Bootstrap (" + e.getStatus().getCode()
                    + ") - workers will still learn it from the announcement");
        }

        ClusterState saved = latestState;
        version.set(Math.max(version.get(), saved.getVersion()));
        if (saved.getJobsCount() > 0) {
            Log.info(node.name(), "Restoring " + saved.getJobsCount() + " job(s) from replicated state (version "
                    + saved.getVersion() + ")");
        }
        node.jobManager().restore(saved);
        node.jobManager().setOnChange(this::replicateAsync);
        replicateAsync();
    }

    /** A newer coordinator exists: stop copying state out. */
    private void stepDown() {
        node.jobManager().setOnChange(() -> { });
        Log.info(node.name(), "Stepping down - a newer coordinator was elected");
    }

    // =====================================================================
    // State replication
    // =====================================================================

    /** Called by the JobManager after every change. Merges bursts of changes into one copy. */
    private void replicateAsync() {
        if (!isCoordinator() || !replicatePending.compareAndSet(false, true)) {
            return;
        }
        replicator.execute(() -> {
            replicatePending.set(false);
            replicate();
        });
    }

    private void replicate() {
        try {
            ClusterState state = node.jobManager().snapshot(term.get(), version.incrementAndGet());
            latestState = state;
            for (WorkerInfo w : node.bootstrapStub().getActiveWorkers(Empty.getDefaultInstance()).getWorkersList()) {
                if (w.getId() == node.self().getId()) {
                    continue;
                }
                try {
                    node.neighbours().stub(w).replicateState(state);
                } catch (StatusRuntimeException e) {
                    // that worker is down; it will be removed by the health checks
                }
            }
        } catch (RuntimeException e) {
            Log.error(node.name(), "Replication failed: " + e.getMessage());
        }
    }

    /** ReplicateState from the coordinator: keep it only if it is newer than what we already hold. */
    public synchronized void receiveState(ClusterState state) {
        ClusterState current = latestState;
        boolean newer = state.getTerm() > current.getTerm()
                || (state.getTerm() == current.getTerm() && state.getVersion() > current.getVersion());
        if (newer) {
            latestState = state;
        }
    }

    // =====================================================================
    // Helpers
    // =====================================================================

    /** Adopts the coordinator recorded at Bootstrap, unless it is stale or the one we declared dead. */
    private synchronized void adopt(WorkerInfo c, long t) {
        if (c.getId() == deadCoordinatorId && t <= term.get()) {
            return;   // Bootstrap has not noticed the failure yet
        }
        if (t > term.get() || (t == term.get() && (coordinatorId == -1 || coordinatorId == c.getId()))) {
            boolean changed = coordinatorId != c.getId() || t != term.get();
            term.set(t);
            coordinatorId = c.getId();
            coordinator = c;
            if (changed) {
                Log.info(node.name(), "Coordinator is Worker-" + c.getId() + " (term " + t + ")");
            }
        }
    }

    private void resolveCoordinatorAddress(int id) {
        try {
            for (WorkerInfo w : node.bootstrapStub().getActiveWorkers(Empty.getDefaultInstance()).getWorkersList()) {
                if (w.getId() == id) {
                    synchronized (this) {
                        if (coordinatorId == id) {
                            coordinator = w;
                        }
                    }
                    return;
                }
            }
        } catch (StatusRuntimeException e) {
            // the watchdog will resolve it from the Bootstrap record later
        }
    }

    private long bootstrapTerm() {
        try {
            return node.bootstrapStub().getCoordinator(Empty.getDefaultInstance()).getTerm();
        } catch (StatusRuntimeException e) {
            return 0;
        }
    }

    private ElectionServiceGrpc.ElectionServiceBlockingStub electionStub(WorkerInfo w) {
        return ElectionServiceGrpc.newBlockingStub(node.channels().get(w.getHost(), w.getPort()))
                .withDeadlineAfter(electionTimeoutMs, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        replicator.shutdownNow();
        flooder.shutdownNow();
        termElection.shutdownNow();
    }

    private static java.util.concurrent.ThreadFactory daemon(String name) {
        return r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        };
    }
}