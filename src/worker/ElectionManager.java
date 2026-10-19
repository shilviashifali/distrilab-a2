package worker;

import common.Config;
import common.ElectionCandidate;
import common.Log;
import common.WorkerInfo;
import common.WorkerService;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Leader election for one worker, using an echo algorithm over the
 * unstructured network: ELECTION spreads from neighbour to neighbour, and
 * each worker returns the best candidate found in its part of the network.
 * Also tracks the Job Allocation Counter (JAC) and the coordinator's term.
 */
public class ElectionManager {

    private final WorkerNode node;
    private final AtomicInteger jac = new AtomicInteger(0);
    private final AtomicInteger termJobs = new AtomicInteger(0);
    private final AtomicBoolean electionRunning = new AtomicBoolean(false);
    private final Set<String> processedElections = ConcurrentHashMap.newKeySet();
    private final Set<String> processedCoordinators = ConcurrentHashMap.newKeySet();
    private final ExecutorService electionThread = Executors.newSingleThreadExecutor();
    private volatile int coordinatorId = -1;

    public ElectionManager(WorkerNode node) {
        this.node = node;
    }

    // ---------------------------------------------------------------------
    // State
    // ---------------------------------------------------------------------

    public int getJac() {
        return jac.get();
    }

    public int getCoordinatorId() {
        return coordinatorId;
    }

    public boolean isCoordinator() {
        return node.getId() != -1 && coordinatorId == node.getId();
    }

    // ---------------------------------------------------------------------
    // ELECTION
    // ---------------------------------------------------------------------

    /** Starts an election from this worker. */
    public void startElection() {
        if (!electionRunning.compareAndSet(false, true)) {
            Log.info(name(), "Election already running");
            return;
        }
        try {
            String electionId = node.getId() + "-" + System.currentTimeMillis();
            processedElections.add(electionId);
            Log.info(name(), "Starting ELECTION " + electionId);

            ElectionCandidate winner = collectBest(electionId, -1);
            Log.info(name(), "Election " + electionId + " winner: Worker-" + winner.getId()
                    + " (JAC " + winner.getJac() + ")");

            try {
                node.getBootstrap().setCoordinator(winner.getId()); // bootstrap only records it for clients
            } catch (RemoteException e) {
                Log.error(name(), "Could not record coordinator with bootstrap: " + e.getMessage());
            }
            receiveCoordinator(electionId, winner.getId()); // apply locally and propagate
        } finally {
            electionRunning.set(false);
        }
    }

    /**
     * Handles an ELECTION message. Returns the best candidate in this part of
     * the network, or null if this election was already processed.
     */
    public ElectionCandidate receiveElection(String electionId, int senderId) {
        if (!processedElections.add(electionId)) {
            Log.info(name(), "Duplicate ELECTION " + electionId + " from Worker-" + senderId + " ignored");
            return null;
        }
        Log.info(name(), "ELECTION " + electionId + " received from Worker-" + senderId);
        return collectBest(electionId, senderId);
    }

    /** Asks every neighbour except the sender, and keeps the best candidate. */
    private ElectionCandidate collectBest(String electionId, int senderId) {
        ElectionCandidate best = new ElectionCandidate(node.getId(), jac.get());
        for (Map.Entry<Integer, WorkerService> neighbour : node.getNeighbours().entrySet()) {
            if (neighbour.getKey() == senderId) {
                continue;
            }
            try {
                ElectionCandidate candidate = neighbour.getValue().receiveElection(electionId, node.getId());
                if (candidate != null && candidate.isBetterThan(best)) {
                    best = candidate;
                }
            } catch (RemoteException e) {
                Log.error(name(), "Worker-" + neighbour.getKey() + " unreachable during election");
            }
        }
        return best;
    }

    // ---------------------------------------------------------------------
    // COORDINATOR
    // ---------------------------------------------------------------------

    /** Applies the election result once and forwards it to all neighbours. */
    public void receiveCoordinator(String electionId, int newCoordinatorId) {
        if (!processedCoordinators.add(electionId)) {
            return; // already received and forwarded
        }
        coordinatorId = newCoordinatorId;
        if (isCoordinator()) {
            termJobs.set(0);
            Log.info(name(), "I am the COORDINATOR for this term (JAC " + jac.get() + ")");
        } else {
            Log.info(name(), "COORDINATOR is Worker-" + newCoordinatorId);
        }
        for (Map.Entry<Integer, WorkerService> neighbour : node.getNeighbours().entrySet()) {
            try {
                neighbour.getValue().receiveCoordinator(electionId, newCoordinatorId);
            } catch (RemoteException e) {
                Log.error(name(), "Could not forward COORDINATOR to Worker-" + neighbour.getKey());
            }
        }
    }

    /** Runs periodically: starts an election when there is no active coordinator. */
    public void checkCoordinator() {
        try {
            WorkerInfo current = node.getBootstrap().getCoordinator();
            if (current != null) {
                if (coordinatorId == -1) {
                    coordinatorId = current.getId(); // this worker joined after the election
                }
                return;
            }
            if (electionRunning.get()) {
                return;
            }
            Thread.sleep(ThreadLocalRandom.current().nextInt(3000)); // stagger workers
            if (node.getBootstrap().getCoordinator() == null) {
                Log.info(name(), "No active coordinator - starting election");
                startElection();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            Log.error(name(), "Coordinator check failed: " + e.getMessage());
        }
    }

    // ---------------------------------------------------------------------
    // Term and JAC
    // ---------------------------------------------------------------------

    /** Reserves a job slot in the current term. Returns the job number (1..JOBS_PER_TERM). */
    public int startTermJob() throws RemoteException {
        if (!isCoordinator()) {
            throw new RemoteException(name() + " is not the coordinator (coordinator is Worker-"
                    + coordinatorId + ")");
        }
        int jobNo = termJobs.incrementAndGet();
        if (jobNo > Config.JOBS_PER_TERM) {
            throw new RemoteException("This coordinator's term has ended - a new election is in progress. "
                    + "Please resubmit.");
        }
        return jobNo;
    }

    /** Called when a job finishes; the last job of the term triggers a new election. */
    public void finishTermJob(int jobNo) {
        if (jobNo == Config.JOBS_PER_TERM) {
            Log.info(name(), "Term complete (" + Config.JOBS_PER_TERM + " jobs) - starting new election");
            electionThread.submit(this::startElection);
        }
    }

    /** JAC increases each time this coordinator assigns work to ANOTHER worker. */
    public void recordAssignment(int workerId) {
        if (workerId != node.getId()) {
            jac.incrementAndGet();
        }
    }

    public void shutdown() {
        electionThread.shutdownNow();
    }

    private String name() {
        return node.displayName();
    }
}