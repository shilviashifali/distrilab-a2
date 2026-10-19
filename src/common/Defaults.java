package common;

/** Default values used when a setting is not given on the command line or in distrilab.properties. */
public final class Defaults {
    public static final String BOOTSTRAP_HOST = "localhost";
    public static final int BOOTSTRAP_PORT = 50050;
    public static final int WORKER_PORT = 0;            // 0 = pick any free port

    public static final int NEIGHBOURS = 2;              // neighbours a joining worker connects to
    public static final int PING_INTERVAL_SECONDS = 2;   // liveness checks
    public static final int MISSED_PINGS_FOR_FAILURE = 3;
    public static final int RPC_TIMEOUT_MS = 2000;       // deadline for short calls
    public static final int ELECTION_DELAY_SECONDS = 8;  // with no coordinator this long, a worker starts an election
    public static final int JOBS_PER_TERM = 5;           // jobs one coordinator accepts before a new election (0 = never rotate)
        
    public static final int WORKER_THREADS = 4;          // tasks a worker can compute at the same time
    public static final int TASKS_PER_WORKER = 3;        // a job is split into (active workers x this) tasks
    public static final int SLOW_AFTER_SECONDS = 6;      // no progress for this long -> check if slow or failed

    private Defaults() {}
}
