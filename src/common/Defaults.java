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

    private Defaults() {}
}
