package common;

public final class Config {
    public static final String DEFAULT_BOOTSTRAP_HOST = "localhost";
    public static final int DEFAULT_BOOTSTRAP_PORT = 1099;
    public static final String BOOTSTRAP_NAME = "Bootstrap";

    public static final int HEARTBEAT_SECONDS = 5;
    public static final String RMI_TIMEOUT_MS = "5000";

    public static final int JOBS_PER_TERM = 5;
    public static final int ELECTION_START_DELAY_SECONDS = 10;

    public static final int WORKER_THREADS = 4;
    public static final int DISPATCH_THREADS = 10;

    private Config() {}
}