package jobs;

import grpc.JobRequest;
import grpc.JobType;
import grpc.Task;
import java.util.List;

/**
 * One job type (MAX, PRIMESUM, PRIMECOUNT). Each subclass knows how to split a
 * request into tasks, compute one task while reporting progress, and combine
 * the partial results. Subclasses hold no state, so one instance per type is shared.
 */
public abstract class Job {

    private static final Job MAX = new MaxJob();
    private static final Job PRIMESUM = new PrimeSumJob();
    private static final Job PRIMECOUNT = new PrimeCountJob();

    /** The Job implementation for a job type. */
    public static Job of(JobType type) {
        return switch (type) {
            case MAX -> MAX;
            case PRIMESUM -> PRIMESUM;
            case PRIMECOUNT -> PRIMECOUNT;
            default -> throw new IllegalArgumentException("Unknown job type: " + type);
        };
    }

    public abstract JobType type();

    /** Throws IllegalArgumentException with a user-friendly message if the request is invalid. */
    public abstract void validate(JobRequest request);

    /** Splits the request into at most {@code parts} tasks with IDs jobId-1, jobId-2, ... */
    public abstract List<Task> split(String jobId, JobRequest request, int parts);

    /** Number of items in a task - the unit progress is measured in. */
    public abstract long size(Task task);

    /** Computes one task. Throws CancellationException if the listener asks to stop. */
    public abstract long execute(Task task, ProgressListener progress);

    /** Combines the partial results of all tasks into the job result. */
    public abstract long combine(List<Long> partialResults);

    public abstract String describe(JobRequest request);

    protected Task.Builder newTask(String jobId, int partNo) {
        return Task.newBuilder().setTaskId(jobId + "-" + partNo).setJobId(jobId).setType(type());
    }

    /** How often (in items) to report progress: about every 1%. */
    protected static long reportEvery(long total) {
        return Math.max(1, total / 100);
    }
}
