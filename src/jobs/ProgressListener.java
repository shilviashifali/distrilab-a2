package jobs;

/** Receives real progress while a task is being computed. */
@FunctionalInterface
public interface ProgressListener {

    /**
     * Called every ~1% of the work.
     * @return false to stop the computation (for example, the coordinator cancelled the task)
     */
    boolean onProgress(long itemsDone, long itemsTotal);
}
