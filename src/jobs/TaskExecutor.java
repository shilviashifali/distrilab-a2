package jobs;

import common.Job;
import common.Log;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** Runs job parts on a worker's thread pool, so a worker can process parts of several jobs at the same time. */
public class TaskExecutor {

    private final ExecutorService pool;

    public TaskExecutor(int threads) {
        this.pool = Executors.newFixedThreadPool(threads);
    }

    public <R extends Serializable> R run(Job<R> task) throws RemoteException {
        Future<R> future = pool.submit(() -> {
            String thread = Thread.currentThread().getName();
            Log.info(thread, "Started " + task.describe());
            R result = task.execute();
            Log.info(thread, "Finished " + task.describe() + " -> " + result);
            return result;
        });
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RemoteException("Task interrupted: " + task.describe(), e);
        } catch (ExecutionException e) {
            throw new RemoteException("Task failed: " + task.describe()
                    + " - " + e.getCause().getMessage(), e.getCause());
        }
    }

    public void shutdown() {
        pool.shutdownNow();
    }
}