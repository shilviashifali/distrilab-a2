package worker;

import common.Config;
import common.Job;
import common.JobResult;
import common.Log;
import common.WorkerService;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Coordinator-side job handling: splits a job across all active workers,
 * sends the parts in parallel, and combines the partial results.
 * Works for every job type through the polymorphic Job interface.
 */
public class JobCoordinator {

    private final WorkerNode node;
    private final ElectionManager election;
    private final ExecutorService dispatchPool = Executors.newFixedThreadPool(Config.DISPATCH_THREADS);

    public JobCoordinator(WorkerNode node, ElectionManager election) {
        this.node = node;
        this.election = election;
    }

    public JobResult submit(Job<?> job) throws RemoteException {
        return process(job);
    }

    private <R extends Serializable> JobResult process(Job<R> job) throws RemoteException {
        int jobNo = election.startTermJob(); // throws if not coordinator or term is over
        Log.info(node.displayName(), "Job " + jobNo + "/" + Config.JOBS_PER_TERM
                + " this term: " + job.describe());
        try {
            List<Map.Entry<Integer, WorkerService>> workers =
                    new ArrayList<>(node.getBootstrap().getActiveWorkers().entrySet());
            if (workers.isEmpty()) {
                throw new RemoteException("No active workers available");
            }

            List<Job<R>> parts = job.split(workers.size());
            List<Future<R>> futures = new ArrayList<>();
            for (int i = 0; i < parts.size(); i++) {
                int workerId = workers.get(i).getKey();
                WorkerService worker = workers.get(i).getValue();
                Job<R> part = parts.get(i);
                election.recordAssignment(workerId);
                Log.info(node.displayName(), "Assigned " + part.describe() + " to Worker-" + workerId);
                futures.add(dispatchPool.submit(() -> worker.executeTask(part)));
            }

            List<R> partialResults = new ArrayList<>();
            for (Future<R> future : futures) {
                partialResults.add(future.get());
            }
            R result = job.combine(partialResults);
            Log.info(node.displayName(), "Completed " + job.describe() + " = " + result
                    + " (JAC now " + election.getJac() + ")");
            return new JobResult(job.getName(), result, parts.size(), node.getId());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RemoteException("Job interrupted: " + job.describe(), e);
        } catch (ExecutionException e) {
            throw new RemoteException("Job failed: " + e.getCause().getMessage(), e.getCause());
        } finally {
            election.finishTermJob(jobNo);
        }
    }

    public void shutdown() {
        dispatchPool.shutdownNow();
    }
}