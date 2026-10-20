
package worker;

import common.Log;
import grpc.Task;
import grpc.TaskProgress;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import jobs.Job;

/**
 * Worker side of a task: computes it on this worker's thread pool
 * and streams real progress back to the coordinator over gRPC.
 * Several tasks can run at the same time.
 */
public class TaskWorker {

    /**
     * Report progress at most once every 20 ms per task.
     * This allows more frequent updates to the client GUI.
     */
    private static final long MIN_REPORT_GAP_MS = 20;

    private final WorkerNode node;
    private final ExecutorService pool;

    public TaskWorker(WorkerNode node, int threads) {
        this.node = node;
        this.pool = Executors.newFixedThreadPool(threads);
    }

    /**
     * Called by gRPC for each ExecuteTask request.
     * Work runs asynchronously on the thread pool.
     */
    public void execute(Task task, StreamObserver<TaskProgress> out) {

        ServerCallStreamObserver<TaskProgress> call =
                (ServerCallStreamObserver<TaskProgress>) out;

        call.setOnCancelHandler(() ->
                Log.info(node.name(),
                        "Coordinator cancelled task "
                        + task.getTaskId()));

        pool.submit(() -> run(task, call));
    }

    private void run(
            Task task,
            ServerCallStreamObserver<TaskProgress> call) {

        int me = node.self().getId();

        Job job = Job.of(task.getType());

        long total = job.size(task);

        long[] lastSent = {0};

        Log.info(node.name(),
                "Started task " + task.getTaskId()
                + " (" + total + " items)");

        try {

            long result = job.execute(task, (done, all) -> {

                if (call.isCancelled()) {
                    return false;
                }

                long now = System.currentTimeMillis();

                if (now - lastSent[0] >= MIN_REPORT_GAP_MS) {

                    lastSent[0] = now;

                    call.onNext(
                            progress(task, me, done, all)
                                    .build());
                }

                return true;
            });

            if (call.isCancelled()) {

                Log.info(node.name(),
                        "Task " + task.getTaskId()
                        + " cancelled before result delivery");

                return;
            }

            call.onNext(
                    progress(task, me, total, total)
                            .setFinished(true)
                            .setResult(result)
                            .build());

            call.onCompleted();

            Log.info(node.name(),
                    "Finished task " + task.getTaskId()
                    + " -> " + result);

        } catch (CancellationException e) {

            Log.info(node.name(),
                    "Stopped task " + task.getTaskId()
                    + " (cancelled)");

        } catch (RuntimeException e) {

            Log.error(node.name(),
                    "Task " + task.getTaskId()
                    + " failed: " + e.getMessage());

            if (!call.isCancelled()) {

                call.onError(
                        Status.INTERNAL
                                .withDescription(e.getMessage())
                                .asRuntimeException());
            }
        }
    }

    private static TaskProgress.Builder progress(
            Task task,
            int workerId,
            long done,
            long total) {

        return TaskProgress.newBuilder()
                .setTaskId(task.getTaskId())
                .setWorkerId(workerId)
                .setItemsDone(done)
                .setItemsTotal(total);
    }

    public void shutdown() {
        pool.shutdownNow();
    }
}
