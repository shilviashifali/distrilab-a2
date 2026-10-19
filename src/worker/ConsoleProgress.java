package worker;

import common.Log;
import grpc.JobProgress;
import grpc.WorkerProgress;
import io.grpc.stub.StreamObserver;

/** Prints a job's progress in the worker console every 10% - used by the "job" console command for testing. */
public class ConsoleProgress implements StreamObserver<JobProgress> {

    private final String source;
    private int lastStep = -1;

    public ConsoleProgress(String source) {
        this.source = source;
    }

    @Override
    public void onNext(JobProgress p) {
        if (p.getFinished()) {
            Log.info(source, "[Job " + p.getJobId() + "] 100% - RESULT = " + p.getResult());
            return;
        }
        int step = (int) (p.getPercent() / 10);
        if (step > lastStep) {
            lastStep = step;
            StringBuilder tasks = new StringBuilder();
            for (WorkerProgress w : p.getWorkersList()) {
                tasks.append(String.format("  %s:%s%s", w.getTaskId().substring(w.getTaskId().lastIndexOf('-') + 1),
                        w.getStatus(), w.getWorkerId() > 0 ? "@W" + w.getWorkerId() : ""));
            }
            Log.info(source, String.format("[Job %s] %.0f%%%s", p.getJobId(), p.getPercent(), tasks));
        }
    }

    @Override
    public void onError(Throwable t) {
        Log.error(source, "Job watch failed: " + t.getMessage());
    }

    @Override
    public void onCompleted() {
        // final result already printed in onNext
    }
}
