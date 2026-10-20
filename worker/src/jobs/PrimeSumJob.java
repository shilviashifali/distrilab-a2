package jobs;

import grpc.JobRequest;
import grpc.JobType;
import grpc.Task;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;

/** PRIMESUM(start, end): the sum of all primes in the range, inclusive. Splits the range into near-equal parts. */
public class PrimeSumJob extends Job {

    @Override
    public JobType type() {
        return JobType.PRIMESUM;
    }

    @Override
    public void validate(JobRequest request) {
        if (request.getStart() > request.getEnd()) {
            throw new IllegalArgumentException("Start must be less than or equal to End.");
        }
    }

    @Override
    public List<Task> split(String jobId, JobRequest request, int parts) {
        long start = request.getStart();
        long total = request.getEnd() - start + 1;
        long count = Math.max(1, Math.min(parts, total));
        long base = total / count;
        long remainder = total % count;
        List<Task> tasks = new ArrayList<>();
        long current = start;
        for (int i = 0; i < count; i++) {
            long size = base + (i < remainder ? 1 : 0);
            long partEnd = current + size - 1;
            tasks.add(newTask(jobId, i + 1).setStart(current).setEnd(partEnd).build());
            current = partEnd + 1;
        }
        return tasks;
    }

    @Override
    public long size(Task task) {
        return task.getEnd() - task.getStart() + 1;
    }

    @Override
    public long execute(Task task, ProgressListener progress) {
        long total = size(task);
        long every = reportEvery(total);
        long sum = 0;
        long done = 0;
        for (long n = task.getStart(); n <= task.getEnd(); n++) {
            if (PrimeUtils.isPrime(n)) {
                sum += n;
            }
            done++;
            if (done % every == 0 && !progress.onProgress(done, total)) {
                throw new CancellationException("Task " + task.getTaskId() + " cancelled");
            }
        }
        return sum;
    }

    @Override
    public long combine(List<Long> partialResults) {
        long total = 0;
        for (long r : partialResults) {
            total += r;
        }
        return total;
    }

    @Override
    public String describe(JobRequest request) {
        return "PRIMESUM(" + request.getStart() + ", " + request.getEnd() + ")";
    }
}
