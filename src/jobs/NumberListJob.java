package jobs;

import grpc.JobRequest;
import grpc.Task;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;

/** Base class for jobs over a list of numbers (MAX, PRIMECOUNT): splits the list into near-equal parts. */
public abstract class NumberListJob extends Job {

    /** Starting value of the running result. */
    protected abstract long initial();

    /** Adds one number to the running result. */
    protected abstract long accumulate(long result, long number);

    @Override
    public void validate(JobRequest request) {
        if (request.getNumbersCount() == 0) {
            throw new IllegalArgumentException("Enter at least one number.");
        }
    }

    @Override
    public List<Task> split(String jobId, JobRequest request, int parts) {
        List<Long> numbers = request.getNumbersList();
        int n = numbers.size();
        int count = Math.max(1, Math.min(parts, n));
        int base = n / count;
        int remainder = n % count;
        List<Task> tasks = new ArrayList<>();
        int index = 0;
        for (int i = 0; i < count; i++) {
            int size = base + (i < remainder ? 1 : 0);
            tasks.add(newTask(jobId, i + 1).addAllNumbers(numbers.subList(index, index + size)).build());
            index += size;
        }
        return tasks;
    }

    @Override
    public long size(Task task) {
        return task.getNumbersCount();
    }

    @Override
    public long execute(Task task, ProgressListener progress) {
        int total = task.getNumbersCount();
        long every = reportEvery(total);
        long result = initial();
        for (int i = 0; i < total; i++) {
            result = accumulate(result, task.getNumbers(i));
            if ((i + 1) % every == 0 && !progress.onProgress(i + 1, total)) {
                throw new CancellationException("Task " + task.getTaskId() + " cancelled");
            }
        }
        return result;
    }

    @Override
    public String describe(JobRequest request) {
        return type() + " of " + request.getNumbersCount() + " numbers";
    }
}
