package jobs;

import grpc.JobType;
import java.util.List;

/** MAX(numbers): the largest value in an unsorted list. */
public class MaxJob extends NumberListJob {

    @Override
    public JobType type() {
        return JobType.MAX;
    }

    @Override
    protected long initial() {
        return Long.MIN_VALUE;
    }

    @Override
    protected long accumulate(long result, long number) {
        return Math.max(result, number);
    }

    @Override
    public long combine(List<Long> partialResults) {
        long max = Long.MIN_VALUE;
        for (long r : partialResults) {
            max = Math.max(max, r);
        }
        return max;
    }
}
