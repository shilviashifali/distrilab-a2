package jobs;

import grpc.JobType;
import java.util.List;

/** PRIMECOUNT(numbers): how many values in an unsorted list are prime. */
public class PrimeCountJob extends NumberListJob {

    @Override
    public JobType type() {
        return JobType.PRIMECOUNT;
    }

    @Override
    protected long initial() {
        return 0;
    }

    @Override
    protected long accumulate(long result, long number) {
        return PrimeUtils.isPrime(number) ? result + 1 : result;
    }

    @Override
    public long combine(List<Long> partialResults) {
        long total = 0;
        for (long r : partialResults) {
            total += r;
        }
        return total;
    }
}
