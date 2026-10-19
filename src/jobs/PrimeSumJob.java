package jobs;

import common.Job;
import java.util.ArrayList;
import java.util.List;

/** PRIMESUM(start, end): the sum of all primes in the range, inclusive. */
public class PrimeSumJob extends Job<Long> {
    private static final long serialVersionUID = 1L;

    private final int start;
    private final int end;

    public PrimeSumJob(int start, int end) {
        if (start > end) {
            throw new IllegalArgumentException("Start must be less than or equal to End.");
        }
        this.start = start;
        this.end = end;
    }

    @Override
    public String getName() {
        return "PRIMESUM";
    }

    @Override
    public List<Job<Long>> split(int parts) {
        if (parts < 1) {
            throw new IllegalArgumentException("Number of parts must be at least 1");
        }
        List<Job<Long>> result = new ArrayList<>();
        long total = (long) end - start + 1;
        long base = total / parts;
        long remainder = total % parts;
        long current = start;
        for (int i = 0; i < parts; i++) {
            long size = base + (i < remainder ? 1 : 0);
            if (size == 0) {
                break; // more workers than numbers in the range
            }
            long partEnd = current + size - 1;
            result.add(new PrimeSumJob((int) current, (int) partEnd));
            current = partEnd + 1;
        }
        return result;
    }

    @Override
    public Long execute() {
        long sum = 0;
        for (long i = Math.max(2L, start); i <= end; i++) {
            if (PrimeUtils.isPrime(i)) {
                sum += i;
            }
        }
        return sum;
    }

    @Override
    public Long combine(List<Long> partialResults) {
        long total = 0;
        for (long s : partialResults) {
            total += s;
        }
        return total;
    }

    @Override
    public String describe() {
        return "PRIMESUM(" + start + ", " + end + ")";
    }
}