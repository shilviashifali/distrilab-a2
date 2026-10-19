package jobs;

import java.util.List;

/** PRIMECOUNT(numbers): how many values in an unsorted list are prime. */
public class PrimeCountJob extends NumberListJob<Integer> {
    private static final long serialVersionUID = 1L;

    public PrimeCountJob(List<Integer> numbers) {
        super(numbers);
    }

    @Override
    public String getName() {
        return "PRIMECOUNT";
    }

    @Override
    protected NumberListJob<Integer> createPart(List<Integer> part) {
        return new PrimeCountJob(part);
    }

    @Override
    public Integer execute() {
        int count = 0;
        for (int n : getNumbers()) {
            if (PrimeUtils.isPrime(n)) {
                count++;
            }
        }
        return count;
    }

    @Override
    public Integer combine(List<Integer> partialResults) {
        int total = 0;
        for (int c : partialResults) {
            total += c;
        }
        return total;
    }
}