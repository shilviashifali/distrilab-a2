package jobs;

import common.Job;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Base class for jobs that work on a list of numbers (MAX, PRIMECOUNT).
 * Splits the list into near-equal parts; subclasses define the computation.
 */
public abstract class NumberListJob<R extends Serializable> extends Job<R> {
    private static final long serialVersionUID = 1L;

    private final List<Integer> numbers;

    protected NumberListJob(List<Integer> numbers) {
        if (numbers == null || numbers.isEmpty()) {
            throw new IllegalArgumentException("Enter at least one number.");
        }
        this.numbers = new ArrayList<>(numbers);
    }

    protected List<Integer> getNumbers() {
        return Collections.unmodifiableList(numbers);
    }

    /** Creates a job of the same type for one part of the list. */
    protected abstract NumberListJob<R> createPart(List<Integer> part);

    @Override
    public List<Job<R>> split(int parts) {
        if (parts < 1) {
            throw new IllegalArgumentException("Number of parts must be at least 1");
        }
        List<Job<R>> result = new ArrayList<>();
        int base = numbers.size() / parts;
        int remainder = numbers.size() % parts;
        int index = 0;
        for (int i = 0; i < parts; i++) {
            int size = base + (i < remainder ? 1 : 0);
            if (size == 0) {
                break; // more workers than numbers
            }
            result.add(createPart(numbers.subList(index, index + size)));
            index += size;
        }
        return result;
    }

    @Override
    public String describe() {
        return getName() + " of " + numbers.size() + " numbers";
    }
}