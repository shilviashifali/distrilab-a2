package jobs;

import java.util.Collections;
import java.util.List;

/** MAX(numbers): the largest value in an unsorted list. */
public class MaxJob extends NumberListJob<Integer> {
    private static final long serialVersionUID = 1L;

    public MaxJob(List<Integer> numbers) {
        super(numbers);
    }

    @Override
    public String getName() {
        return "MAX";
    }

    @Override
    protected NumberListJob<Integer> createPart(List<Integer> part) {
        return new MaxJob(part);
    }

    @Override
    public Integer execute() {
        return Collections.max(getNumbers());
    }

    @Override
    public Integer combine(List<Integer> partialResults) {
        return Collections.max(partialResults);
    }
}