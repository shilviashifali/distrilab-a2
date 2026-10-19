package common;

import java.io.Serializable;
import java.util.List;

/**
 * A distributed computation. Each job type knows how to split itself into
 * smaller parts, compute a part, and combine the partial results.
 * @param <R> type of the result
 */
public abstract class Job<R extends Serializable> implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Short name of the job type, e.g. "MAX". */
    public abstract String getName();

    /** Splits this job into at most {@code parts} smaller jobs of the same type. */
    public abstract List<Job<R>> split(int parts);

    /** Computes the result of this job. */
    public abstract R execute();

    /** Combines the results of the parts into the final result. */
    public abstract R combine(List<R> partialResults);

    /** Human-readable description for logs, e.g. "MAX of 6 numbers". */
    public abstract String describe();

    @Override
    public String toString() {
        return describe();
    }
}