package common;

import java.io.Serializable;

/** A worker's claim in an election: its ID and its Job Allocation Counter. */
public class ElectionCandidate implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int id;
    private final int jac;

    public ElectionCandidate(int id, int jac) {
        this.id = id;
        this.jac = jac;
    }

    public int getId() { return id; }
    public int getJac() { return jac; }

    /** Lowest JAC wins; on a tie, the highest ID wins. */
    public boolean isBetterThan(ElectionCandidate other) {
        return jac < other.jac || (jac == other.jac && id > other.id);
    }
}