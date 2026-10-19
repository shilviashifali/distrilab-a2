package common;

import java.io.Serializable;

/** The final result of a job, returned from the coordinator to the client. */
public class JobResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String jobName;
    private final Serializable value;
    private final int workersUsed;
    private final int coordinatorId;

    public JobResult(String jobName, Serializable value, int workersUsed, int coordinatorId) {
        this.jobName = jobName;
        this.value = value;
        this.workersUsed = workersUsed;
        this.coordinatorId = coordinatorId;
    }

    public String getJobName() { return jobName; }
    public Serializable getValue() { return value; }
    public int getWorkersUsed() { return workersUsed; }
    public int getCoordinatorId() { return coordinatorId; }

    @Override
    public String toString() {
        return jobName + " result: " + value
                + "  (coordinator Worker-" + coordinatorId + ", " + workersUsed + " workers)";
    }
}