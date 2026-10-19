package common;

import java.io.Serializable;

public class WorkerInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    private final int id;
    private final WorkerService stub;

    public WorkerInfo(int id, WorkerService stub) {
        this.id = id;
        this.stub = stub;
    }

    public int getId() { return id; }
    public WorkerService getStub() { return stub; }
}