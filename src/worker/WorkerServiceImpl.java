package worker;

import grpc.Ack;
import grpc.Empty;
import grpc.Task;
import grpc.TaskProgress;
import grpc.WorkerId;
import grpc.WorkerInfo;
import grpc.WorkerList;
import grpc.WorkerServiceGrpc;
import io.grpc.stub.StreamObserver;

/**
 * gRPC endpoint every worker exposes.
 * ReplicateState (Person 2) is added as an override in the marked section;
 * until then gRPC answers it with UNIMPLEMENTED.
 */
public class WorkerServiceImpl extends WorkerServiceGrpc.WorkerServiceImplBase {

    private final NeighbourManager neighbours;
    private final TaskWorker taskWorker;

    public WorkerServiceImpl(NeighbourManager neighbours, TaskWorker taskWorker) {
        this.neighbours = neighbours;
        this.taskWorker = taskWorker;
    }

    // ---------------- Network ----------------

    @Override
    public void ping(Empty req, StreamObserver<Ack> out) {
        out.onNext(Ack.newBuilder().setOk(true).build());
        out.onCompleted();
    }

    @Override
    public void addNeighbour(WorkerInfo req, StreamObserver<Ack> out) {
        neighbours.add(req);
        out.onNext(Ack.newBuilder().setOk(true).build());
        out.onCompleted();
    }

    @Override
    public void removeNeighbour(WorkerId req, StreamObserver<Ack> out) {
        neighbours.remove(req.getId());
        out.onNext(Ack.newBuilder().setOk(true).build());
        out.onCompleted();
    }

    @Override
    public void getNeighbours(Empty req, StreamObserver<WorkerList> out) {
        out.onNext(WorkerList.newBuilder().addAllWorkers(neighbours.all()).build());
        out.onCompleted();
    }

    // ---------------- Tasks ----------------

    @Override
    public void executeTask(Task req, StreamObserver<TaskProgress> out) {
        taskWorker.execute(req, out);
    }

    // ---------------- Replication (Person 2): override replicateState(ClusterState, StreamObserver<Ack>) here ----------------
}
