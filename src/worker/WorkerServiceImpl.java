package worker;

import grpc.Ack;
import grpc.ClusterState;
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
 */
public class WorkerServiceImpl extends WorkerServiceGrpc.WorkerServiceImplBase {

    private final NeighbourManager neighbours;
    private final TaskWorker taskWorker;
    private final ElectionManager election;

    public WorkerServiceImpl(NeighbourManager neighbours, TaskWorker taskWorker, ElectionManager election) {
        this.neighbours = neighbours;
        this.taskWorker = taskWorker;
        this.election = election;
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

    // ---------------- Replication ----------------

    /** The coordinator copies its job state here after every change, so any worker can take over. */
    @Override
    public void replicateState(ClusterState req, StreamObserver<Ack> out) {
        election.receiveState(req);
        out.onNext(Ack.newBuilder().setOk(true).build());
        out.onCompleted();
    }
}