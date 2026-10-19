package distrilab.worker;

import distrilab.grpc.Ack;
import distrilab.grpc.Empty;
import distrilab.grpc.WorkerId;
import distrilab.grpc.WorkerInfo;
import distrilab.grpc.WorkerList;
import distrilab.grpc.WorkerServiceGrpc;
import io.grpc.stub.StreamObserver;

/**
 * gRPC endpoint every worker exposes.
 * Network methods (Person 1) are here. ExecuteTask (Person 3) and
 * ReplicateState (Person 2) are added as overrides in the marked section;
 * until then gRPC answers them with UNIMPLEMENTED.
 */
public class WorkerServiceImpl extends WorkerServiceGrpc.WorkerServiceImplBase {

    private final NeighbourManager neighbours;

    public WorkerServiceImpl(NeighbourManager neighbours) {
        this.neighbours = neighbours;
    }

    // ---------------- Network (Person 1) ----------------

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

    // ---------------- Jobs (Person 3): override executeTask(Task, StreamObserver<TaskProgress>) here ----------------

    // ---------------- Replication (Person 2): override replicateState(ClusterState, StreamObserver<Ack>) here ----------------
}
