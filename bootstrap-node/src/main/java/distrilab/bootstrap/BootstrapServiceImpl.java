package distrilab.bootstrap;

import distrilab.common.Log;
import distrilab.grpc.Ack;
import distrilab.grpc.BootstrapServiceGrpc;
import distrilab.grpc.CoordinatorReply;
import distrilab.grpc.CoordinatorUpdate;
import distrilab.grpc.Empty;
import distrilab.grpc.RandomWorkersRequest;
import distrilab.grpc.RegisterRequest;
import distrilab.grpc.WorkerId;
import distrilab.grpc.WorkerInfo;
import distrilab.grpc.WorkerList;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/** gRPC endpoint of the Bootstrap Node. Validates requests and delegates to WorkerRegistry. */
public class BootstrapServiceImpl extends BootstrapServiceGrpc.BootstrapServiceImplBase {

    private static final String NAME = "Bootstrap";
    private final WorkerRegistry registry;

    public BootstrapServiceImpl(WorkerRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void register(RegisterRequest req, StreamObserver<WorkerInfo> out) {
        if (req.getHost().isBlank() || req.getPort() <= 0) {
            out.onError(Status.INVALID_ARGUMENT
                    .withDescription("Worker must send its host and a valid port").asRuntimeException());
            return;
        }
        WorkerInfo info = registry.register(req.getHost(), req.getPort());
        Log.info(NAME, "Worker-" + info.getId() + " registered at " + req.getHost() + ":" + req.getPort()
                + ". Active: " + ids());
        out.onNext(info);
        out.onCompleted();
    }

    @Override
    public void unregister(WorkerId req, StreamObserver<Ack> out) {
        boolean wasCoordinator = registry.remove(req.getId());
        Log.info(NAME, "Worker-" + req.getId() + " left. Active: " + ids());
        if (wasCoordinator) {
            Log.info(NAME, "Coordinator left - waiting for a new election");
        }
        out.onNext(Ack.newBuilder().setOk(true).build());
        out.onCompleted();
    }

    @Override
    public void getRandomActiveWorkers(RandomWorkersRequest req, StreamObserver<WorkerList> out) {
        int count = Math.max(1, req.getCount());
        out.onNext(WorkerList.newBuilder().addAllWorkers(registry.random(req.getExcludeId(), count)).build());
        out.onCompleted();
    }

    @Override
    public void getActiveWorkers(Empty req, StreamObserver<WorkerList> out) {
        out.onNext(WorkerList.newBuilder().addAllWorkers(registry.active()).build());
        out.onCompleted();
    }

    @Override
    public void setCoordinator(CoordinatorUpdate req, StreamObserver<Ack> out) {
        boolean accepted = registry.setCoordinator(req.getCoordinatorId(), req.getTerm());
        if (accepted) {
            Log.info(NAME, "Coordinator is now Worker-" + req.getCoordinatorId() + " (term " + req.getTerm() + ")");
        } else {
            Log.info(NAME, "Rejected stale coordinator Worker-" + req.getCoordinatorId()
                    + " (term " + req.getTerm() + ", current term " + registry.term() + ")");
        }
        out.onNext(Ack.newBuilder().setOk(accepted)
                .setMessage(accepted ? "" : "stale term, current is " + registry.term()).build());
        out.onCompleted();
    }

    @Override
    public void getCoordinator(Empty req, StreamObserver<CoordinatorReply> out) {
        CoordinatorReply.Builder reply = CoordinatorReply.newBuilder().setTerm(registry.term());
        registry.coordinator().ifPresent(c -> reply.setFound(true).setCoordinator(c));
        out.onNext(reply.build());
        out.onCompleted();
    }

    private String ids() {
        return registry.active().stream().map(w -> w.getId()).sorted().toList().toString();
    }
}
