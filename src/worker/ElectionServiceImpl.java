package worker;

import grpc.Ack;
import grpc.CoordinatorMessage;
import grpc.ElectionMessage;
import grpc.ElectionReply;
import grpc.ElectionServiceGrpc;
import io.grpc.stub.StreamObserver;

/** gRPC endpoint for leader election. Each call is handed straight to the {@link ElectionManager}. */
public class ElectionServiceImpl extends ElectionServiceGrpc.ElectionServiceImplBase {

    private final ElectionManager election;

    public ElectionServiceImpl(ElectionManager election) {
        this.election = election;
    }

    /** ELECTION: reply with the best candidate found in this worker's part of the network. */
    @Override
    public void election(ElectionMessage req, StreamObserver<ElectionReply> out) {
        out.onNext(election.receiveElection(req));
        out.onCompleted();
    }

    /** COORDINATOR: a winner was announced. Reply at once; forwarding to neighbours happens in the background. */
    @Override
    public void coordinator(CoordinatorMessage req, StreamObserver<Ack> out) {
        election.receiveCoordinator(req);
        out.onNext(Ack.newBuilder().setOk(true).build());
        out.onCompleted();
    }
}