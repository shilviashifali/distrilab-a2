package worker;

import grpc.JobAccepted;
import grpc.JobProgress;
import grpc.JobRef;
import grpc.JobRequest;
import grpc.JobServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

/**
 * gRPC endpoint clients use to submit jobs and watch their progress (server
 * streaming).
 */
public class JobServiceImpl extends JobServiceGrpc.JobServiceImplBase {

    private final JobManager jobs;
    private final WorkerNode node;

    public JobServiceImpl(JobManager jobs, WorkerNode node) {
        this.jobs = jobs;
        this.node = node;
    }

    @Override
    public void submitJob(JobRequest req, StreamObserver<JobAccepted> out) {
        if (!node.election().isCoordinator()) {
            // The client should ask Bootstrap for the current coordinator and try again.
            out.onError(Status.FAILED_PRECONDITION
                    .withDescription("Not the coordinator (current coordinator: Worker-" + node.election().coordinatorId() + ")")
                    .asRuntimeException());
            return;
        }
        try {
            String jobId = jobs.submit(req);
            out.onNext(JobAccepted.newBuilder().setJobId(jobId).setCoordinatorId(node.self().getId()).build());
            out.onCompleted();
        } catch (IllegalArgumentException e) {
            out.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void watchJob(JobRef ref, StreamObserver<JobProgress> out) {
        ServerCallStreamObserver<JobProgress> call = (ServerCallStreamObserver<JobProgress>) out;
        call.setOnCancelHandler(() -> jobs.unwatch(ref.getJobId(), out)); // client closed or lost
        if (!jobs.watch(ref.getJobId(), out)) {
            out.onError(Status.NOT_FOUND.withDescription("Unknown job " + ref.getJobId()).asRuntimeException());
        }
    }
}
