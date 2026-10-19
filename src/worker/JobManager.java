package worker;

import common.Log;
import grpc.ClusterState;
import grpc.Empty;
import grpc.JobProgress;
import grpc.JobRecord;
import grpc.JobRequest;
import grpc.Task;
import grpc.TaskProgress;
import grpc.TaskRecord;
import grpc.TaskStatus;
import grpc.WorkerInfo;
import grpc.WorkerProgress;
import grpc.WorkerServiceGrpc;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import jobs.Job;

/**
 * Coordinator side of job processing.
 *
 * <p>Work queue: a job is split into more tasks than there are workers
 * (tasksPerWorker x active workers). Each worker runs one task at a time and is given
 * the next PENDING task when it finishes. Because of this:
 * <ul>
 *   <li>a failed worker only loses the task it was running, which goes back to PENDING;</li>
 *   <li>a worker that joins mid-job automatically receives the next PENDING task;</li>
 *   <li>fast workers do more tasks than slow ones.</li>
 * </ul>
 *
 * <p>Failure handling for a running task:
 * <ul>
 *   <li>ExecuteTask stream breaks (window closed, crash, connection lost) - FAILED: reassigned at once;</li>
 *   <li>no progress for {@code slowAfterMs} but Ping answers - SLOW: keep waiting;</li>
 *   <li>no progress for {@code slowAfterMs} and Ping fails - FAILED: call cancelled, task reassigned.</li>
 * </ul>
 * Every task has a unique ID and an attempt number. A result is accepted only from the
 * current attempt, so a late reply from a worker presumed failed is never counted twice.
 */
public class JobManager {

    private final WorkerNode node;
    private final int tasksPerWorker;
    private final long slowAfterMs;

    private final Object lock = new Object();
    private final Map<String, JobState> jobs = new LinkedHashMap<>();   // in submission order
    private final Map<Integer, TaskState> busy = new HashMap<>();       // workerId -> task it is running
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private volatile Runnable onChange = () -> { };

    public JobManager(WorkerNode node, int tasksPerWorker, int slowAfterSeconds) {
        this.node = node;
        this.tasksPerWorker = Math.max(1, tasksPerWorker);
        this.slowAfterMs = slowAfterSeconds * 1000L;
    }

    public void start() {
        scheduler.scheduleWithFixedDelay(this::tick, 500, 500, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduler.shutdownNow();
        synchronized (lock) {
            for (TaskState ts : busy.values()) {
                if (ts.call != null) {
                    ts.call.cancel("coordinator stopping", null);
                }
            }
        }
    }

    /** Person 2: set a callback that replicates {@link #snapshot} to the other workers after every change. */
    public void setOnChange(Runnable onChange) {
        this.onChange = onChange;
    }

    // =====================================================================
    // Client-facing operations (called by JobServiceImpl and the console)
    // =====================================================================

    /** Accepts a job and splits it into tasks. Returns the job ID. */
    public String submit(JobRequest request) {
        Job job = Job.of(request.getType());
        job.validate(request);
        String jobId = request.getJobId().isBlank()
                ? UUID.randomUUID().toString().substring(0, 8)
                : request.getJobId();
        int workers = Math.max(1, activeWorkers().size());
        synchronized (lock) {
            if (jobs.containsKey(jobId)) {
                return jobId; // same job resubmitted (e.g. after a coordinator change) - keep the existing one
            }
            List<Task> tasks = job.split(jobId, request, workers * tasksPerWorker);
            jobs.put(jobId, new JobState(jobId, request.getClientId(), job, job.describe(request), tasks));
            Log.info(name(), "Job " + jobId + " accepted: " + job.describe(request)
                    + " -> " + tasks.size() + " tasks for " + workers + " workers");
        }
        changed();
        kick(); // dispatch now instead of waiting for the next tick
        return jobId;
    }

    /** Sends the current progress, then every update until the job finishes. False if the job is unknown. */
    public boolean watch(String jobId, StreamObserver<JobProgress> watcher) {
        synchronized (lock) {
            JobState js = jobs.get(jobId);
            if (js == null) {
                return false;
            }
            watcher.onNext(js.progress(selfId()));
            if (js.finished) {
                watcher.onCompleted();
            } else {
                js.watchers.add(watcher);
            }
            return true;
        }
    }

    public void unwatch(String jobId, StreamObserver<JobProgress> watcher) {
        synchronized (lock) {
            JobState js = jobs.get(jobId);
            if (js != null) {
                js.watchers.remove(watcher);
            }
        }
    }

    // =====================================================================
    // Scheduler: runs every 500 ms on one thread
    // =====================================================================

    private void tick() {
        try {
            if (!hasUnfinishedJobs()) {
                return;
            }
            checkQuietTasks();
            dispatch(activeWorkers()); // re-reads the worker list every time, so new workers are used
        } catch (RuntimeException e) {
            Log.error(name(), "Scheduler error: " + e.getMessage()); // keep the scheduler alive
        }
    }

    /** Gives PENDING tasks to workers that are not running anything. */
    private void dispatch(List<WorkerInfo> active) {
        List<TaskState> started = new ArrayList<>();
        synchronized (lock) {
            Deque<WorkerInfo> free = new ArrayDeque<>();
            for (WorkerInfo w : active) {
                if (!busy.containsKey(w.getId())) {
                    free.add(w);
                }
            }
            for (JobState js : jobs.values()) {
                if (js.finished) {
                    continue;
                }
                for (TaskState ts : js.tasks) {
                    if (free.isEmpty()) {
                        break;
                    }
                    if (ts.status == TaskStatus.PENDING) {
                        WorkerInfo w = free.poll();
                        ts.assign(w);
                        busy.put(w.getId(), ts);
                        started.add(ts);
                        // Person 2: election.recordAssignment(w.getId()) to update the JAC
                        Log.info(name(), "Assigned task " + ts.id() + " to Worker-" + w.getId()
                                + (ts.reassignments > 0 ? " (reassigned)" : ""));
                    }
                }
            }
        }
        for (TaskState ts : started) {
            startCall(ts, ts.attempt);
        }
        if (!started.isEmpty()) {
            notifyWatchers(started);
            changed();
        }
    }

    /** Opens the ExecuteTask stream to the worker and handles its progress, result or failure. */
    private void startCall(TaskState ts, int attempt) {
        WorkerInfo w = ts.worker;
        WorkerServiceGrpc.newStub(node.channels().get(w.getHost(), w.getPort()))
                .executeTask(ts.task, new ClientResponseObserver<Task, TaskProgress>() {
                    @Override
                    public void beforeStart(ClientCallStreamObserver<Task> requestStream) {
                        synchronized (lock) {
                            if (ts.isCurrent(attempt)) {
                                ts.call = requestStream; // kept so the call can be cancelled
                            }
                        }
                    }

                    @Override
                    public void onNext(TaskProgress p) {
                        onTaskProgress(ts, attempt, p);
                    }

                    @Override
                    public void onError(Throwable t) {
                        onTaskFailed(ts, attempt, "connection lost: " + Status.fromThrowable(t).getCode());
                    }

                    @Override
                    public void onCompleted() {
                        onTaskFailed(ts, attempt, "stream ended without a result"); // ignored if already DONE
                    }
                });
    }

    private void onTaskProgress(TaskState ts, int attempt, TaskProgress p) {
        boolean done = false;
        synchronized (lock) {
            if (!ts.isCurrent(attempt)) {
                return; // late reply from an attempt that was already reassigned
            }
            ts.itemsDone = p.getItemsDone();
            ts.lastProgressAt = System.currentTimeMillis();
            if (ts.slow) {
                ts.slow = false;
                Log.info(name(), "Worker-" + ts.worker.getId() + " is making progress again on " + ts.id());
            }
            if (p.getFinished()) {
                done = true;
                ts.status = TaskStatus.DONE;
                ts.result = p.getResult();
                ts.call = null;
                busy.remove(ts.worker.getId());
                Log.info(name(), "Task " + ts.id() + " done by Worker-" + ts.worker.getId() + " -> " + ts.result);
            }
            JobState js = jobs.get(ts.task.getJobId());
            if (done && js.allTasksDone()) {
                js.finish();
                Log.info(name(), "Job " + js.jobId + " FINISHED: " + js.description + " = " + js.result);
            }
            js.notifyWatchers(selfId());
        }
        if (done) {
            changed();
            kick(); // the worker is free - give it the next task now
        }
    }

    /** The worker running this task failed or became unreachable: put the task back in the queue. */
    private void onTaskFailed(TaskState ts, int attempt, String reason) {
        ClientCallStreamObserver<Task> call;
        synchronized (lock) {
            if (!ts.isCurrent(attempt)) {
                return; // already done or already reassigned
            }
            int workerId = ts.worker.getId();
            Log.info(name(), "Worker-" + workerId + " FAILED during task " + ts.id()
                    + " (" + reason + ") - task back in the queue");
            call = ts.call;
            busy.remove(workerId);
            ts.requeue();
            jobs.get(ts.task.getJobId()).notifyWatchers(selfId());
        }
        if (call != null) {
            call.cancel("task reassigned", null);
        }
        changed();
        kick();
    }

    /**
     * Active / slow / failed: a task with no progress for slowAfterMs is checked with a Ping.
     * Ping answers = the worker is alive but SLOW (keep waiting); Ping fails = FAILED (reassign).
     */
    private void checkQuietTasks() {
        long now = System.currentTimeMillis();
        List<TaskState> quiet = new ArrayList<>();
        synchronized (lock) {
            for (TaskState ts : busy.values()) {
                if (now - ts.lastProgressAt > slowAfterMs) {
                    quiet.add(ts);
                }
            }
        }
        for (TaskState ts : quiet) {
            WorkerInfo w;
            int attempt;
            synchronized (lock) {
                if (ts.status != TaskStatus.ASSIGNED) {
                    continue;
                }
                w = ts.worker;
                attempt = ts.attempt;
            }
            if (ping(w)) {
                synchronized (lock) {
                    if (ts.isCurrent(attempt) && !ts.slow) {
                        ts.slow = true;
                        Log.info(name(), "Worker-" + w.getId() + " is SLOW on task " + ts.id()
                                + " (answers pings, no progress for " + slowAfterMs / 1000 + "s) - waiting");
                        jobs.get(ts.task.getJobId()).notifyWatchers(selfId());
                    }
                }
            } else {
                onTaskFailed(ts, attempt, "no progress and not answering pings");
            }
        }
    }

    // =====================================================================
    // State for coordinator failover (Person 2)
    // =====================================================================

    /** Everything a new coordinator needs to continue the jobs. */
    public ClusterState snapshot(long term, long version) {
        ClusterState.Builder state = ClusterState.newBuilder()
                .setTerm(term).setVersion(version).setCoordinatorId(selfId());
        synchronized (lock) {
            for (JobState js : jobs.values()) {
                JobRecord.Builder job = JobRecord.newBuilder()
                        .setJobId(js.jobId).setClientId(js.clientId).setType(js.job.type());
                for (TaskState ts : js.tasks) {
                    job.addTasks(TaskRecord.newBuilder()
                            .setTask(ts.task)
                            .setStatus(ts.status)
                            .setWorkerId(ts.worker == null ? 0 : ts.worker.getId())
                            .setItemsDone(ts.itemsDone)
                            .setResult(ts.result));
                }
                state.addJobs(job);
            }
        }
        return state.build();
    }

    /**
     * Loads replicated state when this worker becomes coordinator. Finished tasks keep their
     * results; tasks that were running on the old coordinator's watch go back to PENDING.
     */
    public void restore(ClusterState state) {
        synchronized (lock) {
            for (JobRecord record : state.getJobsList()) {
                if (jobs.containsKey(record.getJobId())) {
                    continue;
                }
                Job job = Job.of(record.getType());
                List<Task> tasks = new ArrayList<>();
                for (TaskRecord tr : record.getTasksList()) {
                    tasks.add(tr.getTask());
                }
                JobState js = new JobState(record.getJobId(), record.getClientId(), job,
                        record.getType() + " job", tasks);
                for (int i = 0; i < record.getTasksCount(); i++) {
                    TaskRecord tr = record.getTasks(i);
                    if (tr.getStatus() == TaskStatus.DONE) {
                        TaskState ts = js.tasks.get(i);
                        ts.status = TaskStatus.DONE;
                        ts.result = tr.getResult();
                        ts.itemsDone = ts.itemsTotal;
                    }
                }
                if (js.allTasksDone()) {
                    js.finish();
                }
                jobs.put(js.jobId, js);
                Log.info(name(), "Recovered job " + js.jobId + " from replicated state");
            }
        }
        kick();
    }

    // =====================================================================
    // Helpers
    // =====================================================================

    /** Runs the scheduler now instead of waiting for the next tick (ignored while shutting down). */
    private void kick() {
        try {
            scheduler.execute(this::tick);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // stopping - nothing to schedule
        }
    }

    private boolean hasUnfinishedJobs() {
        synchronized (lock) {
            for (JobState js : jobs.values()) {
                if (!js.finished) {
                    return true;
                }
            }
            return false;
        }
    }

    private List<WorkerInfo> activeWorkers() {
        try {
            return node.bootstrapStub().getActiveWorkers(Empty.getDefaultInstance()).getWorkersList();
        } catch (StatusRuntimeException e) {
            Log.error(name(), "Bootstrap unreachable: " + e.getStatus().getCode());
            return List.of();
        }
    }

    private boolean ping(WorkerInfo w) {
        try {
            node.neighbours().stub(w).ping(Empty.getDefaultInstance());
            return true;
        } catch (StatusRuntimeException e) {
            return false;
        }
    }

    private void notifyWatchers(List<TaskState> tasks) {
        synchronized (lock) {
            for (TaskState ts : tasks) {
                jobs.get(ts.task.getJobId()).notifyWatchers(selfId());
            }
        }
    }

    private void changed() {
        try {
            onChange.run();
        } catch (RuntimeException e) {
            Log.error(name(), "State change callback failed: " + e.getMessage());
        }
    }

    private int selfId() {
        return node.self() == null ? 0 : node.self().getId();
    }

    private String name() {
        return node.name();
    }

    // =====================================================================
    // Job and task records (only accessed while holding the lock)
    // =====================================================================

    private static final class JobState {
        final String jobId;
        final String clientId;
        final Job job;
        final String description;
        final List<TaskState> tasks = new ArrayList<>();
        final List<StreamObserver<JobProgress>> watchers = new ArrayList<>();
        boolean finished;
        long result;

        JobState(String jobId, String clientId, Job job, String description, List<Task> tasks) {
            this.jobId = jobId;
            this.clientId = clientId;
            this.job = job;
            this.description = description;
            for (Task t : tasks) {
                this.tasks.add(new TaskState(t, job.size(t)));
            }
        }

        boolean allTasksDone() {
            for (TaskState ts : tasks) {
                if (ts.status != TaskStatus.DONE) {
                    return false;
                }
            }
            return true;
        }

        void finish() {
            List<Long> partials = new ArrayList<>();
            for (TaskState ts : tasks) {
                partials.add(ts.result);
            }
            result = job.combine(partials);
            finished = true;
        }

        /** Overall % from completed items across all tasks, plus a per-task breakdown. */
        JobProgress progress(int coordinatorId) {
            long done = 0;
            long total = 0;
            JobProgress.Builder p = JobProgress.newBuilder().setJobId(jobId).setCoordinatorId(coordinatorId);
            for (TaskState ts : tasks) {
                long taskDone = ts.status == TaskStatus.DONE ? ts.itemsTotal : ts.itemsDone;
                done += taskDone;
                total += ts.itemsTotal;
                p.addWorkers(WorkerProgress.newBuilder()
                        .setWorkerId(ts.worker == null ? 0 : ts.worker.getId())
                        .setTaskId(ts.id())
                        .setItemsDone(taskDone)
                        .setItemsTotal(ts.itemsTotal)
                        .setStatus(ts.label()));
            }
            p.setPercent(total == 0 ? 100.0 : done * 100.0 / total);
            if (finished) {
                p.setFinished(true).setResult(result).setPercent(100.0);
            }
            return p.build();
        }

        void notifyWatchers(int coordinatorId) {
            JobProgress p = progress(coordinatorId);
            for (StreamObserver<JobProgress> w : new ArrayList<>(watchers)) {
                try {
                    w.onNext(p);
                    if (finished) {
                        w.onCompleted();
                    }
                } catch (RuntimeException e) {
                    watchers.remove(w); // client went away
                }
            }
            if (finished) {
                watchers.clear();
            }
        }
    }

    private static final class TaskState {
        final Task task;
        final long itemsTotal;
        TaskStatus status = TaskStatus.PENDING;
        WorkerInfo worker;          // current or last worker
        int attempt;                // increases on every assignment
        int reassignments;
        long itemsDone;
        long result;
        long lastProgressAt;
        boolean slow;
        ClientCallStreamObserver<Task> call;

        TaskState(Task task, long itemsTotal) {
            this.task = task;
            this.itemsTotal = itemsTotal;
        }

        String id() {
            return task.getTaskId();
        }

        void assign(WorkerInfo w) {
            worker = w;
            status = TaskStatus.ASSIGNED;
            attempt++;
            itemsDone = 0;
            slow = false;
            call = null;
            lastProgressAt = System.currentTimeMillis();
        }

        void requeue() {
            status = TaskStatus.PENDING;
            reassignments++;
            itemsDone = 0;
            slow = false;
            call = null;
        }

        boolean isCurrent(int attemptNo) {
            return status == TaskStatus.ASSIGNED && attempt == attemptNo;
        }

        String label() {
            switch (status) {
                case DONE:
                    return "DONE";
                case ASSIGNED:
                    return slow ? "SLOW" : "RUNNING";
                default:
                    return reassignments > 0 ? "REASSIGNED" : "WAITING";
            }
        }
    }
}
