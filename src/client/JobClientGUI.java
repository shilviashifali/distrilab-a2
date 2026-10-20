
package client;

import common.Defaults;
import grpc.BootstrapServiceGrpc;
import grpc.CoordinatorReply;
import grpc.Empty;
import grpc.JobAccepted;
import grpc.JobProgress;
import grpc.JobRef;
import grpc.JobRequest;
import grpc.JobServiceGrpc;
import grpc.JobType;
import grpc.WorkerInfo;
import grpc.WorkerProgress;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.swing.*;

public class JobClientGUI extends JFrame {

    private JComboBox<String> jobType;
    private JTextField inputField;
    private JButton submitButton;
    private JProgressBar progressBar;
    private JTextArea workerDetails;
    private JLabel statusLabel;

    private volatile boolean closed = false;
    private volatile long generation = 0;

    public JobClientGUI() {
        setTitle("DistriLab - Distributed Job Monitor");
        setSize(750, 550);
        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        setLocationRelativeTo(null);

        JPanel mainPanel = new JPanel(
                new BorderLayout(15, 15));
        mainPanel.setBorder(
                BorderFactory.createEmptyBorder(20, 20, 20, 20));

        JPanel inputPanel = new JPanel(
                new GridLayout(3, 2, 10, 10));

        jobType = new JComboBox<>(
                new String[]{"MAX", "PRIMESUM", "PRIMECOUNT"});

        inputField = new JTextField();
        submitButton = new JButton("Submit Job");

        inputPanel.add(new JLabel("Job Type:"));
        inputPanel.add(jobType);
        inputPanel.add(new JLabel("Numbers / Range:"));
        inputPanel.add(inputField);
        inputPanel.add(new JLabel(""));
        inputPanel.add(submitButton);

        progressBar = new JProgressBar(0, 100);
        progressBar.setValue(0);
        progressBar.setStringPainted(true);
        progressBar.setPreferredSize(new Dimension(600, 25));

        statusLabel = new JLabel("Status: Waiting for job");

        JPanel progressPanel = new JPanel(
                new BorderLayout(5, 5));

        progressPanel.add(
                new JLabel("Overall Job Progress:"),
                BorderLayout.NORTH);
        progressPanel.add(
                progressBar, BorderLayout.CENTER);
        progressPanel.add(
                statusLabel, BorderLayout.SOUTH);

        workerDetails = new JTextArea();
        workerDetails.setEditable(false);
        workerDetails.setFont(
                new java.awt.Font("Monospaced",
                        java.awt.Font.PLAIN, 12));
        workerDetails.setText(
                "Worker progress will appear here.");
        workerDetails.setLineWrap(true);
        workerDetails.setWrapStyleWord(true);

        JScrollPane scrollPane =
                new JScrollPane(workerDetails);
        scrollPane.setBorder(
                BorderFactory.createTitledBorder(
                        "Worker Details"));

        JPanel centerPanel = new JPanel(
                new BorderLayout(10, 10));
        centerPanel.add(
                progressPanel, BorderLayout.NORTH);
        centerPanel.add(
                scrollPane, BorderLayout.CENTER);

        mainPanel.add(inputPanel, BorderLayout.NORTH);
        mainPanel.add(centerPanel, BorderLayout.CENTER);
        add(mainPanel);

        submitButton.addActionListener(e -> submitJob());
    }

    private void updateUI(long run, Runnable action) {
        SwingUtilities.invokeLater(() -> {
            if (!closed && generation == run) {
                action.run();
            }
        });
    }

    private ManagedChannel connect(WorkerInfo worker) {
        return ManagedChannelBuilder
                .forAddress(worker.getHost(), worker.getPort())
                .usePlaintext()
                .build();
    }

    private CoordinatorReply findCoordinator(
            ManagedChannel bootstrap) {

        return BootstrapServiceGrpc
                .newBlockingStub(bootstrap)
                .withDeadlineAfter(
                        Defaults.RPC_TIMEOUT_MS,
                        TimeUnit.MILLISECONDS)
                .getCoordinator(Empty.getDefaultInstance());
    }

    private void submitJob() {
        final JobRequest request;

        try {
            String input = inputField.getText().trim();

            JobType type = JobType.valueOf(
                    jobType.getSelectedItem().toString());

            JobRequest.Builder builder =
                    JobRequest.newBuilder()
                            .setJobId(UUID.randomUUID().toString())
                            .setClientId("GUI-Client")
                            .setType(type);

            if (type == JobType.PRIMESUM) {
                String[] range = input.split(",", -1);

                if (range.length != 2) {
                    throw new IllegalArgumentException(
                            "Enter range as start,end");
                }

                long start =
                        Long.parseLong(range[0].trim());
                long end =
                        Long.parseLong(range[1].trim());

                if (start > end) {
                    throw new IllegalArgumentException(
                            "Start must not exceed end");
                }

                builder.setStart(start);
                builder.setEnd(end);

            } else {
                if (input.isEmpty()) {
                    throw new IllegalArgumentException(
                            "Enter comma-separated numbers");
                }

                for (String number : input.split(",", -1)) {
                    builder.addNumbers(
                            Long.parseLong(number.trim()));
                }
            }

            request = builder.build();

        } catch (IllegalArgumentException ex) {
            JOptionPane.showMessageDialog(
                    this,
                    "Invalid input: " + ex.getMessage(),
                    "Input Error",
                    JOptionPane.ERROR_MESSAGE);
            return;
        }

        final long run = ++generation;

        submitButton.setEnabled(false);
        progressBar.setValue(0);
        workerDetails.setText("");
        statusLabel.setText(
                "Status: Connecting to coordinator...");

        Thread thread = new Thread(() ->
                submitAndWatch(request, run),
                "job-submit-watch");

        thread.setDaemon(true);
        thread.start();
    }

    private void submitAndWatch(
            JobRequest request, long run) {

        ManagedChannel bootstrap = null;
        ManagedChannel coordinatorChannel = null;

        try {
            bootstrap = ManagedChannelBuilder
                    .forAddress(
                            Defaults.BOOTSTRAP_HOST,
                            Defaults.BOOTSTRAP_PORT)
                    .usePlaintext()
                    .build();

            CoordinatorReply reply =
                    findCoordinator(bootstrap);

            if (!reply.getFound()) {
                throw new IllegalStateException(
                        "No coordinator available. "
                        + "Wait for leader election.");
            }

            coordinatorChannel =
                    connect(reply.getCoordinator());

            long submittedAt = System.nanoTime();

            JobAccepted accepted =
                    JobServiceGrpc
                            .newBlockingStub(coordinatorChannel)
                            .withDeadlineAfter(
                                    Defaults.RPC_TIMEOUT_MS,
                                    TimeUnit.MILLISECONDS)
                            .submitJob(request);

            String jobId = accepted.getJobId();

            System.out.printf(
                    "[GUI] Job accepted: %s "
                    + "(submit RPC %.1f ms)%n",
                    jobId,
                    (System.nanoTime() - submittedAt)
                    / 1_000_000.0);

            updateUI(run, () -> {
                statusLabel.setText(
                        "Status: Job accepted. Watching progress...");
                workerDetails.setText(
                        "Job accepted successfully!\n\n"
                        + "Job ID: " + jobId
                        + "\nJob Type: " + request.getType()
                        + "\nCoordinator: Worker-"
                        + accepted.getCoordinatorId()
                        + "\n\nWaiting for live updates...");
            });

            coordinatorChannel.shutdown();
            coordinatorChannel = null;

            watchProgress(bootstrap, jobId, run);

        } catch (Exception ex) {
            updateUI(run, () -> {
                statusLabel.setText(
                        "Status: Submission failed");
                workerDetails.setText(
                        "Submission error:\n"
                        + ex.getMessage());

                JOptionPane.showMessageDialog(
                        this,
                        ex.getMessage(),
                        "Connection Error",
                        JOptionPane.ERROR_MESSAGE);
            });

        } finally {
            if (coordinatorChannel != null) {
                coordinatorChannel.shutdownNow();
            }
            if (bootstrap != null) {
                bootstrap.shutdownNow();
            }
            updateUI(run, () ->
                    submitButton.setEnabled(true));
        }
    }

    private void watchProgress(
            ManagedChannel bootstrap,
            String jobId,
            long run) throws InterruptedException {

        int attempts = 0;
        final int maxAttempts = 20;
        final int retryDelayMs = 1500;

        while (!closed && generation == run
                && attempts < maxAttempts) {

            ManagedChannel channel = null;

            try {
                CoordinatorReply reply =
                        findCoordinator(bootstrap);

                if (!reply.getFound()) {
                    throw new IllegalStateException(
                            "Election in progress");
                }

                WorkerInfo coordinator =
                        reply.getCoordinator();

                channel = connect(coordinator);

                final int coordinatorId = coordinator.getId();

                updateUI(run, () ->
                        statusLabel.setText(
                                "Status: Streaming from Worker-"
                                + coordinatorId));

                JobRef ref = JobRef.newBuilder()
                        .setJobId(jobId)
                        .build();

                JobServiceGrpc.JobServiceBlockingStub stub =
                        JobServiceGrpc.newBlockingStub(channel);

                java.util.Iterator<JobProgress> stream =
                        stub.watchJob(ref);

                boolean receivedUpdate = false;

                long watchStartedAt = System.nanoTime();

                System.out.printf(
                        "[GUI] Watching job %s on Worker-%d%n",
                        jobId, coordinatorId);

                while (!closed && generation == run
                        && stream.hasNext()) {

                    JobProgress progress = stream.next();

                    System.out.printf(
                            "[GUI] Update after %.1f ms: "
                            + "%.2f%%, finished=%s, tasks=%d%n",
                            (System.nanoTime() - watchStartedAt)
                            / 1_000_000.0,
                            progress.getPercent(),
                            progress.getFinished(),
                            progress.getWorkersCount());

                    receivedUpdate = true;
                    attempts = 0;

                    updateUI(run, () ->
                            displayProgress(progress));

                    if (progress.getFinished()) {
                        return;
                    }
                }

                if (receivedUpdate) {
                    updateUI(run, () ->
                            statusLabel.setText(
                                    "Status: Stream ended. Reconnecting..."));
                }

            } catch (StatusRuntimeException ex) {
                Status.Code code = ex.getStatus().getCode();

                updateUI(run, () ->
                        statusLabel.setText(
                                "Status: Stream interrupted ("
                                + code + "). Reconnecting..."));

            } catch (RuntimeException ex) {
                updateUI(run, () ->
                        statusLabel.setText(
                                "Status: Waiting for coordinator..."));

            } finally {
                if (channel != null) {
                    channel.shutdownNow();
                }
            }

            attempts++;
            Thread.sleep(retryDelayMs);
        }

        if (!closed && generation == run) {
            updateUI(run, () -> {
                statusLabel.setText(
                        "Status: Progress connection unavailable");
                workerDetails.append(
                        "\n\nUnable to reconnect after "
                        + maxAttempts + " attempts."
                        + "\nCheck coordinator and worker logs.");
            });
        }
    }

    private void displayProgress(JobProgress progress) {
        int percent = (int) Math.max(
                0, Math.min(100,
                        Math.round(progress.getPercent())));

        progressBar.setValue(percent);

        StringBuilder details = new StringBuilder();

        details.append("Job ID: ")
                .append(progress.getJobId())
                .append("\nCoordinator: Worker-")
                .append(progress.getCoordinatorId())
                .append("\nOverall Progress: ")
                .append(String.format("%.1f",
                        progress.getPercent()))
                .append("%\n\n");

        details.append("INDIVIDUAL WORKER PROGRESS\n");
        details.append("--------------------------\n");

        for (WorkerProgress worker :
                progress.getWorkersList()) {

            long done = worker.getItemsDone();
            long total = worker.getItemsTotal();

            double workerPercent =
                    total > 0
                    ? (done * 100.0 / total)
                    : 0;

            details.append("Worker-")
                    .append(worker.getWorkerId())
                    .append("\nTask: ")
                    .append(worker.getTaskId())
                    .append("\nStatus: ")
                    .append(worker.getStatus())
                    .append("\nItems: ")
                    .append(done)
                    .append(" / ")
                    .append(total)
                    .append("\nProgress: ")
                    .append(String.format("%.1f",
                            workerPercent))
                    .append("%\n\n");
        }

        if (progress.getFinished()) {
            progressBar.setValue(100);

            details.append("==========================\n")
                    .append("JOB COMPLETED\n")
                    .append("Final Result: ")
                    .append(progress.getResult())
                    .append("\n");

            statusLabel.setText(
                    "Status: Completed successfully");

        } else {
            statusLabel.setText(
                    "Status: Processing - "
                    + percent + "%");
        }

        workerDetails.setText(details.toString());
        workerDetails.setCaretPosition(0);
    }

    @Override
    public void dispose() {
        closed = true;
        generation++;
        super.dispose();
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() ->
                new JobClientGUI().setVisible(true));
    }
}
