package client;

import grpc.CoordinatorReply;
import grpc.JobAccepted;
import grpc.JobProgress;
import grpc.JobRef;
import grpc.JobRequest;
import grpc.WorkerInfo;
import grpc.WorkerProgress;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.Iterator;
import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.SwingUtilities;

/**
 * One submitted job: a row with its own progress bar, and a background thread that
 * submits the job and follows its WatchJob stream. If the coordinator fails, the thread
 * asks the Bootstrap for the new coordinator and resubscribes, so the bar continues.
 */
public class JobPanel extends JPanel {

    private static final long serialVersionUID = 1L;
    private static final int RETRY_DELAY_MS = 1500;
    private static final int MAX_SUBMIT_ATTEMPTS = 20;     // ~30 s: covers an election or a term change
    private static final int MAX_RECONNECT_ATTEMPTS = 40;  // ~60 s without any update before giving up

    private final transient JobClientGUI gui;
    private final int number;
    private final transient JobRequest request;
    private final String description;
    private final JProgressBar bar = new JProgressBar(0, 1000);
    private final JLabel statusLabel = new JLabel("Submitting...");
    private volatile transient JobProgress latest;
    private transient BreakdownDialog dialog;

    public JobPanel(JobClientGUI gui, int number, JobRequest request, String description) {
        super(new BorderLayout(12, 0));
        this.gui = gui;
        this.number = number;
        this.request = request;
        this.description = description;

        setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createMatteBorder(0, 0, 1, 0, new Color(0xE0E0E0)),
                BorderFactory.createEmptyBorder(8, 10, 8, 10)));

        JLabel name = new JLabel("<html><b>Job " + number + "</b><br>" + description + "</html>");
        name.setPreferredSize(new Dimension(240, 36));

        bar.setStringPainted(true);
        bar.setString("0%");
        bar.setPreferredSize(new Dimension(200, 24));
        bar.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
        bar.setToolTipText("Click to see which worker is doing each task");

        statusLabel.setPreferredSize(new Dimension(250, 36));
        statusLabel.setForeground(JobClientGUI.GREY);

        add(name, BorderLayout.WEST);
        add(bar, BorderLayout.CENTER);
        add(statusLabel, BorderLayout.EAST);

        MouseAdapter openBreakdown = new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                showBreakdown();
            }
        };
        bar.addMouseListener(openBreakdown);
        addMouseListener(openBreakdown);
        setMaximumSize(new Dimension(Integer.MAX_VALUE, 60));
    }

    public void start() {
        Thread t = new Thread(this::run, "job-" + number);
        t.setDaemon(true);
        t.start();
    }

    private void run() {
        try {
            String jobId = submit();
            if (jobId != null) {
                watch(jobId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // =====================================================================
    // gRPC: submit, then follow the progress stream
    // =====================================================================

    /** Sends the job to the current coordinator, retrying while a coordinator is being (re)elected. */
    private String submit() throws InterruptedException {
        for (int attempt = 1; attempt <= MAX_SUBMIT_ATTEMPTS; attempt++) {
            WorkerInfo coordinator = null;
            try {
                CoordinatorReply reply = gui.findCoordinator();
                if (reply.getFound()) {
                    coordinator = reply.getCoordinator();
                    JobAccepted accepted = gui.jobStub(coordinator).submitJob(request);
                    gui.log("[Job " + number + "] Accepted by coordinator Worker-" + accepted.getCoordinatorId());
                    status("Accepted by Worker-" + accepted.getCoordinatorId(), JobClientGUI.GREY);
                    return accepted.getJobId();
                }
                status("Waiting for a coordinator (election in progress)...", JobClientGUI.ORANGE);
            } catch (StatusRuntimeException e) {
                Status.Code code = e.getStatus().getCode();
                if (code == Status.Code.INVALID_ARGUMENT) {
                    fail("Rejected: " + e.getStatus().getDescription());
                    return null;
                }
                if (coordinator != null && code == Status.Code.UNAVAILABLE) {
                    gui.dropChannel(coordinator);
                }
                String why = e.getStatus().getDescription() != null ? e.getStatus().getDescription() : code.toString();
                status("Retrying... (" + why + ")", JobClientGUI.ORANGE);
                if (attempt == 1) {
                    gui.log("[Job " + number + "] Not accepted yet (" + why + ") - retrying");
                }
            }
            Thread.sleep(RETRY_DELAY_MS);
        }
        fail("Could not submit: no coordinator available");
        return null;
    }

    /** Follows WatchJob; on a broken stream, finds the (new) coordinator and resubscribes. */
    private void watch(String jobId) throws InterruptedException {
        JobRef ref = JobRef.newBuilder().setJobId(jobId).build();
        int failures = 0;
        while (failures < MAX_RECONNECT_ATTEMPTS) {
            WorkerInfo coordinator = null;
            try {
                CoordinatorReply reply = gui.findCoordinator();
                if (reply.getFound()) {
                    coordinator = reply.getCoordinator();
                    Iterator<JobProgress> stream = gui.watchStub(coordinator).watchJob(ref);
                    while (stream.hasNext()) {
                        JobProgress p = stream.next();
                        if (failures > 0) {
                            gui.log("[Job " + number + "] Reconnected to coordinator Worker-" + p.getCoordinatorId());
                        }
                        failures = 0;
                        update(p);
                        if (p.getFinished()) {
                            gui.log("[Job " + number + "] Finished: " + description + " = " + JobClientGUI.fmt(p.getResult()));
                            return;
                        }
                    }
                } else {
                    status("Coordinator lost - waiting for a new election...", JobClientGUI.ORANGE);
                }
            } catch (StatusRuntimeException e) {
                if (coordinator != null && e.getStatus().getCode() == Status.Code.UNAVAILABLE) {
                    gui.dropChannel(coordinator);
                }
                if (failures == 0) {
                    gui.log("[Job " + number + "] Lost connection to coordinator ("
                            + e.getStatus().getCode() + ") - reconnecting");
                }
                status("Reconnecting to the coordinator...", JobClientGUI.ORANGE);
            }
            failures++;
            Thread.sleep(RETRY_DELAY_MS);
        }
        fail("Lost contact with the cluster");
    }

    // =====================================================================
    // UI updates (always on the Swing thread)
    // =====================================================================

    private void update(JobProgress p) {
        latest = p;
        SwingUtilities.invokeLater(() -> {
            bar.setValue((int) Math.round(p.getPercent() * 10));
            bar.setString(String.format("%.1f%%", p.getPercent()));
            if (p.getFinished()) {
                setStatus("<html><b>Done</b> = " + JobClientGUI.fmt(p.getResult()) + "</html>", JobClientGUI.GREEN);
            } else {
                int running = 0;
                int slow = 0;
                int reassigned = 0;
                for (WorkerProgress w : p.getWorkersList()) {
                    switch (w.getStatus()) {
                        case "RUNNING" -> running++;
                        case "SLOW" -> slow++;
                        case "REASSIGNED" -> reassigned++;
                        default -> { }
                    }
                }
                String text = "Running: " + (running + slow) + " task(s) on workers, coordinator Worker-" + p.getCoordinatorId();
                if (slow > 0 || reassigned > 0) {
                    text += " (" + (slow > 0 ? slow + " slow " : "") + (reassigned > 0 ? reassigned + " reassigned" : "") + ")";
                }
                setStatus("<html>" + text + "</html>", slow > 0 || reassigned > 0 ? JobClientGUI.ORANGE : JobClientGUI.GREY);
            }
            if (dialog != null && dialog.isVisible()) {
                dialog.update(p);
            }
        });
    }

    private void status(String text, Color color) {
        SwingUtilities.invokeLater(() -> setStatus("<html>" + text + "</html>", color));
    }

    private void setStatus(String html, Color color) {
        statusLabel.setText(html);
        statusLabel.setForeground(color);
    }

    private void fail(String reason) {
        gui.log("[Job " + number + "] FAILED: " + reason);
        SwingUtilities.invokeLater(() -> {
            bar.setString("Failed");
            setStatus("<html><b>Failed:</b> " + reason + "</html>", JobClientGUI.RED);
        });
    }

    private void showBreakdown() {
        if (dialog == null) {
            dialog = new BreakdownDialog(gui, "Job " + number + ": " + description);
        }
        JobProgress p = latest;
        if (p != null) {
            dialog.update(p);
        }
        dialog.setVisible(true);
        dialog.toFront();
    }
}
