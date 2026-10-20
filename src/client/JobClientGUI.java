package client;

import common.Channels;
import common.Defaults;
import common.Log;
import common.Settings;
import grpc.BootstrapServiceGrpc;
import grpc.CoordinatorReply;
import grpc.Empty;
import grpc.JobRequest;
import grpc.JobServiceGrpc;
import grpc.JobType;
import grpc.WorkerInfo;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.NumberFormat;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.UnsupportedLookAndFeelException;

/**
 * DistriLab client. Any number of jobs can be submitted; each gets its own row with a
 * live progress bar fed by the WatchJob gRPC stream. Clicking a progress bar opens the
 * worker-level breakdown for that job. Run this program several times for several clients.
 *
 * Usage: JobClientGUI [--bootstrapHost=localhost] [--bootstrapPort=50050]
 */
public class JobClientGUI extends JFrame {

    private static final long serialVersionUID = 1L;

    static final Color GREEN = new Color(0x2E7D32);
    static final Color ORANGE = new Color(0xE65100);
    static final Color RED = new Color(0xC62828);
    static final Color GREY = new Color(0x616161);
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss");

    private final String clientId = "client-" + UUID.randomUUID().toString().substring(0, 6);
    private final String bootstrapHost;
    private final int bootstrapPort;
    private final int timeoutMs;
    private final transient Channels channels = new Channels();

    private final JComboBox<String> typeBox = new JComboBox<>(new String[]{"PRIMESUM", "MAX", "PRIMECOUNT"});
    private final JTextField inputField = new JTextField(32);
    private final JLabel hintLabel = new JLabel();
    private final JPanel jobsList = new JPanel();
    private final JLabel emptyLabel = new JLabel("No jobs yet. Submit one above.");
    private final JTextArea logArea = new JTextArea();
    private final AtomicInteger jobCounter = new AtomicInteger();

    public JobClientGUI(Settings settings) {
        super("DistriLab Client");
        this.bootstrapHost = settings.get("bootstrapHost", Defaults.BOOTSTRAP_HOST);
        this.bootstrapPort = settings.getInt("bootstrapPort", Defaults.BOOTSTRAP_PORT);
        this.timeoutMs = settings.getInt("rpcTimeoutMs", Defaults.RPC_TIMEOUT_MS);

        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {
                channels.closeAll();
            }
        });

        JPanel root = new JPanel(new BorderLayout(0, 10));
        root.setBorder(BorderFactory.createEmptyBorder(12, 14, 12, 14));
        root.add(buildHeader(), BorderLayout.NORTH);

        JSplitPane split = new JSplitPane(JSplitPane.VERTICAL_SPLIT, buildJobsPanel(), buildLogPanel());
        split.setResizeWeight(0.65);
        split.setBorder(null);

        JPanel center = new JPanel(new BorderLayout(0, 10));
        center.add(buildForm(), BorderLayout.NORTH);
        center.add(split, BorderLayout.CENTER);
        root.add(center, BorderLayout.CENTER);

        setContentPane(root);
        setSize(900, 680);
        setMinimumSize(new Dimension(760, 520));
        setLocationRelativeTo(null);
        updateHint();
        log("Client " + clientId + " started. Bootstrap at " + bootstrapHost + ":" + bootstrapPort);
    }

    // =====================================================================
    // Layout
    // =====================================================================

    private JPanel buildHeader() {
        JLabel title = new JLabel("DistriLab Client");
        title.setFont(title.getFont().deriveFont(Font.BOLD, 20f));
        JLabel info = new JLabel("Client ID: " + clientId + "     Bootstrap: " + bootstrapHost + ":" + bootstrapPort);
        info.setForeground(GREY);
        JPanel header = new JPanel();
        header.setLayout(new BoxLayout(header, BoxLayout.Y_AXIS));
        header.add(title);
        header.add(Box.createVerticalStrut(2));
        header.add(info);
        return header;
    }

    private JPanel buildForm() {
        JPanel form = new JPanel(new GridBagLayout());
        form.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createTitledBorder("New job"),
                BorderFactory.createEmptyBorder(4, 8, 8, 8)));
        GridBagConstraints c = new GridBagConstraints();
        c.insets = new Insets(4, 4, 4, 4);
        c.anchor = GridBagConstraints.WEST;

        c.gridx = 0;
        c.gridy = 0;
        form.add(new JLabel("Job type"), c);
        c.gridx = 1;
        typeBox.addActionListener(e -> updateHint());
        form.add(typeBox, c);

        c.gridx = 0;
        c.gridy = 1;
        form.add(new JLabel("Input"), c);
        c.gridx = 1;
        c.weightx = 1;
        c.fill = GridBagConstraints.HORIZONTAL;
        inputField.addActionListener(e -> onSubmit()); // Enter submits
        form.add(inputField, c);

        c.gridx = 1;
        c.gridy = 2;
        c.insets = new Insets(0, 6, 4, 4);
        hintLabel.setForeground(GREY);
        hintLabel.setFont(hintLabel.getFont().deriveFont(Font.ITALIC));
        form.add(hintLabel, c);

        JButton loadCsv = new JButton("Load CSV...");
        loadCsv.addActionListener(e -> onLoadCsv());
        JButton submit = new JButton("Submit Job");
        submit.setFont(submit.getFont().deriveFont(Font.BOLD));
        submit.addActionListener(e -> onSubmit());
        JPanel buttons = new JPanel(new FlowLayout(FlowLayout.RIGHT, 6, 0));
        buttons.add(loadCsv);
        buttons.add(submit);

        c.gridx = 1;
        c.gridy = 3;
        c.insets = new Insets(4, 4, 0, 4);
        form.add(buttons, c);
        return form;
    }

    private JPanel buildJobsPanel() {
        jobsList.setLayout(new BoxLayout(jobsList, BoxLayout.Y_AXIS));
        emptyLabel.setForeground(GREY);
        emptyLabel.setBorder(BorderFactory.createEmptyBorder(12, 10, 12, 10));
        jobsList.add(emptyLabel);

        JPanel wrapper = new JPanel(new BorderLayout());
        wrapper.add(jobsList, BorderLayout.NORTH); // rows stay at the top
        JScrollPane scroll = new JScrollPane(wrapper);
        scroll.setBorder(null);
        scroll.getVerticalScrollBar().setUnitIncrement(16);

        JPanel panel = new JPanel(new BorderLayout());
        panel.setBorder(BorderFactory.createTitledBorder("Jobs  (click a progress bar to see the worker breakdown)"));
        panel.add(scroll, BorderLayout.CENTER);
        panel.setPreferredSize(new Dimension(100, 300));
        return panel;
    }

    private JPanel buildLogPanel() {
        logArea.setEditable(false);
        logArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        JButton clear = new JButton("Clear Log");
        clear.addActionListener(e -> logArea.setText(""));
        JPanel top = new JPanel(new FlowLayout(FlowLayout.RIGHT, 0, 0));
        top.add(clear);

        JPanel panel = new JPanel(new BorderLayout(0, 4));
        panel.setBorder(BorderFactory.createTitledBorder("Log"));
        panel.add(top, BorderLayout.NORTH);
        panel.add(new JScrollPane(logArea), BorderLayout.CENTER);
        return panel;
    }

    private void updateHint() {
        boolean range = "PRIMESUM".equals(typeBox.getSelectedItem());
        hintLabel.setText(range
                ? "Range as start,end   e.g. 1,100000000"
                : "Comma-separated whole numbers   e.g. 4,8,15,16,23,42");
    }

    // =====================================================================
    // Actions
    // =====================================================================

    private void onSubmit() {
        JobRequest request;
        String description;
        try {
            JobType type = JobType.valueOf((String) typeBox.getSelectedItem());
            JobRequest.Builder b = JobRequest.newBuilder()
                    .setJobId(UUID.randomUUID().toString())   // same ID on every retry = safe resubmission
                    .setClientId(clientId)
                    .setType(type);
            String text = inputField.getText().trim();
            if (type == JobType.PRIMESUM) {
                String[] parts = text.split("[,\\s]+");
                if (parts.length != 2 || parts[0].isEmpty()) {
                    throw new IllegalArgumentException("Enter the range as start,end (for example 1,100000000).");
                }
                long start = parseNumber(parts[0]);
                long end = parseNumber(parts[1]);
                if (start > end) {
                    throw new IllegalArgumentException("Start must be less than or equal to end.");
                }
                b.setStart(start).setEnd(end);
                description = "PRIMESUM(" + fmt(start) + ", " + fmt(end) + ")";
            } else {
                List<Long> numbers = new ArrayList<>();
                for (String part : text.split(",")) {
                    if (!part.isBlank()) {
                        numbers.add(parseNumber(part));
                    }
                }
                if (numbers.isEmpty()) {
                    throw new IllegalArgumentException("Enter at least one number, separated by commas.");
                }
                b.addAllNumbers(numbers);
                description = type + " of " + fmt(numbers.size()) + " numbers";
            }
            request = b.build();
        } catch (IllegalArgumentException e) {
            JOptionPane.showMessageDialog(this, e.getMessage(), "Invalid input", JOptionPane.WARNING_MESSAGE);
            return;
        }

        int number = jobCounter.incrementAndGet();
        JobPanel row = new JobPanel(this, number, request, description);
        jobsList.remove(emptyLabel);
        jobsList.add(row, 0); // newest job at the top
        jobsList.revalidate();
        jobsList.repaint();
        log("[Job " + number + "] Submitting " + description);
        row.start();
    }

    /** Reads whole numbers from a CSV file (commas and/or new lines); non-numbers such as headers are skipped. */
    private void onLoadCsv() {
        JFileChooser chooser = new JFileChooser(new File("."));
        if (chooser.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) {
            return;
        }
        File file = chooser.getSelectedFile();
        try {
            List<String> numbers = new ArrayList<>();
            int skipped = 0;
            for (String line : Files.readAllLines(file.toPath())) {
                for (String part : line.split(",")) {
                    String value = part.replace("﻿", "").trim(); // Excel byte-order mark
                    if (value.isEmpty()) {
                        continue;
                    }
                    try {
                        Long.parseLong(value);
                        numbers.add(value);
                    } catch (NumberFormatException e) {
                        skipped++;
                    }
                }
            }
            if (numbers.isEmpty()) {
                log("No numbers found in " + file.getName());
                return;
            }
            boolean range = "PRIMESUM".equals(typeBox.getSelectedItem());
            if (range && numbers.size() < 2) {
                log("For PRIMESUM the CSV must contain a start and an end value.");
                return;
            }
            inputField.setText(range ? numbers.get(0) + "," + numbers.get(1) : String.join(",", numbers));
            log("Loaded " + (range ? "range " + numbers.get(0) + " to " + numbers.get(1) : numbers.size() + " numbers")
                    + " from " + file.getName() + (skipped > 0 ? " (skipped " + skipped + " non-numeric values)" : ""));
        } catch (IOException e) {
            log("Could not read " + file.getName() + ": " + e.getMessage());
        }
    }

    private static long parseNumber(String text) {
        try {
            return Long.parseLong(text.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("'" + abbreviate(text.trim()) + "' is not a valid whole number.\n"
                    + "Numbers must be between " + fmt(Long.MIN_VALUE) + " and " + fmt(Long.MAX_VALUE) + ".");
        }
    }

    private static String abbreviate(String s) {
        return s.length() > 30 ? s.substring(0, 27) + "..." : s;
    }

    // =====================================================================
    // Services used by JobPanel
    // =====================================================================

    /** Asks the Bootstrap Node who the coordinator currently is. */
    CoordinatorReply findCoordinator() {
        return BootstrapServiceGrpc.newBlockingStub(channels.get(bootstrapHost, bootstrapPort))
                .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                .getCoordinator(Empty.getDefaultInstance());
    }

    /** Stub for short calls (SubmitJob) to the coordinator, with a deadline. */
    JobServiceGrpc.JobServiceBlockingStub jobStub(WorkerInfo coordinator) {
        return JobServiceGrpc.newBlockingStub(channels.get(coordinator.getHost(), coordinator.getPort()))
                .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);
    }

    /** Stub for the long-running WatchJob stream (no deadline). */
    JobServiceGrpc.JobServiceBlockingStub watchStub(WorkerInfo coordinator) {
        return JobServiceGrpc.newBlockingStub(channels.get(coordinator.getHost(), coordinator.getPort()));
    }

    /** Forgets the connection to a coordinator that has failed. */
    void dropChannel(WorkerInfo coordinator) {
        channels.close(coordinator.getHost(), coordinator.getPort());
    }

    /** Adds a timestamped line to the log. Safe to call from any thread. */
    void log(String message) {
        String line = "[" + LocalTime.now().format(TIME) + "] " + message + "\n";
        SwingUtilities.invokeLater(() -> {
            logArea.append(line);
            logArea.setCaretPosition(logArea.getDocument().getLength());
        });
    }

    static String fmt(long n) {
        return NumberFormat.getIntegerInstance().format(n);
    }

    // =====================================================================
    // Start-up
    // =====================================================================

    public static void main(String[] args) {
        Settings settings;
        try {
            settings = Settings.load(args);
        } catch (IllegalArgumentException e) {
            Log.error("Client", e.getMessage());
            return;
        }
        try {
            for (UIManager.LookAndFeelInfo info : UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (ReflectiveOperationException | UnsupportedLookAndFeelException e) {
            // keep the default look and feel
        }
        SwingUtilities.invokeLater(() -> new JobClientGUI(settings).setVisible(true));
    }
}
