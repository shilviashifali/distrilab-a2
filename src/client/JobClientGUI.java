package client;

import common.BootstrapService;
import common.Config;
import common.Job;
import common.JobResult;
import common.WorkerInfo;
import jobs.MaxJob;
import jobs.PrimeCountJob;
import jobs.PrimeSumJob;
import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.io.File;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.swing.*;

/**
 * DistriLab client GUI. Each submission runs on its own SwingWorker thread,
 * so several jobs can be in progress at once without freezing the window.
 */
public class JobClientGUI extends JFrame {
    private static final long serialVersionUID = 1L;

    private static final String MAX = "MAX";
    private static final String PRIMESUM = "PRIMESUM";
    private static final String PRIMECOUNT = "PRIMECOUNT";

    private final JTextField hostField = new JTextField(Config.DEFAULT_BOOTSTRAP_HOST, 12);
    private final JTextField portField = new JTextField(String.valueOf(Config.DEFAULT_BOOTSTRAP_PORT), 5);
    private final JComboBox<String> jobTypeBox = new JComboBox<>(new String[]{MAX, PRIMESUM, PRIMECOUNT});
    private final JTextField numbersField = new JTextField(28);
    private final JTextField startField = new JTextField(8);
    private final JTextField endField = new JTextField(8);
    private final JPanel numbersPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
    private final JPanel rangePanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
    private final JTextArea resultsArea = new JTextArea();
    private final AtomicInteger jobCounter = new AtomicInteger(0);

    public JobClientGUI() {
        super("DistriLab Client");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(640, 520);
        setLayout(new BorderLayout(10, 10));
        add(buildInputPanel(), BorderLayout.NORTH);
        add(buildResultsPanel(), BorderLayout.CENTER);
        updateInputFields();
    }

    // ---------------------------------------------------------------------
    // Layout
    // ---------------------------------------------------------------------

    private JPanel buildInputPanel() {
        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        panel.setBorder(BorderFactory.createEmptyBorder(10, 10, 0, 10));

        JPanel connectionPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        connectionPanel.add(new JLabel("Bootstrap host:"));
        connectionPanel.add(hostField);
        connectionPanel.add(new JLabel("Port:"));
        connectionPanel.add(portField);
        panel.add(connectionPanel);

        JPanel typePanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        typePanel.add(new JLabel("Job type:"));
        jobTypeBox.addActionListener(e -> updateInputFields());
        typePanel.add(jobTypeBox);
        panel.add(typePanel);

        numbersPanel.add(new JLabel("Numbers (comma-separated):"));
        numbersPanel.add(numbersField);
        panel.add(numbersPanel);

        rangePanel.add(new JLabel("Start:"));
        rangePanel.add(startField);
        rangePanel.add(new JLabel("End:"));
        rangePanel.add(endField);
        panel.add(rangePanel);

        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        JButton loadButton = new JButton("Load CSV");
        loadButton.addActionListener(this::onLoadCsv);
        JButton submitButton = new JButton("Submit Job");
        submitButton.addActionListener(this::onSubmit);
        JButton clearButton = new JButton("Clear Log");
        clearButton.addActionListener(e -> resultsArea.setText(""));
        buttonPanel.add(loadButton);
        buttonPanel.add(submitButton);
        buttonPanel.add(clearButton);
        panel.add(buttonPanel);

        return panel;
    }

    private JPanel buildResultsPanel() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBorder(BorderFactory.createTitledBorder("Results Log"));
        resultsArea.setEditable(false);
        resultsArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 13));
        panel.add(new JScrollPane(resultsArea), BorderLayout.CENTER);
        return panel;
    }

    private void updateInputFields() {
        boolean isRange = PRIMESUM.equals(jobTypeBox.getSelectedItem());
        rangePanel.setVisible(isRange);
        numbersPanel.setVisible(!isRange);
        revalidate();
        repaint();
    }

    // ---------------------------------------------------------------------
    // Actions
    // ---------------------------------------------------------------------

    private void onLoadCsv(ActionEvent e) {
        JFileChooser chooser = new JFileChooser(new File("."));
        if (chooser.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) {
            return;
        }
        File file = chooser.getSelectedFile();
        List<String> skipped = new ArrayList<>();
        try {
            List<Integer> numbers = CsvLoader.load(file, skipped);
            if (numbers.isEmpty()) {
                log("No numbers found in " + file.getName());
                return;
            }
            if (PRIMESUM.equals(jobTypeBox.getSelectedItem())) {
                if (numbers.size() < 2) {
                    log("For PRIMESUM the CSV must contain a start and an end value.");
                    return;
                }
                startField.setText(String.valueOf(numbers.get(0)));
                endField.setText(String.valueOf(numbers.get(1)));
                log("Loaded range " + numbers.get(0) + " to " + numbers.get(1) + " from " + file.getName());
            } else {
                numbersField.setText(numbers.stream().map(String::valueOf).collect(Collectors.joining(",")));
                log("Loaded " + numbers.size() + " numbers from " + file.getName());
            }
            if (!skipped.isEmpty()) {
                log("Skipped non-numeric values: " + skipped);
            }
        } catch (IOException ex) {
            log("Could not read CSV: " + ex.getMessage());
        }
    }

    private void onSubmit(ActionEvent e) {
        final Job<?> job;
        final String host;
        final int port;
        try {
            job = buildJob();
            host = hostField.getText().trim();
            port = parseInt(portField.getText(), "Port");
        } catch (IllegalArgumentException ex) {
            log("Invalid input - " + ex.getMessage());
            return;
        }

        final String tag = "[Job " + jobCounter.incrementAndGet() + "] ";
        log(tag + "Submitting " + job.describe());

        new SwingWorker<JobResult, Void>() {
            @Override
            protected JobResult doInBackground() throws Exception {
                return submitToCoordinator(host, port, job);
            }

            @Override
            protected void done() {
                try {
                    log(tag + get());
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    log(tag + "Interrupted");
                } catch (ExecutionException ex) {
                    log(tag + "Failed: " + rootMessage(ex));
                }
            }
        }.execute();
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    /** Builds the right Job subclass for the selected type. */
    private Job<?> buildJob() {
        String type = (String) jobTypeBox.getSelectedItem();
        if (PRIMESUM.equals(type)) {
            return new PrimeSumJob(parseInt(startField.getText(), "Start"),
                                   parseInt(endField.getText(), "End"));
        }
        List<Integer> numbers = parseNumbers(numbersField.getText());
        return MAX.equals(type) ? new MaxJob(numbers) : new PrimeCountJob(numbers);
    }

    private static JobResult submitToCoordinator(String host, int port, Job<?> job)
            throws RemoteException, NotBoundException {
        Registry registry = LocateRegistry.getRegistry(host, port);
        BootstrapService bootstrap = (BootstrapService) registry.lookup(Config.BOOTSTRAP_NAME);
        WorkerInfo coordinator = bootstrap.getCoordinator();
        if (coordinator == null) {
            throw new IllegalStateException(
                    "No coordinator yet - an election is in progress. Try again in a few seconds.");
        }
        return coordinator.getStub().submitJob(job);
    }

    private static int parseInt(String text, String field) {
        try {
            return Integer.parseInt(text.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(field + " must be a whole number.");
        }
    }

    private static List<Integer> parseNumbers(String text) {
        List<Integer> numbers = new ArrayList<>();
        for (String part : text.split(",")) {
            String value = part.trim();
            if (!value.isEmpty()) {
                numbers.add(parseInt(value, "'" + value + "'"));
            }
        }
        if (numbers.isEmpty()) {
            throw new IllegalArgumentException("Enter at least one number.");
        }
        return numbers;
    }

    private static String rootMessage(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause.getMessage();
    }

    private void log(String message) {
        resultsArea.append(message + "\n");
        resultsArea.setCaretPosition(resultsArea.getDocument().getLength());
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> new JobClientGUI().setVisible(true));
    }
}