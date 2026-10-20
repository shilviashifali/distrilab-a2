package client;

import grpc.JobProgress;
import grpc.WorkerProgress;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Font;
import java.awt.Frame;
import javax.swing.BorderFactory;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableCellRenderer;

/** Worker-level breakdown of one job, opened by clicking its progress bar. Updates live. */
public class BreakdownDialog extends JDialog {

    private static final long serialVersionUID = 1L;
    private static final String[] COLUMNS = {"Task", "Worker", "Status", "Items done", "Progress"};

    private final JLabel summary = new JLabel(" ");
    private final DefaultTableModel model = new DefaultTableModel(COLUMNS, 0) {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isCellEditable(int row, int column) {
            return false;
        }
    };

    public BreakdownDialog(Frame owner, String title) {
        super(owner, title, false);
        JTable table = new JTable(model);
        table.setRowHeight(24);
        table.setFillsViewportHeight(true);
        table.getColumnModel().getColumn(2).setCellRenderer(new StatusRenderer());
        table.getColumnModel().getColumn(4).setCellRenderer(new ProgressRenderer());
        table.getColumnModel().getColumn(3).setPreferredWidth(190);
        table.getColumnModel().getColumn(4).setPreferredWidth(170);

        summary.setBorder(BorderFactory.createEmptyBorder(4, 2, 8, 2));
        JPanel content = new JPanel(new BorderLayout());
        content.setBorder(BorderFactory.createEmptyBorder(10, 12, 12, 12));
        content.add(summary, BorderLayout.NORTH);
        content.add(new JScrollPane(table), BorderLayout.CENTER);
        setContentPane(content);
        setSize(760, 380);
        setLocationRelativeTo(owner);
    }

    /** Replaces the table with the latest progress message. */
    public void update(JobProgress p) {
        summary.setText(String.format("<html>Coordinator: <b>Worker-%d</b> &nbsp;&nbsp; Overall: <b>%.1f%%</b>%s</html>",
                p.getCoordinatorId(), p.getPercent(),
                p.getFinished() ? " &nbsp;&nbsp; Result: <b>" + JobClientGUI.fmt(p.getResult()) + "</b>" : ""));
        model.setRowCount(0);
        for (WorkerProgress w : p.getWorkersList()) {
            String task = w.getTaskId().substring(w.getTaskId().lastIndexOf('-') + 1);
            int percent = w.getItemsTotal() == 0 ? 0 : (int) Math.round(w.getItemsDone() * 100.0 / w.getItemsTotal());
            model.addRow(new Object[]{
                "Task " + task,
                w.getWorkerId() > 0 ? "Worker-" + w.getWorkerId() : "-",
                w.getStatus(),
                JobClientGUI.fmt(w.getItemsDone()) + " / " + JobClientGUI.fmt(w.getItemsTotal()),
                percent
            });
        }
    }

    /** Colours the status: DONE green, SLOW/REASSIGNED orange, WAITING grey. */
    private static final class StatusRenderer extends DefaultTableCellRenderer {
        private static final long serialVersionUID = 1L;

        @Override
        public Component getTableCellRendererComponent(JTable t, Object value, boolean selected,
                                                       boolean focus, int row, int col) {
            super.getTableCellRendererComponent(t, value, selected, focus, row, col);
            String s = String.valueOf(value);
            setFont(getFont().deriveFont(Font.BOLD));
            Color c = switch (s) {
                case "DONE" -> JobClientGUI.GREEN;
                case "SLOW", "REASSIGNED" -> JobClientGUI.ORANGE;
                case "RUNNING" -> new Color(0x1565C0);
                default -> JobClientGUI.GREY;
            };
            if (!selected) {
                setForeground(c);
            }
            return this;
        }
    }

    /** Draws the per-task percentage as a small progress bar. */
    private static final class ProgressRenderer extends JProgressBar implements TableCellRenderer {
        private static final long serialVersionUID = 1L;

        ProgressRenderer() {
            super(0, 100);
            setStringPainted(true);
        }

        @Override
        public Component getTableCellRendererComponent(JTable t, Object value, boolean selected,
                                                       boolean focus, int row, int col) {
            int v = value instanceof Integer i ? i : 0;
            setValue(v);
            setString(v + "%");
            return this;
        }
    }
}
