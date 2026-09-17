package client.gui;

import client.parser.CsvDataParser;
import common.interfaces.CoordinatorService;
import common.models.JobRequest;
import common.models.JobResult;
import common.models.JobType;
import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JTextArea;
import javax.swing.JTextField;

//Java Swing GUI Frontend for client job submissions.
//Accepts manual data entry, PRIMESUM range (start, end), or loading data from CSV files.
//Supports concurrent execution/submission within a single client and across multiple client processes.

public class ClientGuiFrame extends JFrame {
    private static final long serialVersionUID = 1L;

    // GUI UI Components
    private JTextField coordinatorHostField;
    private JTextField coordinatorPortField;
    private JComboBox<JobType> jobTypeComboBox;
    private JTextArea manualInputArea; // For MAX and PRIMECOUNT numbers
    private JTextField rangeStartField; // For PRIMESUM start
    private JTextField rangeEndField; // For PRIMESUM end
    private JButton browseCsvButton;
    private JButton submitAsyncButton;
    private JTextArea resultsOutputArea;
    private JLabel statusLabel;

    // Dedicated thread pool supporting concurrent submissions within this single
    // client GUI.
    private final ExecutorService clientSubmissionPool = Executors.newCachedThreadPool();

    public ClientGuiFrame() {
        super("CS324 Distributed Computing Client");
        initComponents();
    }

    // Initializes GUI components, layouts, and event listeners.
    private void initComponents() {
        // Method stub: initialize Swing components, borders, panels, and attach
        // listeners
    }

    // Opens a file chooser dialog to load numbers from a CSV file.
    private void handleCsvBrowse() {
        // Method stub: open JFileChooser, call CsvDataParser.parseCsv, and populate
        // manualInputArea
    }

    // Submits a computation job concurrently to the coordinator to prevent UI
    // freezing.
    // Allows multiple concurrent submissions within this client process.
    private void handleAsyncJobSubmission() {
        // Method stub: read inputs, build JobRequest (list or range), dispatch via
        // clientSubmissionPool, update GUI on completion
    }

    // Locates the active coordinator remote service in the RMI registry.
    private CoordinatorService lookupCoordinator(String host, int port) {
        // Method stub: RMI registry lookup for CoordinatorService
        return null;
    }
}
