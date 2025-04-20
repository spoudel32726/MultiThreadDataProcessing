import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.*;

public class DataProcessingSystem {
    private static final Logger LOGGER = Logger.getLogger(DataProcessingSystem.class.getName());
    private final BlockingQueue<String> taskQueue;
    private final List<String> results;
    private final ExecutorService executorService;
    private final int numWorkers;

    public DataProcessingSystem(int numWorkers, int numTasks) {
        this.numWorkers = numWorkers;
        this.taskQueue = new ArrayBlockingQueue<>(100);
        this.results = new CopyOnWriteArrayList<>();
        this.executorService = Executors.newFixedThreadPool(numWorkers);
        initializeTasks(numTasks);
    }

    private void initializeTasks(int numTasks) {
        for (int i = 1; i <= numTasks; i++) {
            try {
                taskQueue.put("Task-" + i);
            } catch (InterruptedException e) {
                LOGGER.severe("Failed to add task: " + e.getMessage());
                Thread.currentThread().interrupt();
            }
        }
    }

    public void startProcessing() {
        for (int i = 0; i < numWorkers; i++) {
            executorService.submit(() -> {
                String workerName = Thread.currentThread().getName();
                LOGGER.info(workerName + " started.");
                while (!taskQueue.isEmpty()) {
                    try {
                        String task = taskQueue.take();
                        String result = processTask(task);
                        results.add(result);
                    } catch (InterruptedException e) {
                        LOGGER.severe(workerName + " interrupted: " + e.getMessage());
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        LOGGER.severe(workerName + " error: " + e.getMessage());
                    }
                }
                LOGGER.info(workerName + " completed.");
            });
        }
    }

    private String processTask(String task) {
        try {
            // Simulate computational work
            Thread.sleep((int) (Math.random() * 1000));
            return "Processed: " + task;
        } catch (InterruptedException e) {
            LOGGER.severe("Task processing interrupted: " + e.getMessage());
            Thread.currentThread().interrupt();
            return "Failed: " + task;
        }
    }

    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
                LOGGER.warning("Executor did not terminate gracefully.");
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            LOGGER.severe("Shutdown interrupted: " + e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    public List<String> getResults() {
        return new ArrayList<>(results);
    }

    public static void main(String[] args) {
        // Configure logger
        try {
            FileHandler fileHandler = new FileHandler("processing.log", true);
            fileHandler.setFormatter(new SimpleFormatter());
            LOGGER.addHandler(fileHandler);
        } catch (IOException e) {
            LOGGER.severe("Failed to configure logger: " + e.getMessage());
        }

        DataProcessingSystem system = new DataProcessingSystem(4, 20);
        system.startProcessing();
        system.shutdown();
        system.getResults().forEach(System.out::println);
    }
}