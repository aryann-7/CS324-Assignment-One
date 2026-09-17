package worker.computation;

import common.models.JobResult;
import common.models.JobTask;
import java.util.concurrent.Callable;

/**
 * Multithreading runner responsible for executing partitioned job tasks
 * using Java threads across the three distinct job types:
 * 1. MAX(numbers): Largest value from an unsorted list.
 * 2. PRIMESUM(start, end): Sum of primes within range [start, end].
 * 3. PRIMECOUNT(numbers): Count of primes in an unsorted list.
 */
public class JobRunner implements Callable<JobResult> {

    private final JobTask task;

    public JobRunner(JobTask task) {
        this.task = task;
    }

    /**
     * Executes the task on a Java thread and returns the computed result.
     */
    @Override
    public JobResult call() throws Exception {
        // Method stub: dispatch execution based on task.getJobType()
        return null;
    }

    /**
     * Computes the maximum value from an unsorted list of numbers.
     */
    private JobResult computeMax() {
        // Method stub: find largest value in task.getDataChunk()
        return null;
    }

    /**
     * Computes the sum of prime numbers within a specified range [start, end].
     */
    private JobResult computePrimeSum() {
        // Method stub: compute sum of primes from task.getRangeStart() to task.getRangeEnd()
        return null;
    }

    /**
     * Computes the total count of prime numbers in an unsorted list of numbers.
     */
    private JobResult computePrimeCount() {
        // Method stub: count primes in task.getDataChunk()
        return null;
    }

    /**
     * Helper to verify if a number is prime.
     */
    private boolean isPrime(long n) {
        // Method stub: primality test
        return false;
    }
}

