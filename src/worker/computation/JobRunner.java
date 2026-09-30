package worker.computation;

import common.models.JobResult;
import common.models.JobTask;
import java.util.concurrent.Callable;

public class JobRunner implements Callable<JobResult> {
    private final JobTask task;

    public JobRunner(JobTask task) {
        this.task = task;
    }

    // Executes the task on a Java thread and returns the computed result.
    @Override
    public JobResult call() throws Exception {
        long startTime = System.currentTimeMillis();
        JobResult result = new JobResult();
        result.setJobId(task.getJobId());
        result.setTaskId(task.getTaskId());

        try {
            if (task.getJobType() == null) {
                result.setSuccess(false);
                result.setErrorMessage("JobType cannot be null");
                result.setExecutionTimeMs(System.currentTimeMillis() - startTime);
                return result;
            }

            switch (task.getJobType()) {
                case MAX:
                    result = computeMax();
                    break;
                case PRIMESUM:
                    result = computePrimeSum();
                    break;
                case PRIMECOUNT:
                    result = computePrimeCount();
                    break;
                default:
                    result.setSuccess(false);
                    result.setErrorMessage("Unsupported JobType: " + task.getJobType());
                    break;
            }
        } catch (Exception e) {
            result.setSuccess(false);
            result.setErrorMessage("Execution error: " + e.getMessage());
        }

        result.setJobId(task.getJobId());
        result.setTaskId(task.getTaskId());
        result.setExecutionTimeMs(System.currentTimeMillis() - startTime);
        return result;
    }

    // Computes the maximum value from an unsorted list of numbers.
    private JobResult computeMax() {
        JobResult result = new JobResult();
        if (task.getDataChunk() == null || task.getDataChunk().isEmpty()) {
            result.setSuccess(false);
            result.setErrorMessage("Data chunk is empty or null for MAX task");
            return result;
        }

        long max = Long.MIN_VALUE;
        for (Long num : task.getDataChunk()) {
            if (num != null && num > max) {
                max = num;
            }
        }

        result.setSuccess(true);
        result.setResultValue(max);
        return result;
    }

    // Computes the sum of prime numbers within a specified range [start, end]
    // using a cache-friendly Segmented Sieve of Eratosthenes.
    private JobResult computePrimeSum() {
        JobResult result = new JobResult();
        long start = task.getRangeStart();
        long end = task.getRangeEnd();

        if (start > end) {
            long temp = start;
            start = end;
            end = temp;
        }

        if (end < 2) {
            result.setSuccess(true);
            result.setResultValue(0L);
            return result;
        }

        long actualStart = Math.max(2, start);

        // Precompute base primes up to sqrt(end) using simple sieve
        int limit = (int) Math.sqrt(end);
        java.util.List<Integer> basePrimes = simpleSieve(limit);

        // 1MB cache-friendly block size
        final int BLOCK_SIZE = 1_000_000;
        boolean[] isPrimeBlock = new boolean[BLOCK_SIZE];
        long sum = 0;

        for (long low = actualStart; low <= end; low += BLOCK_SIZE) {
            long high = Math.min(low + BLOCK_SIZE - 1, end);
            int blockLen = (int) (high - low + 1);

            java.util.Arrays.fill(isPrimeBlock, 0, blockLen, true);

            for (int p : basePrimes) {
                // Find minimum multiple of p in range [low, high]
                long firstMultiple = ((low + p - 1) / p) * p;
                if (firstMultiple < (long) p * p) {
                    firstMultiple = (long) p * p;
                }

                for (long j = firstMultiple; j <= high; j += p) {
                    isPrimeBlock[(int) (j - low)] = false;
                }
            }

            for (int i = 0; i < blockLen; i++) {
                if (isPrimeBlock[i]) {
                    sum += (low + i);
                }
            }
        }

        result.setSuccess(true);
        result.setResultValue(sum);
        return result;
    }

    // Helper: Simple sieve to generate small primes up to sqrt(end)
    private java.util.List<Integer> simpleSieve(int limit) {
        java.util.List<Integer> primes = new java.util.ArrayList<>();
        if (limit < 2) {
            return primes;
        }

        boolean[] composite = new boolean[limit + 1];
        for (int p = 2; (long) p * p <= limit; p++) {
            if (!composite[p]) {
                for (int j = p * p; j <= limit; j += p) {
                    composite[j] = true;
                }
            }
        }

        for (int p = 2; p <= limit; p++) {
            if (!composite[p]) {
                primes.add(p);
            }
        }
        return primes;
    }

    // Computes the total count of prime numbers in an unsorted list of numbers.
    private JobResult computePrimeCount() {
        JobResult result = new JobResult();
        if (task.getDataChunk() == null) {
            result.setSuccess(false);
            result.setErrorMessage("Data chunk is null for PRIMECOUNT task");
            return result;
        }

        long count = 0;
        for (Long num : task.getDataChunk()) {
            if (num != null && isPrime(num)) {
                count++;
            }
        }

        result.setSuccess(true);
        result.setResultValue(count);
        return result;
    }

    // Helper to verify if a number is prime.
    private boolean isPrime(long n) {
        if (n <= 1) {
            return false;
        }
        if (n <= 3) {
            return true;
        }
        if (n % 2 == 0 || n % 3 == 0) {
            return false;
        }
        for (long i = 5; i * i <= n; i += 6) {
            if (n % i == 0 || n % (i + 2) == 0) {
                return false;
            }
        }
        return true;
    }
}