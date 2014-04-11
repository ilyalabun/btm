package bitronix.tm.journal;

import bitronix.tm.utils.Uid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * This journal provides successful recovery
 * in case of either primary or secondary log corruption.
 * Holds 2 underlying journals, all changes are made simultaneously to both.
 *
 * @author Ilya Labun
 */
public class MultiplexedJournal implements Journal {

    private final static Logger log = LoggerFactory.getLogger(MultiplexedJournal.class);

    private ExecutorService executor = Executors.newFixedThreadPool(2);  //TODO choose proper pool size and/or type

    /**
     * Primary journal
     */
    private final Journal primary;

    /**
     * Secondary journal
     */
    private final Journal secondary;

    public MultiplexedJournal(Journal primary, Journal secondary) {
        this.primary = primary;
        this.secondary = secondary;
    }

    public void log(final int status, final Uid gtrid, final Set<String> uniqueNames) throws IOException {
        executeInParallel(new VoidParallelJournalTask() {
            public void voidDoWithPrimary() throws IOException {
                primary.log(status, gtrid, uniqueNames);
            }

            public void voidDoWithSecondary() throws IOException {
                secondary.log(status, gtrid, uniqueNames);
            }
        });
    }

    public void open() throws IOException {
        synchronized (executor) {
            if (executor.isShutdown())
                executor = Executors.newFixedThreadPool(2);
        }


        executeInParallel(new VoidParallelJournalTask() {
             public void voidDoWithPrimary() throws IOException {
                 primary.open();
             }

             public void voidDoWithSecondary() throws IOException {
                 secondary.open();
             }
         });
    }

    public synchronized void close() throws IOException {
        if (executor.isShutdown()) {
            return;
        }

        executeInParallel(new VoidParallelJournalTask() {
            @Override
            protected void voidDoWithPrimary() throws IOException {
                primary.close();
            }

            @Override
            protected void voidDoWithSecondary() throws IOException {
                secondary.close();
            }
        });
    }

    public synchronized void shutdown() {
        if (executor.isShutdown())
            return;

        try {
            executeInParallel(new VoidParallelJournalTask() {
                @Override
                protected void voidDoWithPrimary() throws IOException {
                    primary.shutdown();
                }

                @Override
                protected void voidDoWithSecondary() throws IOException {
                    secondary.shutdown();
                }
            });
        } catch (IOException e) {
            log.error("error shutting down multiplexed journal. Transaction log integrity could be compromised!", e);
        } finally {
            try {
                shutdownExecutor();
            } catch (IOException e) {
                log.error("error shutting down executor", e);
            }
        }
    }

    private void shutdownExecutor() throws IOException {
        executor.shutdown();
        try {
            executor.awaitTermination(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Executor shutdown terminated", e);
        }
    }

    public void force() throws IOException {
        executeInParallel(new VoidParallelJournalTask() {
             public void voidDoWithPrimary() throws IOException {
                 primary.force();
             }

             public void voidDoWithSecondary() throws IOException {
                 secondary.force();
             }
         });
    }

    public Map<Uid, JournalRecord> collectDanglingRecords() throws IOException {
        return collectAllRecords().getDanglingRecords();
    }

    public JournalRecords collectAllRecords() throws IOException {
        final ExecutionResult<JournalRecords> executionResults = executeInParallelDontThrow(new ParallelJournalTask<JournalRecords>() {
            public JournalRecords doWithPrimary() throws IOException {
                return primary.collectAllRecords();
            }

            public JournalRecords doWithSecondary() throws IOException {
                return secondary.collectAllRecords();
            }
        });

        final Throwable primaryThrowable = executionResults.getPrimaryThrowable();
        throwIfUnchecked(primaryThrowable);

        final Throwable secondaryThrowable = executionResults.getSecondaryThrowable();
        throwIfUnchecked(secondaryThrowable);

        if (primaryThrowable != null && secondaryThrowable != null) {
            throw new IOException(String.format("Failed to collect dangling records because both journals failed.\nPrimary throwable:\n %s\nSecondary throwable:\n%s",
                    getStackTrace(primaryThrowable), getStackTrace(secondaryThrowable)));
        }

        final JournalRecords primaryResult = executionResults.getPrimaryResult();
        final JournalRecords secondaryResult = executionResults.getSecondaryResult();

        if (primaryThrowable == null && secondaryThrowable != null) {
            log.warn("Failed to collect dangling records from secondary journal", secondaryThrowable);
            return primaryResult;
        }

        if (primaryThrowable != null && secondaryThrowable == null) {
            log.warn("Failed to collect dangling records from primary journal", secondaryThrowable);
            return secondaryResult;
        }

        return mergeResults(primaryResult, secondaryResult);
    }

    private JournalRecords mergeResults(JournalRecords primaryResults, JournalRecords secondaryResults) {
        Map<Uid, JournalRecord> committedRecords = new HashMap<Uid, JournalRecord>();
        committedRecords.putAll(primaryResults.getCommittedRecords());
        committedRecords.putAll(secondaryResults.getCommittedRecords());

        Map<Uid, JournalRecord> danglingRecords = new HashMap<Uid, JournalRecord>();
        danglingRecords.putAll(removeCommittedRecords(primaryResults.getDanglingRecords(), secondaryResults.getCommittedRecords()));
        danglingRecords.putAll(removeCommittedRecords(secondaryResults.getDanglingRecords(), primaryResults.getCommittedRecords()));

        return new JournalRecords(danglingRecords, committedRecords);
    }

    /**
     * Matches dangling records from journal A with committed records from journal B
     * and removes form dangling records those which are already committed.
     *
     * @param journalADangling dangling records from journal A
     * @param journalBCommitted committed records from journal B
     * @return <code>journalADangling</code> with removed committed records.
     */
    private Map<Uid, JournalRecord> removeCommittedRecords(Map<Uid, JournalRecord> journalADangling, Map<Uid, JournalRecord> journalBCommitted) {
        Set<Uid> danglingUids = new HashSet<Uid>(journalADangling.keySet());

        for (Uid uid: danglingUids) {
            if (!journalBCommitted.containsKey(uid)) //there's no corresponding committed records for transaction
                continue;

            JournalRecord danglingRecord = journalADangling.get(uid);
            JournalRecord committedRecord = journalBCommitted.get(uid);

            final Set<String> danglingNames = new HashSet<String>(danglingRecord.getUniqueNames());
            final Set<String> committedNames = new HashSet<String>(committedRecord.getUniqueNames());

            danglingNames.removeAll(committedNames);
            if (danglingNames.isEmpty()) {
                journalADangling.remove(uid);
            } else {
                journalADangling.put(uid, new TransactionLogRecord(danglingRecord.getStatus(), danglingRecord.getGtrid(), danglingNames));
            }
        }

        return journalADangling;
    }


    private String getStackTrace(Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }


    private void throwIfUnchecked(Throwable t) {
        if (t instanceof RuntimeException)
            throw (RuntimeException)t;

        if (t instanceof Error)
            throw (Error)t;
    }

    private <T> ExecutionResult<T> executeInParallel(final ParallelJournalTask<T> task) throws IOException {
        final Future<T> primaryFuture = submitPrimary(task);
        final Future<T> secondaryFuture = submitSecondary(task);

        try {
            return new ExecutionResult<T>(primaryFuture.get(), secondaryFuture.get());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (ExecutionException e) {
            launderThrowable(e);   //always throws exception
        }

        //dead code indeed
        return null;
    }

    private <T> ExecutionResult<T> executeInParallelDontThrow(final ParallelJournalTask<T> task) throws IOException {
        ExecutionResult<T> result = new ExecutionResult<T>();
        final Future<T> primaryFuture = submitPrimary(task);
        final Future<T> secondaryFuture = submitSecondary(task);

        try {
            final T primaryResult = primaryFuture.get();
            result.setPrimaryResult(primaryResult);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (ExecutionException e) {
            result.setPrimaryThrowable(e.getCause());
        }

        try {
            final T secondaryResult = secondaryFuture.get();
            result.setSecondaryResult(secondaryResult);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (ExecutionException e) {
            result.setSecondaryThrowable(e.getCause());
        }

        return result;
    }


    private <T> Future<T> submitPrimary(final ParallelJournalTask<T> task) {
        return executor.submit(new Callable<T>() {
                public T call() throws Exception {
                    return task.doWithPrimary();
                }
            });
    }

    private <T> Future<T> submitSecondary(final ParallelJournalTask<T> task) {
        return executor.submit(new Callable<T>() {
                public T call() throws Exception {
                    return task.doWithSecondary();
                }
            });
    }



    private void launderThrowable(ExecutionException e) throws IOException {
        final Throwable cause = e.getCause();
        if (cause instanceof IOException)
            throw (IOException) cause;

        //if exception is not IOException -- it is definetely RuntimeException or Error,
        //throw it forward
        throwIfUnchecked(e);
    }


    private interface ParallelJournalTask<T> {
        T doWithPrimary() throws IOException;

        T doWithSecondary() throws IOException;
    }

    private abstract static class VoidParallelJournalTask implements ParallelJournalTask<Void> {
        public Void doWithPrimary() throws IOException {
            voidDoWithPrimary();
            return null;
        }

        public Void doWithSecondary() throws IOException {
            voidDoWithSecondary();
            return null;
        }

        protected abstract void voidDoWithPrimary() throws IOException;
        protected abstract void voidDoWithSecondary() throws IOException;
    }

    private final static class ExecutionResult<T> {
        private T primaryResult;

        private Throwable primaryThrowable;

        private T secondaryResult;

        private Throwable secondaryThrowable;

        private ExecutionResult() {
        }

        private ExecutionResult(T primaryResult, T secondaryResult) {
            this.primaryResult = primaryResult;
            this.secondaryResult = secondaryResult;
        }

        private T getPrimaryResult() {
            return primaryResult;
        }

        private void setPrimaryResult(T primaryResult) {
            this.primaryResult = primaryResult;
        }

        private Throwable getPrimaryThrowable() {
            return primaryThrowable;
        }

        private void setPrimaryThrowable(Throwable primaryThrowable) {
            this.primaryThrowable = primaryThrowable;
        }

        private T getSecondaryResult() {
            return secondaryResult;
        }

        private void setSecondaryResult(T secondaryResult) {
            this.secondaryResult = secondaryResult;
        }

        private Throwable getSecondaryThrowable() {
            return secondaryThrowable;
        }

        private void setSecondaryThrowable(Throwable secondaryThrowable) {
            this.secondaryThrowable = secondaryThrowable;
        }
    }
}
