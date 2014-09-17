package bitronix.tm;

import bitronix.tm.journal.DiskJournal;
import bitronix.tm.journal.Journal;
import bitronix.tm.journal.MultiplexedJournal;
import bitronix.tm.journal.NullJournal;
import bitronix.tm.recovery.Recoverer;
import bitronix.tm.resource.ResourceLoader;
import bitronix.tm.timer.TaskScheduler;
import bitronix.tm.twopc.executor.AsyncExecutor;
import bitronix.tm.twopc.executor.Executor;
import bitronix.tm.twopc.executor.SyncExecutor;
import bitronix.tm.utils.ClassLoaderUtils;
import bitronix.tm.utils.DefaultExceptionAnalyzer;
import bitronix.tm.utils.ExceptionAnalyzer;
import bitronix.tm.utils.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Ilya Labun
 */
public class ServicesInstance {
    private final static Logger log = LoggerFactory.getLogger(ServicesInstance.class);

    private final String key;

    private final Lock transactionManagerLock = new ReentrantLock();
    private volatile BitronixTransactionManager transactionManager;

    private final AtomicReference<BitronixTransactionSynchronizationRegistry> transactionSynchronizationRegistryRef = new AtomicReference<BitronixTransactionSynchronizationRegistry>();
    private final AtomicReference<Configuration> configurationRef = new AtomicReference<Configuration>();
    private final AtomicReference<Journal> journalRef = new AtomicReference<Journal>();
    private final AtomicReference<TaskScheduler> taskSchedulerRef = new AtomicReference<TaskScheduler>();
    private final AtomicReference<ResourceLoader> resourceLoaderRef = new AtomicReference<ResourceLoader>();
    private final AtomicReference<Recoverer> recovererRef = new AtomicReference<Recoverer>();
    private final AtomicReference<Executor> executorRef = new AtomicReference<Executor>();
    private final AtomicReference<ExceptionAnalyzer> exceptionAnalyzerRef = new AtomicReference<ExceptionAnalyzer>();

    public ServicesInstance(String key) {
        this.key = key;
    }

    /**
     * Create an initialized transaction manager.
     * @return the transaction manager.
     */
    public BitronixTransactionManager getTransactionManager() {
        transactionManagerLock.lock();
        try {
            if (transactionManager == null) {
                transactionManager = new BitronixTransactionManager();
            }
            return transactionManager;
        } finally {
            transactionManagerLock.unlock();
        }
    }

    /**
     * Create the JTA 1.1 TransactionSynchronizationRegistry.
     * @return the TransactionSynchronizationRegistry.
     */
    public BitronixTransactionSynchronizationRegistry getTransactionSynchronizationRegistry() {
        BitronixTransactionSynchronizationRegistry transactionSynchronizationRegistry = transactionSynchronizationRegistryRef.get();
        if (transactionSynchronizationRegistry == null) {
            transactionSynchronizationRegistry = new BitronixTransactionSynchronizationRegistry();
            if (!transactionSynchronizationRegistryRef.compareAndSet(null, transactionSynchronizationRegistry)) {
                transactionSynchronizationRegistry = transactionSynchronizationRegistryRef.get();
            }
        }
        return transactionSynchronizationRegistry;
    }

    /**
     * Create the configuration of all the components of the transaction manager.
     * @return the global configuration.
     */
    public Configuration getConfiguration() {
        Configuration configuration = configurationRef.get();
        if (configuration == null) {
            configuration = new Configuration();
            if (!configurationRef.compareAndSet(null, configuration)) {
                configuration = configurationRef.get();
            }
        }
        return configuration;
    }

    /**
     * Create the transactions journal.
     * @return the transactions journal.
     */
    public Journal getJournal() {
        Journal journal = journalRef.get();
        if (journal == null) {
            String configuredJournal = getConfiguration().getJournal();
            if ("multiplexed".equals(configuredJournal)) {
                String primaryJournal = getConfiguration().getPrimaryJournal();
                String secondaryJournal = getConfiguration().getSecondaryJournal();

                journal = new MultiplexedJournal(
                        createJournal(primaryJournal, getConfiguration().getPrimaryDiskConfiguration()),
                        createJournal(secondaryJournal, getConfiguration().getSecondaryDiskConfiguration()));
            } else {
                journal = createJournal(configuredJournal, getConfiguration().getDiskConfiguration());
            }

            if (log.isDebugEnabled()) { log.debug("using journal " + journal); }
            if (!journalRef.compareAndSet(null, journal)) {
                journal = journalRef.get();
            }
        }
        return journal;
    }

    /**
     * Create the task scheduler.
     * @return the task scheduler.
     */
    public TaskScheduler getTaskScheduler() {
        TaskScheduler taskScheduler = taskSchedulerRef.get();
        if (taskScheduler == null) {
            taskScheduler = new TaskScheduler();
            if (!taskSchedulerRef.compareAndSet(null, taskScheduler)) {
                taskScheduler = taskSchedulerRef.get();
            } else {
                taskScheduler.start();
            }
        }
        return taskScheduler;
    }

    /**
     * Create the resource loader.
     * @return the resource loader.
     */
    public ResourceLoader getResourceLoader() {
        ResourceLoader resourceLoader = resourceLoaderRef.get();
        if (resourceLoader == null) {
            resourceLoader = new ResourceLoader();
            if (!resourceLoaderRef.compareAndSet(null, resourceLoader)) {
                resourceLoader = resourceLoaderRef.get();
            }
        }
        return resourceLoader;
    }

    /**
     * Create the transaction recoverer.
     * @return the transaction recoverer.
     */
    public Recoverer getRecoverer() {
        Recoverer recoverer = recovererRef.get();
        if (recoverer == null) {
            recoverer = new Recoverer();
            if (!recovererRef.compareAndSet(null, recoverer)) {
                recoverer = recovererRef.get();
            }
        }
        return recoverer;
    }

    /**
     * Create the 2PC executor.
     * @return the 2PC executor.
     */
    public Executor getExecutor() {
        Executor executor = executorRef.get();
        if (executor == null) {
            if (getConfiguration().isAsynchronous2Pc()) {
                if (log.isDebugEnabled()) { log.debug("using AsyncExecutor"); }
                executor = new AsyncExecutor();
            } else {
                if (log.isDebugEnabled()) { log.debug("using SyncExecutor"); }
                executor = new SyncExecutor();
            }
            if (!executorRef.compareAndSet(null, executor)) {
                executor.shutdown();
                executor = executorRef.get();
            }
        }
        return executor;
    }

    /**
     * Create the exception analyzer.
     * @return the exception analyzer.
     */
   public ExceptionAnalyzer getExceptionAnalyzer() {
        ExceptionAnalyzer analyzer = exceptionAnalyzerRef.get();
        if (analyzer == null) {
            String exceptionAnalyzerName = getConfiguration().getExceptionAnalyzer();
            analyzer = new DefaultExceptionAnalyzer();
            if (exceptionAnalyzerName != null) {
                try {
                    analyzer = (ExceptionAnalyzer) ClassLoaderUtils.loadClass(exceptionAnalyzerName).newInstance();
                } catch (Exception ex) {
                    log.warn("failed to initialize custom exception analyzer, using default one instead", ex);
                }
            }
            if (!exceptionAnalyzerRef.compareAndSet(null, analyzer)) {
                analyzer.shutdown();
                analyzer = exceptionAnalyzerRef.get();
            }
        }
        return analyzer;
    }

    /**
     * Check if the transaction manager has started.
     * @return true if the transaction manager has started.
     */
    public boolean isTransactionManagerRunning() {
        return transactionManager != null;
    }

    /**
     * Check if the task scheduler has started.
     * @return true if the task scheduler has started.
     */
    public boolean isTaskSchedulerRunning() {
        return taskSchedulerRef.get() != null;
    }

    public String getKey() {
        return key;
    }

    /**
     * Clear services references. Called at the end of the shutdown procedure.
     */
    protected synchronized void clear() {
        transactionManager = null;

        transactionSynchronizationRegistryRef.set(null);
        configurationRef.set(null);
        journalRef.set(null);
        taskSchedulerRef.set(null);
        resourceLoaderRef.set(null);
        recovererRef.set(null);
        executorRef.set(null);
        exceptionAnalyzerRef.set(null);
    }

    private static Journal createJournal(String configuredJournal, DiskJournalConfiguration diskJournalConfiguration) {
        if ("null".equals(configuredJournal) || null == configuredJournal) {
            return new NullJournal();
        } else if ("disk".equals(configuredJournal)) {
            return new DiskJournal(diskJournalConfiguration);
        } else {
            try {
                Class<?> clazz = ClassLoaderUtils.loadClass(configuredJournal);
                return (Journal) clazz.newInstance();
            } catch (Exception ex) {
                throw new InitializationException("invalid journal implementation '" + configuredJournal + "'", ex);
            }
        }
    }
}
