package bitronix.tm;

import bitronix.tm.utils.ConfigurationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
* @author Ilya Labun
*/
public class DiskJournalConfiguration implements PropertiesPack {

    private static Logger log = LoggerFactory.getLogger(DiskJournalConfiguration.class);

    private volatile String logPart1Filename;
    private volatile String logPart2Filename;
    private volatile boolean forcedWriteEnabled;
    private volatile boolean forceBatchingEnabled;
    private volatile int maxLogSizeInMb;
    private volatile boolean filterLogStatus;
    private volatile boolean skipCorruptedLogs;
    private Configuration configuration;

    public DiskJournalConfiguration(Configuration configuration, Properties properties, String prefix, String defaultPart1Name, String defaultPart2Name) {
        this.configuration = configuration;
        logPart1Filename = ConfigurationUtils.getString(properties, prefix + ".logPart1Filename", defaultPart1Name);
        logPart2Filename = ConfigurationUtils.getString(properties, prefix + ".logPart2Filename", defaultPart2Name);
        forcedWriteEnabled = ConfigurationUtils.getBoolean(properties, prefix + ".forcedWriteEnabled", true);
        forceBatchingEnabled = ConfigurationUtils.getBoolean(properties, prefix + ".forceBatchingEnabled", true);
        maxLogSizeInMb = ConfigurationUtils.getInt(properties, prefix + ".maxLogSize", 2);
        filterLogStatus = ConfigurationUtils.getBoolean(properties, prefix + ".filterLogStatus", false);
        skipCorruptedLogs = ConfigurationUtils.getBoolean(properties, prefix + ".skipCorruptedLogs", false);
    }

    /**
     * Get the journal fragment file 1 name.
     * <p>Property name:<br/><b>bitronix.tm.journal.disk.logPart1Filename -</b> <i>(defaults to btm1.tlog)</i></p>
     *
     * @return the journal fragment file 1 name.
     */
    public String getLogPart1Filename() {
        return logPart1Filename;
    }

    /**
     * Set the journal fragment file 1 name.
     *
     * @param logPart1Filename the journal fragment file 1 name.
     * @return this.
     * @see #getLogPart1Filename()
     */
    public Configuration setLogPart1Filename(String logPart1Filename) {
        ConfigurationUtils.checkNotStarted();
        this.logPart1Filename = logPart1Filename;
        return configuration;
    }

    /**
     * Get the journal fragment file 2 name.
     * <p>Property name:<br/><b>bitronix.tm.journal.disk.logPart2Filename -</b> <i>(defaults to btm2.tlog)</i></p>
     *
     * @return the journal fragment file 2 name.
     */
    public String getLogPart2Filename() {
        return logPart2Filename;
    }

    /**
     * Set the journal fragment file 2 name.
     *
     * @param logPart2Filename the journal fragment file 2 name.
     * @return this.
     * @see #getLogPart2Filename()
     */
    public Configuration setLogPart2Filename(String logPart2Filename) {
        ConfigurationUtils.checkNotStarted();
        this.logPart2Filename = logPart2Filename;
        return configuration;
    }

    /**
     * Are logs forced to disk?  Do not set to false in production since without disk force, integrity is not
     * guaranteed.
     * <p>Property name:<br/><b>bitronix.tm.journal.disk.forcedWriteEnabled -</b> <i>(defaults to true)</i></p>
     *
     * @return true if logs are forced to disk, false otherwise.
     */
    public boolean isForcedWriteEnabled() {
        return forcedWriteEnabled;
    }

    /**
     * Set if logs are forced to disk.  Do not set to false in production since without disk force, integrity is not
     * guaranteed.
     *
     * @param forcedWriteEnabled true if logs should be forced to disk, false otherwise.
     * @return this.
     * @see #isForcedWriteEnabled()
     */
    public Configuration setForcedWriteEnabled(boolean forcedWriteEnabled) {
        ConfigurationUtils.checkNotStarted();
        this.forcedWriteEnabled = forcedWriteEnabled;
        return configuration;
    }

    /**
     * Are disk forces batched? Disabling batching can seriously lower the transaction manager's throughput.
     * <p>Property name:<br/><b>bitronix.tm.journal.disk.forceBatchingEnabled -</b> <i>(defaults to true)</i></p>
     *
     * @return true if disk forces are batched, false otherwise.
     */
    public boolean isForceBatchingEnabled() {
        return forceBatchingEnabled;
    }

    /**
     * Set if disk forces are batched. Disabling batching can seriously lower the transaction manager's throughput.
     *
     * @param forceBatchingEnabled true if disk forces are batched, false otherwise.
     * @return this.
     * @see #isForceBatchingEnabled()
     */
    public Configuration setForceBatchingEnabled(boolean forceBatchingEnabled) {
        ConfigurationUtils.checkNotStarted();
        log.warn("forceBatchingEnabled is not longer used");
        this.forceBatchingEnabled = forceBatchingEnabled;
        return configuration;
    }

    /**
     * Maximum size in megabytes of the journal fragments. Larger logs allow transactions to stay longer in-doubt but
     * the TM pauses longer when a fragment is full.
     * <p>Property name:<br/><b>bitronix.tm.journal.disk.maxLogSize -</b> <i>(defaults to 2)</i></p>
     *
     * @return the maximum size in megabytes of the journal fragments.
     */
    public int getMaxLogSizeInMb() {
        return maxLogSizeInMb;
    }

    /**
     * Set the Maximum size in megabytes of the journal fragments. Larger logs allow transactions to stay longer
     * in-doubt but the TM pauses longer when a fragment is full.
     *
     * @param maxLogSizeInMb the maximum size in megabytes of the journal fragments.
     * @return this.
     * @see #getMaxLogSizeInMb()
     */
    public Configuration setMaxLogSizeInMb(int maxLogSizeInMb) {
        ConfigurationUtils.checkNotStarted();
        this.maxLogSizeInMb = maxLogSizeInMb;
        return configuration;
    }

    /**
     * Should only mandatory logs be written? Enabling this parameter lowers space usage of the fragments but makes
     * debugging more complex.
     * <p>Property name:<br/><b>bitronix.tm.journal.disk.filterLogStatus -</b> <i>(defaults to false)</i></p>
     *
     * @return true if only mandatory logs should be written.
     */
    public boolean isFilterLogStatus() {
        return filterLogStatus;
    }

    /**
     * Set if only mandatory logs should be written. Enabling this parameter lowers space usage of the fragments but
     * makes debugging more complex.
     *
     * @param filterLogStatus true if only mandatory logs should be written.
     * @return this.
     * @see #isFilterLogStatus()
     */
    public Configuration setFilterLogStatus(boolean filterLogStatus) {
        ConfigurationUtils.checkNotStarted();
        this.filterLogStatus = filterLogStatus;
        return configuration;
    }

    /**
     * Should corrupted logs be skipped?
     * <p>Property name:<br/><b>bitronix.tm.journal.disk.skipCorruptedLogs -</b> <i>(defaults to false)</i></p>
     *
     * @return true if corrupted logs should be skipped.
     */
    public boolean isSkipCorruptedLogs() {
        return skipCorruptedLogs;
    }

    /**
     * Set if corrupted logs should be skipped.
     *
     * @param skipCorruptedLogs true if corrupted logs should be skipped.
     * @return this.
     * @see #isSkipCorruptedLogs()
     */
    public Configuration setSkipCorruptedLogs(boolean skipCorruptedLogs) {
        ConfigurationUtils.checkNotStarted();
        this.skipCorruptedLogs = skipCorruptedLogs;
        return configuration;
    }



}
