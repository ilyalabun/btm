/*
 * Copyright (C) 2006-2013 Bitronix Software (http://www.bitronix.be)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bitronix.tm;

import bitronix.tm.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Configuration repository of the transaction manager. You can set configurable values either via the properties file
 * or by setting properties of the {@link Configuration} object.
 * Once the transaction manager has started it is not possible to change the configuration: all calls to setters will
 * throw a {@link IllegalStateException}.
 * <p>The configuration filename must be specified with the <code>bitronix.tm.configuration</code> system property.</p>
 * <p>The default settings are good enough for running in a test environment but certainly not for production usage.
 * Also, all properties are reset to their default value after the transaction manager has shut down.</p>
 * <p>All those properties can refer to other defined ones or to system properties using the Ant notation:
 * <code>${some.property.name}</code>.</p>
 *
 * @author Ludovic Orban
 */
public class Configuration implements Service {

    private final static Logger log = LoggerFactory.getLogger(Configuration.class);

    private final static int MAX_SERVER_ID_LENGTH = 51;
    private final static String SERVER_ID_CHARSET_NAME = "US-ASCII";

    private volatile String serverId;
    private final AtomicReference<byte[]> serverIdArray = new AtomicReference<byte[]>();

    private volatile DiskJournalConfiguration diskConfiguration;

    private volatile DiskJournalConfiguration primaryDiskConfiguration;
    private volatile DiskJournalConfiguration secondaryDiskConfiguration;

    private volatile boolean failOnRecordCorruption;

    private volatile boolean asynchronous2Pc;
    private volatile boolean warnAboutZeroResourceTransaction;
    private volatile boolean debugZeroResourceTransaction;
    private volatile int defaultTransactionTimeout;
    private volatile int gracefulShutdownInterval;
    private volatile int backgroundRecoveryIntervalSeconds;
    private volatile boolean disableJmx;
    private volatile boolean synchronousJmxRegistration;
    private volatile String jndiUserTransactionName;
    private volatile String jndiTransactionSynchronizationRegistryName;
    private volatile String journal;
    private volatile String primaryJournal;
    private volatile String secondaryJournal;
    private volatile String exceptionAnalyzer;
    private volatile boolean currentNodeOnlyRecovery;
    private volatile boolean allowMultipleLrc;
    private volatile String resourceConfigurationFilename;
    private volatile boolean conservativeJournaling;
    private volatile String jdbcProxyFactoryClass;
    private volatile StatisticsCollector statsCollector = StatisticsCollector.VOID;

    protected Configuration() {
        try {
            InputStream in = null;
            Properties properties;
            try {
                String configurationFilename = System.getProperty("bitronix.tm.configuration");
                if (configurationFilename != null) {
                    if (log.isDebugEnabled()) {
                        log.debug("loading configuration file " + configurationFilename);
                    }
                    in = new FileInputStream(configurationFilename);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("loading default configuration");
                    }
                    in = ClassLoaderUtils.getResourceAsStream("bitronix-default-config.properties");
                }
                properties = new Properties();
                if (in != null)
                    properties.load(in);
                else if (log.isDebugEnabled()) {
                    log.debug("no configuration file found, using default settings");
                }
            } finally {
                if (in != null) in.close();
            }

            serverId = ConfigurationUtils.getString(properties, "bitronix.tm.serverId", null);

            diskConfiguration = new DiskJournalConfiguration(this, properties, "bitronix.tm.journal.disk", "btm1.tlog", "btm2.tlog");
            primaryDiskConfiguration = new DiskJournalConfiguration(this, properties, "bitronix.tm.journal.multiplexed.primary.disk", "btm1.tlog", "btm2.tlog");
            secondaryDiskConfiguration = new DiskJournalConfiguration(this, properties, "bitronix.tm.journal.multiplexed.secondary.disk", "btm3.tlog", "btm4.tlog");
            asynchronous2Pc = ConfigurationUtils.getBoolean(properties, "bitronix.tm.2pc.async", false);
            warnAboutZeroResourceTransaction = ConfigurationUtils.getBoolean(properties, "bitronix.tm.2pc.warnAboutZeroResourceTransactions", true);
            debugZeroResourceTransaction = ConfigurationUtils.getBoolean(properties, "bitronix.tm.2pc.debugZeroResourceTransactions", false);
            defaultTransactionTimeout = ConfigurationUtils.getInt(properties, "bitronix.tm.timer.defaultTransactionTimeout", 60);
            gracefulShutdownInterval = ConfigurationUtils.getInt(properties, "bitronix.tm.timer.gracefulShutdownInterval", 60);
            backgroundRecoveryIntervalSeconds = ConfigurationUtils.getInt(properties, "bitronix.tm.timer.backgroundRecoveryIntervalSeconds", 60);
            disableJmx = ConfigurationUtils.getBoolean(properties, "bitronix.tm.disableJmx", false);
            synchronousJmxRegistration = ConfigurationUtils.getBoolean(properties, "bitronix.tm.jmx.sync", false);
            jndiUserTransactionName = ConfigurationUtils.getString(properties, "bitronix.tm.jndi.userTransactionName", "java:comp/UserTransaction");
            jndiTransactionSynchronizationRegistryName = ConfigurationUtils.getString(properties, "bitronix.tm.jndi.transactionSynchronizationRegistryName", "java:comp/TransactionSynchronizationRegistry");
            journal = ConfigurationUtils.getString(properties, "bitronix.tm.journal", "disk");
            primaryJournal = ConfigurationUtils.getString(properties, "bitronix.tm.journal.multiplexed.primary.journal", "disk");
            secondaryJournal = ConfigurationUtils.getString(properties, "bitronix.tm.journal.multiplexed.secondary.journal", "disk");
            exceptionAnalyzer = ConfigurationUtils.getString(properties, "bitronix.tm.exceptionAnalyzer", null);
            currentNodeOnlyRecovery = ConfigurationUtils.getBoolean(properties, "bitronix.tm.currentNodeOnlyRecovery", true);
            allowMultipleLrc = ConfigurationUtils.getBoolean(properties, "bitronix.tm.allowMultipleLrc", false);
            resourceConfigurationFilename = ConfigurationUtils.getString(properties, "bitronix.tm.resource.configuration", null);
            conservativeJournaling = ConfigurationUtils.getBoolean(properties, "bitronix.tm.conservativeJournaling", false);
            jdbcProxyFactoryClass = ConfigurationUtils.getString(properties, "bitronix.tm.jdbcProxyFactoryClass", "auto");
            failOnRecordCorruption = ConfigurationUtils.getBoolean(properties, "bitronix.tm.journal.multiplexed.failOnRecordCorruption", true);
        } catch (IOException ex) {
            throw new InitializationException("error loading configuration", ex);
        }
    }


    /**
     * ASCII ID that must uniquely identify this TM instance. It must not exceed 51 characters or it will be truncated.
     * <p>Property name:<br/><b>bitronix.tm.serverId -</b> <i>(defaults to server's IP address but that's unsafe for
     * production use)</i></p>
     * @return the unique ID of this TM instance.
     */
    public String getServerId() {
        return serverId;
    }

    /**
     * Set the ASCII ID that must uniquely identify this TM instance. It must not exceed 51 characters or it will be
     * truncated.
     * @see #getServerId()
     * @param serverId the unique ID of this TM instance.
     * @return this.
     */
    public Configuration setServerId(String serverId) {
        ConfigurationUtils.checkNotStarted();
        this.serverId = serverId;
        return this;
    }

    /**
     * Should two phase commit be executed asynchronously? Asynchronous two phase commit can improve performance when
     * there are many resources enlisted in transactions but is more CPU intensive due to the dynamic thread spawning
     * requirements. It also makes debugging more complex.
     * <p>Property name:<br/><b>bitronix.tm.2pc.async -</b> <i>(defaults to false)</i></p>
     * @return true if two phase commit should be executed asynchronously.
     */
    public boolean isAsynchronous2Pc() {
        return asynchronous2Pc;
    }

    /**
     * Set if two phase commit should be executed asynchronously. Asynchronous two phase commit can improve performance
     * when there are many resources enlisted in transactions but is more CPU intensive due to the dynamic thread
     * spawning requirements. It also makes debugging more complex.
     * @see #isAsynchronous2Pc()
     * @param asynchronous2Pc true if two phase commit should be executed asynchronously.
     * @return this.
     */
    public Configuration setAsynchronous2Pc(boolean asynchronous2Pc) {
        ConfigurationUtils.checkNotStarted();
        this.asynchronous2Pc = asynchronous2Pc;
        return this;
    }

    /**
     * Should transactions executed without a single enlisted resource result in a warning or not? Most of the time
     * transactions executed with no enlisted resource reflect a bug or a mis-configuration somewhere.
     * <p>Property name:<br/><b>bitronix.tm.2pc.warnAboutZeroResourceTransactions -</b> <i>(defaults to true)</i></p>
     * @return true if transactions executed without a single enlisted resource should result in a warning.
     */
    public boolean isWarnAboutZeroResourceTransaction() {
        return warnAboutZeroResourceTransaction;
    }

    /**
     * Set if transactions executed without a single enlisted resource should result in a warning or not. Most of the
     * time transactions executed with no enlisted resource reflect a bug or a mis-configuration somewhere.
     * @see #isWarnAboutZeroResourceTransaction()
     * @param warnAboutZeroResourceTransaction true if transactions executed without a single enlisted resource should
     *        result in a warning.
     * @return this.
     */
    public Configuration setWarnAboutZeroResourceTransaction(boolean warnAboutZeroResourceTransaction) {
        ConfigurationUtils.checkNotStarted();
        this.warnAboutZeroResourceTransaction = warnAboutZeroResourceTransaction;
        return this;
    }

    /**
     * Should creation and commit call stacks of transactions executed without a single enlisted tracked and logged
     * or not?
     * <p>Property name:<br/><b>bitronix.tm.2pc.debugZeroResourceTransactions -</b> <i>(defaults to false)</i></p>
     * @return true if creation and commit call stacks of transactions executed without a single enlisted resource
     *         should be tracked and logged.
     */
    public boolean isDebugZeroResourceTransaction() {
        return debugZeroResourceTransaction;
    }

    /**
     * Set if creation and commit call stacks of transactions executed without a single enlisted resource should be
     * tracked and logged.
     * @see #isDebugZeroResourceTransaction()
     * @see #isWarnAboutZeroResourceTransaction()
     * @param debugZeroResourceTransaction true if the creation and commit call stacks of transaction executed without
     *        a single enlisted resource should be tracked and logged.
     * @return this.
     */
    public Configuration setDebugZeroResourceTransaction(boolean debugZeroResourceTransaction) {
        ConfigurationUtils.checkNotStarted();
        this.debugZeroResourceTransaction = debugZeroResourceTransaction;
        return this;
    }

    /**
     * Default transaction timeout in seconds.
     * <p>Property name:<br/><b>bitronix.tm.timer.defaultTransactionTimeout -</b> <i>(defaults to 60)</i></p>
     * @return the default transaction timeout in seconds.
     */
    public int getDefaultTransactionTimeout() {
        return defaultTransactionTimeout;
    }

    /**
     * Set the default transaction timeout in seconds.
     * @see #getDefaultTransactionTimeout()
     * @param defaultTransactionTimeout the default transaction timeout in seconds.
     * @return this.
     */
    public Configuration setDefaultTransactionTimeout(int defaultTransactionTimeout) {
        ConfigurationUtils.checkNotStarted();
        this.defaultTransactionTimeout = defaultTransactionTimeout;
        return this;
    }

    /**
     * Maximum amount of seconds the TM will wait for transactions to get done before aborting them at shutdown time.
     * <p>Property name:<br/><b>bitronix.tm.timer.gracefulShutdownInterval -</b> <i>(defaults to 60)</i></p>
     * @return the maximum amount of time in seconds.
     */
    public int getGracefulShutdownInterval() {
        return gracefulShutdownInterval;
    }

    /**
     * Set the maximum amount of seconds the TM will wait for transactions to get done before aborting them at shutdown
     * time.
     * @see #getGracefulShutdownInterval()
     * @param gracefulShutdownInterval the maximum amount of time in seconds.
     * @return this.
     */
    public Configuration setGracefulShutdownInterval(int gracefulShutdownInterval) {
        ConfigurationUtils.checkNotStarted();
        this.gracefulShutdownInterval = gracefulShutdownInterval;
        return this;
    }

    /**
     * Interval in minutes at which to run the recovery process in the background. Disabled when set to 0.
     * <p>Property name:<br/><b>bitronix.tm.timer.backgroundRecoveryInterval -</b> <i>(defaults to 0)</i></p>
     * @return the interval in minutes.
     * @deprecated superceded by #getBackgroundRecoveryIntervalSeconds().
     */
    public int getBackgroundRecoveryInterval() {
        return getBackgroundRecoveryIntervalSeconds() / 60;
    }

    /**
     * Set the interval in minutes at which to run the recovery process in the background. Disabled when set to 0.
     * @see #getBackgroundRecoveryInterval()
     * @param backgroundRecoveryInterval the interval in minutes.
     * @deprecated superceded by #setBackgroundRecoveryIntervalSeconds(int).
     * @return this.
     */
    public Configuration setBackgroundRecoveryInterval(int backgroundRecoveryInterval) {
        log.warn("setBackgroundRecoveryInterval() is deprecated, consider using setBackgroundRecoveryIntervalSeconds() instead.");
        setBackgroundRecoveryIntervalSeconds(backgroundRecoveryInterval * 60);
        return this;
    }

    /**
     * Interval in seconds at which to run the recovery process in the background. Disabled when set to 0.
     * <p>Property name:<br/><b>bitronix.tm.timer.backgroundRecoveryIntervalSeconds -</b> <i>(defaults to 60)</i></p>
     * @return the interval in seconds.
     */
    public int getBackgroundRecoveryIntervalSeconds() {
        return backgroundRecoveryIntervalSeconds;
    }

    /**
     * Set the interval in seconds at which to run the recovery process in the background. Disabled when set to 0.
     * @see #getBackgroundRecoveryIntervalSeconds()
     * @param backgroundRecoveryIntervalSeconds the interval in minutes.
     * @return this.
     */
    public Configuration setBackgroundRecoveryIntervalSeconds(int backgroundRecoveryIntervalSeconds) {
        ConfigurationUtils.checkNotStarted();
        this.backgroundRecoveryIntervalSeconds = backgroundRecoveryIntervalSeconds;
        return this;
    }

    /**
     * Should JMX Mbeans not be registered even if a JMX MBean server is detected?
     * <p>Property name:<br/><b>bitronix.tm.disableJmx -</b> <i>(defaults to false)</i></p>
     * @return true if JMX MBeans should never be registered.
     */
    public boolean isDisableJmx() {
        return disableJmx;
    }

    /**
     * Set to true if JMX Mbeans should not be registered even if a JMX MBean server is detected.
     * @see #isDisableJmx()
     * @param disableJmx true if JMX MBeans should never be registered.
     * @return this.
     */
    public Configuration setDisableJmx(boolean disableJmx) {
        ConfigurationUtils.checkNotStarted();
        this.disableJmx = disableJmx;
        return this;
    }

    /**
     * Should JMX registrations and un-registrations be done in a synchronous / blocking way.
     * <p/>
     * By default all JMX registrations are done asynchronously. Registrations and un-registrations
     * are combined to avoid the registration of short lived instances and increase the overall throughput.
     *
     * @return true if the caller should be blocked when MBeans are registered (defaults to false).
     */
    public boolean isSynchronousJmxRegistration() {
        return synchronousJmxRegistration;
    }

    /**
     * Toggles synchronous and asynchronous JMX registration mode.
     * @param synchronousJmxRegistration true if the caller should be blocked when MBeans are registered
     *                                   (defaults to false).
     * @return this.
     */
    public Configuration setSynchronousJmxRegistration(boolean synchronousJmxRegistration) {
        ConfigurationUtils.checkNotStarted();
        this.synchronousJmxRegistration = synchronousJmxRegistration;
        return this;
    }

    /**
     * Get the name the {@link javax.transaction.UserTransaction} should be bound under in the
     * {@link bitronix.tm.jndi.BitronixContext}.
     * @return the name the {@link javax.transaction.UserTransaction} should
     *         be bound under in the {@link bitronix.tm.jndi.BitronixContext}.
     */
    public String getJndiUserTransactionName() {
        return jndiUserTransactionName;
    }

    /**
     * Set the name the {@link javax.transaction.UserTransaction} should be bound under in the
     * {@link bitronix.tm.jndi.BitronixContext}.
     * @see #getJndiUserTransactionName()
     * @param jndiUserTransactionName the name the {@link javax.transaction.UserTransaction} should
     *        be bound under in the {@link bitronix.tm.jndi.BitronixContext}.
     * @return this.
     */
    public Configuration setJndiUserTransactionName(String jndiUserTransactionName) {
        ConfigurationUtils.checkNotStarted();
        this.jndiUserTransactionName = jndiUserTransactionName;
        return this;
    }

    /**
     * Get the name the {@link javax.transaction.TransactionSynchronizationRegistry} should be bound under in the
     * {@link bitronix.tm.jndi.BitronixContext}.
     * @return the name the {@link javax.transaction.TransactionSynchronizationRegistry} should
     *         be bound under in the {@link bitronix.tm.jndi.BitronixContext}.
     */
    public String getJndiTransactionSynchronizationRegistryName() {
        return jndiTransactionSynchronizationRegistryName;
    }

    /**
     * Set the name the {@link javax.transaction.TransactionSynchronizationRegistry} should be bound under in the
     * {@link bitronix.tm.jndi.BitronixContext}.
     * @see #getJndiUserTransactionName()
     * @param jndiTransactionSynchronizationRegistryName the name the {@link javax.transaction.TransactionSynchronizationRegistry} should
     *        be bound under in the {@link bitronix.tm.jndi.BitronixContext}.
     * @return this.
     */
    public Configuration setJndiTransactionSynchronizationRegistryName(String jndiTransactionSynchronizationRegistryName) {
        ConfigurationUtils.checkNotStarted();
        this.jndiTransactionSynchronizationRegistryName = jndiTransactionSynchronizationRegistryName;
        return this;
    }

    public StatisticsCollector getStatsCollector() {
        return statsCollector;
    }

    public void setStatsCollector(StatisticsCollector statsCollector) {
        this.statsCollector = statsCollector;
    }

    /**
     * Get the journal implementation. Can be <code>disk</code>, <code>null</code> or a class name.
     * @return the journal name.
     */
    public String getJournal() {
        return journal;
    }

    /**
     * Set the journal name. Can be <code>disk</code>, <code>null</code> or a class name.
     * @see #getJournal()
     * @param journal the journal name.
     * @return this.
     */
    public Configuration setJournal(String journal) {
        ConfigurationUtils.checkNotStarted();
        this.journal = journal;
        return this;
    }

    /**
     * Get the exception analyzer implementation. Can be <code>null</code> for the default one or a class name.
     * @return the exception analyzer name.
     */
    public String getExceptionAnalyzer() {
        return exceptionAnalyzer;
    }

    /**
     * Set the exception analyzer implementation. Can be <code>null</code> for the default one or a class name.
     * @see #getExceptionAnalyzer()
     * @param exceptionAnalyzer the exception analyzer name.
     * @return this.
     */
    public Configuration setExceptionAnalyzer(String exceptionAnalyzer) {
        ConfigurationUtils.checkNotStarted();
        this.exceptionAnalyzer = exceptionAnalyzer;
        return this;
    }

    /**
     * Should the recovery process <b>not</b> recover XIDs generated with another JVM unique ID? Setting this property to true
     * is useful in clustered environments where multiple instances of BTM are running on different nodes.
     * @see #getServerId() contains the value used as the JVM unique ID.
     * @return true if recovery should filter out recovered XIDs that do not contain this JVM's unique ID, false otherwise.
     */
    public boolean isCurrentNodeOnlyRecovery() {
        return currentNodeOnlyRecovery;
    }

    /**
     * Set to true if recovery should filter out recovered XIDs that do not contain this JVM's unique ID, false otherwise.
     * @see #isCurrentNodeOnlyRecovery()
     * @param currentNodeOnlyRecovery true if recovery should filter out recovered XIDs that do not contain this JVM's unique ID, false otherwise.
     * @return this.
     */
    public Configuration setCurrentNodeOnlyRecovery(boolean currentNodeOnlyRecovery) {
        ConfigurationUtils.checkNotStarted();
        this.currentNodeOnlyRecovery = currentNodeOnlyRecovery;
        return this;
    }

    /**
     * Should the transaction manager allow enlistment of multiple LRC resources in a single transaction?
     * This is highly unsafe but could be useful for testing.
     * @return true if the transaction manager should allow enlistment of multiple LRC resources in a single transaction, false otherwise.
     */
    public boolean isAllowMultipleLrc() {
        return allowMultipleLrc;
    }

    /**
     * Configuration for primary multiplexed disk journal
     *
     * @return configuration
     */
    public DiskJournalConfiguration getPrimaryDiskConfiguration() {
        return primaryDiskConfiguration;
    }

    /**
     * Configuration for primary multiplexed disk journal
     * @return configuration
     */
    public DiskJournalConfiguration getSecondaryDiskConfiguration() {
        return secondaryDiskConfiguration;
    }

    /**
     * Primary journal for multiplexed journal.
     *
     * @return journal name
     */
    public String getPrimaryJournal() {
        return primaryJournal;
    }

    /**
     * Secondary journal for multiplexed journal.
     *
     * @return journal name
     */
    public String getSecondaryJournal() {
        return secondaryJournal;
    }

    public void setDiskConfiguration(DiskJournalConfiguration diskConfiguration) {
        this.diskConfiguration = diskConfiguration;
    }

    public void setPrimaryDiskConfiguration(DiskJournalConfiguration primaryDiskConfiguration) {
        this.primaryDiskConfiguration = primaryDiskConfiguration;
    }

    public void setSecondaryDiskConfiguration(DiskJournalConfiguration secondaryDiskConfiguration) {
        this.secondaryDiskConfiguration = secondaryDiskConfiguration;
    }

    public void setPrimaryJournal(String primaryJournal) {
        this.primaryJournal = primaryJournal;
    }

    public void setSecondaryJournal(String secondaryJournal) {
        this.secondaryJournal = secondaryJournal;
    }

    /**
     * Set to true if the transaction manager should allow enlistment of multiple LRC resources in a single transaction.
     *
     * @param allowMultipleLrc true if the transaction manager should allow enlistment of multiple LRC resources in a single transaction, false otherwise.
     * @return this
     */
    public Configuration setAllowMultipleLrc(boolean allowMultipleLrc) {
        ConfigurationUtils.checkNotStarted();
        this.allowMultipleLrc = allowMultipleLrc;
        return this;
    }

    /**
     * Should the Disk Journal follow a conservative (sequential write) policy?
     * @return true if the Disk Journal should serialize writes to the transaction log, false otherwise.
     */
    public boolean isConservativeJournaling() {
        return conservativeJournaling;
    }

    /**
     * Set to true if the Disk Journal should follow a conservative (sequential write) policy.
     * @param conservativeJournaling true if the Disk Journal should follow a conservative (sequential write) policy
     * @return this
     */
    public Configuration setConservativeJournaling(boolean conservativeJournaling) {
        ConfigurationUtils.checkNotStarted();
    	this.conservativeJournaling = conservativeJournaling;
    	return this;
    }

    /**
     * Get the factory class for creating JDBC proxy instances.
     *
     * @return the name of the factory class
     */
    public String getJdbcProxyFactoryClass() {
        return jdbcProxyFactoryClass;
    }


    /**
     * Set the name of the factory class for creating JDBC proxy instances.
     *
     * @param jdbcProxyFactoryClass the name of the proxy class
     */
    public void setJdbcProxyFactoryClass(String jdbcProxyFactoryClass) {
        this.jdbcProxyFactoryClass = jdbcProxyFactoryClass;
    }


    /**
     * {@link bitronix.tm.resource.ResourceLoader} configuration file name. {@link bitronix.tm.resource.ResourceLoader}
     * will be disabled if this value is null.
     * <p>Property name:<br/><b>bitronix.tm.resource.configuration -</b> <i>(defaults to null)</i></p>
     * @return the filename of the resources configuration file or null if not configured.
     */
    public String getResourceConfigurationFilename() {
        return resourceConfigurationFilename;
    }

    /**
     * Set the {@link bitronix.tm.resource.ResourceLoader} configuration file name.
     * @see #getResourceConfigurationFilename()
     * @param resourceConfigurationFilename the filename of the resources configuration file or null you do not want to
     *        use the {@link bitronix.tm.resource.ResourceLoader}.
     * @return this.
     */
    public Configuration setResourceConfigurationFilename(String resourceConfigurationFilename) {
        ConfigurationUtils.checkNotStarted();
        this.resourceConfigurationFilename = resourceConfigurationFilename;
        return this;
    }

    public DiskJournalConfiguration getDiskConfiguration() {
        return diskConfiguration;
    }

    public boolean isFailOnRecordCorruption() {
        return failOnRecordCorruption;
    }

    /**
     * Fail when both journals have same corrupted records
     * @param failOnRecordCorruption true if fail is acceptable
     * @return this.
     */
    public Configuration setFailOnRecordCorruption(boolean failOnRecordCorruption) {
        ConfigurationUtils.checkNotStarted();
        this.failOnRecordCorruption = failOnRecordCorruption;
        return this;

    }

    /**
     * Build the server ID byte array that will be prepended in generated UIDs. Once built, the value is cached for the duration of the JVM lifespan.
     * @return the server ID.
     */
    public byte[] buildServerIdArray() {
        byte[] id = serverIdArray.get();
        if (id == null) {
            // DCL is not a problem here, we just want to avoid multiple concurrent creations of the same array as it would look ugly in the logs.
            // More important is to avoid contended synchronizations when accessing this array as it is part of Uid creation happening when a TX is opened.
            synchronized (this) {
                while ((id = serverIdArray.get()) == null) {
                    try {
                        id = serverId.getBytes(SERVER_ID_CHARSET_NAME);

                        String transcodedId = new String(id, SERVER_ID_CHARSET_NAME);
                        if (!transcodedId.equals(serverId)) {
                            log.warn("The given server ID '" + serverId + "' is not compatible with the ID charset '" + SERVER_ID_CHARSET_NAME + "' as it transcodes to '" + transcodedId + "'. " +
                                    "It is highly recommended that you specify a compatible server ID using only characters that are allowed in the ID charset.");
                        }
                    } catch (Exception ex) {
                        log.warn("Cannot get the unique server ID for this JVM ('bitronix.tm.serverId'). Make sure it is configured and you use only " + SERVER_ID_CHARSET_NAME + " characters. " +
                                "Will use IP address instead (unsafe for production usage!).");
                        try {
                            id = InetAddress.getLocalHost().getHostAddress().getBytes(SERVER_ID_CHARSET_NAME);
                        } catch (Exception ex2) {
                            final String unknownServerId = "unknown-server-id";
                            log.warn("Cannot get the local IP address. Please verify your network configuration. Will use the constant '" + unknownServerId + "' as server ID (highly unsafe!).", ex2);
                            id = unknownServerId.getBytes();
                        }
                    }

                    if (id.length > MAX_SERVER_ID_LENGTH) {
                        byte[] truncatedServerId = new byte[MAX_SERVER_ID_LENGTH];
                        System.arraycopy(id, 0, truncatedServerId, 0, MAX_SERVER_ID_LENGTH);
                        log.warn("The applied server ID '" + new String(id) + "' has to be truncated to " + MAX_SERVER_ID_LENGTH +
                                " chars (builtin hard limit) resulting in " + new String(truncatedServerId) + ". This may be highly unsafe if IDs differ with suffixes only!");
                        id = truncatedServerId;
                    }

                    if (serverIdArray.compareAndSet(null, id)) {
                        String idAsString;
                        try {
                            idAsString = new String(id, SERVER_ID_CHARSET_NAME);
                            if (serverId == null)
                                serverId = idAsString;

                            log.info("JVM unique ID: <" + idAsString + "> - Using this server ID to ensure uniqueness of transaction IDs across the network.");
                        } catch (UnsupportedEncodingException e) {
                            log.error("Unable to translate server is into " + SERVER_ID_CHARSET_NAME + " character set", e);
                        }
                    }
                }
            }
        }
        return id;
    }

    public void shutdown() {
        serverIdArray.set(null);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder(512);
        sb.append("a Configuration with [");

        try {
            sb.append(PropertyUtils.propertiesToString(this));
        } catch (PropertyException ex) {
            sb.append("???");
            if (log.isDebugEnabled()) { log.debug("error accessing properties of Configuration object", ex); }
        }

        sb.append("]");
        return sb.toString();
    }
}
