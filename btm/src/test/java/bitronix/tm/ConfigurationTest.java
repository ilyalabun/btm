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

import bitronix.tm.utils.ConfigurationUtils;
import junit.framework.TestCase;

import java.util.Properties;

/**
 *
 * @author Ludovic Orban
 */
public class ConfigurationTest extends TestCase {

    public void testGetString() throws Exception {
        Properties props = new Properties();
        props.setProperty("1", "one");
        props.setProperty("2", "two");
        System.setProperty("3", "three");
        props.setProperty("4", "four");
        System.setProperty("4", "four-sys");
        props.setProperty("12", "${1} ${2}");
        props.setProperty("13", "${1} ${3}");
        props.setProperty("14", "${1} ${}");
        props.setProperty("15", "${1} ${tatata");
        props.setProperty("16", "${1} ${4}");
        props.setProperty("17", "x$");
        props.setProperty("18", "x${");

        assertEquals("one", ConfigurationUtils.getString(props, "1", null));
        assertEquals("two", ConfigurationUtils.getString(props, "2", null));
        assertEquals("three", ConfigurationUtils.getString(props, "3", null));
        assertEquals("one two", ConfigurationUtils.getString(props, "12", null));
        assertEquals("one three", ConfigurationUtils.getString(props, "13", null));
        assertEquals("one four-sys", ConfigurationUtils.getString(props, "16", null));

        try {
            ConfigurationUtils.getString(props, "14", null);
            fail("expected IllegalArgumentException: property ref cannot refer to an empty name: ${}");
        } catch (IllegalArgumentException ex) {
            assertEquals("property ref cannot refer to an empty name: ${}", ex.getMessage());
        }

        try {
            ConfigurationUtils.getString(props, "15", null);
            fail("expected IllegalArgumentException: unclosed property ref: ${tatata");
        } catch (IllegalArgumentException ex) {
            assertEquals("unclosed property ref: ${tatata", ex.getMessage());
        }

        assertEquals("x$", ConfigurationUtils.getString(props, "17", null));

        try {
            ConfigurationUtils.getString(props, "18", null);
            fail("expected IllegalArgumentException: unclosed property ref: ${");
        } catch (IllegalArgumentException ex) {
            assertEquals("unclosed property ref: ${", ex.getMessage());
        }
    }

    public void testGetIntBoolean() {
        Properties props = new Properties();
        props.setProperty("one", "1");
        props.setProperty("two", "2");
        System.setProperty("three", "3");
        System.setProperty("vrai", "true");
        props.setProperty("faux", "false");

        assertEquals(1, ConfigurationUtils.getInt(props, "one", -1));
        assertEquals(2, ConfigurationUtils.getInt(props, "two", -1));
        assertEquals(3, ConfigurationUtils.getInt(props, "three", -1));
        assertEquals(10, ConfigurationUtils.getInt(props, "ten", 10));

        assertEquals(true, ConfigurationUtils.getBoolean(props, "vrai", false));
        assertEquals(false, ConfigurationUtils.getBoolean(props, "faux", true));
        assertEquals(true, ConfigurationUtils.getBoolean(props, "wrong", true));
    }

    public void testToString() {
        final String expectation = "a Configuration with [allowMultipleLrc=false, asynchronous2Pc=false," +
                " backgroundRecoveryInterval=1, backgroundRecoveryIntervalSeconds=60, conservativeJournaling=false, currentNodeOnlyRecovery=true," +
                " debugZeroResourceTransaction=false, defaultTransactionTimeout=60, disableJmx=false," +
                " diskConfiguration.filterLogStatus=false, diskConfiguration.forceBatchingEnabled=true, diskConfiguration.forcedWriteEnabled=true," +
                " diskConfiguration.logPart1Filename=target/btm1.tlog, diskConfiguration.logPart2Filename=target/btm2.tlog," +
                " diskConfiguration.maxLogSizeInMb=2, diskConfiguration.skipCorruptedLogs=false," +
                " exceptionAnalyzer=null, gracefulShutdownInterval=10, jdbcProxyFactoryClass=auto," +
                " jndiTransactionSynchronizationRegistryName=java:comp/TransactionSynchronizationRegistry," +
                " jndiUserTransactionName=java:comp/UserTransaction, journal=disk," +
                " primaryDiskConfiguration.filterLogStatus=false, primaryDiskConfiguration.forceBatchingEnabled=true," +
                " primaryDiskConfiguration.forcedWriteEnabled=true, primaryDiskConfiguration.logPart1Filename=btm1.tlog," +
                " primaryDiskConfiguration.logPart2Filename=btm2.tlog, primaryDiskConfiguration.maxLogSizeInMb=2," +
                " primaryDiskConfiguration.skipCorruptedLogs=false, primaryJournal=disk, resourceConfigurationFilename=null," +
                " secondaryDiskConfiguration.filterLogStatus=false, secondaryDiskConfiguration.forceBatchingEnabled=true," +
                " secondaryDiskConfiguration.forcedWriteEnabled=true, secondaryDiskConfiguration.logPart1Filename=btm3.tlog," +
                " secondaryDiskConfiguration.logPart2Filename=btm4.tlog, secondaryDiskConfiguration.maxLogSizeInMb=2," +
                " secondaryDiskConfiguration.skipCorruptedLogs=false, secondaryJournal=disk," +
                " serverId=null, synchronousJmxRegistration=false," +
                " warnAboutZeroResourceTransaction=true]";

        assertEquals(expectation, new Configuration().toString());
    }

}
