package bitronix.tm.journal;

import bitronix.tm.BitronixXid;
import bitronix.tm.DiskJournalConfiguration;
import bitronix.tm.TransactionManagerServices;
import bitronix.tm.mock.resource.MockXid;
import bitronix.tm.testUtils.JournalCorrupter;
import bitronix.tm.testUtils.TestUtils;
import bitronix.tm.utils.Uid;
import bitronix.tm.utils.UidGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.transaction.Status;
import javax.transaction.xa.Xid;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test correct reaction on log record corruption by negative values.
 * This corner case gives us additional durability.
 *
 * @author Ilya Labun
 */
@RunWith(Parameterized.class)
public class DiskJournalCorruptionTest {

    private DiskJournal journal;

    private DiskJournalConfiguration conf = TransactionManagerServices.getConfiguration().getDiskConfiguration();

    private int positionToCorrupt;

    private boolean shouldThrowException;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {0, false},
                {4, true},
                {8, false},
                {12, false},
                {20, false},
                {28, false},
                {32, false}
        });
    }

    public DiskJournalCorruptionTest(int positionToCorrupt, boolean shouldThrowException) {
        this.positionToCorrupt = positionToCorrupt;
        this.shouldThrowException = shouldThrowException;
    }

    @Before
    public void before() throws IOException {
        TestUtils.deleteJournalFiles(conf, false);
        TransactionManagerServices.getConfiguration().getDiskConfiguration().setSkipCorruptedLogs(true);

        Xid xid0 = new MockXid(0, UidGenerator.generateUid().getArray(), BitronixXid.FORMAT_ID);
        Xid xid1 = new MockXid(1, UidGenerator.generateUid().getArray(), BitronixXid.FORMAT_ID);

        Set names = new HashSet();
        names.add("trx0");
        names.add("trx1");

        Uid gtrid0 = new Uid(xid0.getGlobalTransactionId());
        Uid gtrid1 = new Uid(xid1.getGlobalTransactionId());

        journal = new DiskJournal(conf);
        journal.open();

        journal.log(Status.STATUS_COMMITTING, gtrid0, names);
        journal.log(Status.STATUS_COMMITTED, gtrid0, names);

        journal.log(Status.STATUS_COMMITTING, gtrid1, names);
        journal.log(Status.STATUS_COMMITTED, gtrid1, names);

        journal.close();
        journal.shutdown();
    }

    @After
    public void after() throws IOException {
        journal.close();
        journal.shutdown();

        TestUtils.deleteJournalFiles(conf, true);
    }

    /**
     * Sets various parts of first log record to negative values.
     */
    @Test
    public void testCorruptRecord() throws IOException {
        TestUtils.corruptJournal(conf, new JournalCorrupter() {
            public void corrupt(Journal diskJournal, DiskJournalConfiguration diskConfiguration) throws IOException {
                RandomAccessFile file = new RandomAccessFile(diskConfiguration.getLogPart1Filename(), "rw");
                try {
                    FileChannel fileChannel = file.getChannel();
                    fileChannel.position(TransactionLogHeader.HEADER_LENGTH + positionToCorrupt);

                    ByteBuffer buffer = ByteBuffer.allocate(1);
                    buffer.put((byte) 0xFF);
                    buffer.flip();
                    fileChannel.write(buffer);
                } finally {
                    file.close();
                }

            }
        });

        journal.open();
        try {
            final JournalRecords allRecords = journal.collectAllRecords();
            assertEquals(0, allRecords.getDanglingRecords().size());
            assertEquals(2, allRecords.getCommittedRecords().size());
            assertEquals(1, allRecords.getCorruptedRecords().size());
        } catch (RuntimeException e) {
            assertTrue("Exception should not be thrown here", shouldThrowException);
            return;
        }

        assertFalse("Exception should be thrown in this test, but it is not", shouldThrowException);
    }

}
