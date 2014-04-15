package bitronix.tm.journal;

import bitronix.tm.*;
import bitronix.tm.mock.resource.MockXid;
import bitronix.tm.testUtils.HighLevelJournalCorrupter;
import bitronix.tm.testUtils.JournalCorrupter;
import bitronix.tm.testUtils.JournalWriter;
import bitronix.tm.testUtils.TestUtils;
import bitronix.tm.utils.Uid;
import bitronix.tm.utils.UidGenerator;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.transaction.Status;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Ilya Labun
 */
@RunWith(Parameterized.class)
public class MultiplexedJournalTest extends BaseRecoveryTest {

    boolean corruptPrimary;

    public MultiplexedJournalTest(boolean corruptPrimary) {
        this.corruptPrimary = corruptPrimary;
    }

    @Parameterized.Parameters
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][]{{true}, {false}});
    }

    @Test
    public void testRecoveryWhenEverythingOk() throws Exception {
        Xid xid0 = new MockXid(0, UidGenerator.generateUid().getArray(), BitronixXid.FORMAT_ID);

        Set names = new HashSet();
        names.add(pds.getUniqueName());
        final Uid gtrid = new Uid(xid0.getGlobalTransactionId());
        journal.log(Status.STATUS_COMMITTING, gtrid, names);
        journal.log(Status.STATUS_COMMITTED, gtrid, names);
        journal.force();
        reopenJournal();

        TransactionManagerServices.getRecoverer().run();

        assertEquals(0, TransactionManagerServices.getRecoverer().getCommittedCount());
        assertEquals(0, TransactionManagerServices.getRecoverer().getRolledbackCount());
        assertEquals(0, xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN).length);
    }

    @Test
    public void testThrowsExceptionWhenSameRecordsCorrupted() throws Exception {
        testCorruptedJournal(new JournalCorrupter() {
            public void corrupt(Journal diskJournal, DiskJournalConfiguration diskConfiguration) throws IOException {
                RandomAccessFile file = new RandomAccessFile(diskConfiguration.getLogPart1Filename(), "rw");
                try {
                    FileChannel fileChannel = file.getChannel();
                    fileChannel.position(TransactionLogHeader.HEADER_LENGTH + 8);

                    ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE / 8);
                    buffer.putInt(0xDEADBEEF);
                    buffer.flip();
                    fileChannel.write(buffer);
                } finally {
                    file.close();
                }
            }
        }, true);

        try {
            journal.collectAllRecords();
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("Both journals have same corrupted records."));
        }
    }

    @Test
    public void testJournalDeleteRecovery() throws Exception {
        Xid xid0 = new MockXid(0, UidGenerator.generateUid().getArray(), BitronixXid.FORMAT_ID);
        xaResource.addInDoubtXid(xid0);

        Set names = new HashSet();
        names.add(pds.getUniqueName());
        journal.log(Status.STATUS_COMMITTING, new Uid(xid0.getGlobalTransactionId()), names);
        journal.force();

        if (corruptPrimary) {
            TestUtils.deleteJournalFiles(TransactionManagerServices.getConfiguration().getPrimaryDiskConfiguration(), true);
        } else {
            TestUtils.deleteJournalFiles(TransactionManagerServices.getConfiguration().getSecondaryDiskConfiguration(), true);
        }
        reopenJournal();

        TransactionManagerServices.getRecoverer().run();

        assertEquals(1, TransactionManagerServices.getRecoverer().getCommittedCount());
        assertEquals(0, TransactionManagerServices.getRecoverer().getRolledbackCount());
        assertEquals(0, xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN).length);
    }

    @Test
    public void testCorruptCommittedRecord() throws Exception {
        testCorruptedJournal(new HighLevelJournalCorrupter() {
            protected void corrupt(JournalRecords allRecords, Journal newJournal) throws IOException {
                final Map<Uid, JournalRecord> committedRecords = allRecords.getCommittedRecords();
                for (Uid uid : committedRecords.keySet()) {
                    JournalRecord record = committedRecords.get(uid);
                    newJournal.log(Status.STATUS_COMMITTING, record.getGtrid(), record.getUniqueNames());
                }
            }
        }, false);

        TransactionManagerServices.getRecoverer().run();
        assertEquals(0, TransactionManagerServices.getRecoverer().getCommittedCount());
        assertEquals(0, TransactionManagerServices.getRecoverer().getRolledbackCount());
        assertEquals(0, xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN).length);
    }


    @Test
    public void testCorruptCommittingRecord() throws Exception {
        testCorruptedJournal(new HighLevelJournalCorrupter() {
            protected void corrupt(JournalRecords allRecords, Journal diskJournal) throws IOException {
                final Map<Uid, JournalRecord> committedRecords = allRecords.getCommittedRecords();
                for (Uid uid : committedRecords.keySet()) {
                    JournalRecord record = committedRecords.get(uid);
                    diskJournal.log(record.getStatus(), record.getGtrid(), record.getUniqueNames());
                }
            }
        }, false);

        TransactionManagerServices.getRecoverer().run();
        assertEquals(0, TransactionManagerServices.getRecoverer().getCommittedCount());
        assertEquals(0, TransactionManagerServices.getRecoverer().getRolledbackCount());
        assertEquals(0, xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN).length);
    }

    private void testCorruptedJournal(JournalCorrupter corrupter, boolean corruptBoth) throws IOException {
        testCorruptedJournal(new JournalWriter() {
            public void write() throws IOException {
                Xid xid0 = new MockXid(0, UidGenerator.generateUid().getArray(), BitronixXid.FORMAT_ID);

                Set names = new HashSet();
                names.add(pds.getUniqueName());
                final Uid gtrid = new Uid(xid0.getGlobalTransactionId());
                journal.log(Status.STATUS_COMMITTING, gtrid, names);
                journal.log(Status.STATUS_COMMITTED, gtrid, names);
            }
        }, corrupter, corruptBoth);
    }

    private void testCorruptedJournal(JournalWriter writer, JournalCorrupter corrupter, boolean corruptBoth) throws IOException {
        writer.write();

        journal.force();

        journal.close();
        journal.shutdown();

        if (corruptBoth) {
            TestUtils.corruptJournal(TransactionManagerServices.getConfiguration().getPrimaryDiskConfiguration(), corrupter);
            TestUtils.corruptJournal(TransactionManagerServices.getConfiguration().getSecondaryDiskConfiguration(), corrupter);
        } else if (corruptPrimary) {
            TestUtils.corruptJournal(TransactionManagerServices.getConfiguration().getPrimaryDiskConfiguration(), corrupter);
        } else {
            TestUtils.corruptJournal(TransactionManagerServices.getConfiguration().getSecondaryDiskConfiguration(), corrupter);
        }

        journal.open();
    }

    @Override
    protected void cleanupJournals() {
        new File(TransactionManagerServices.getConfiguration().getPrimaryDiskConfiguration().getLogPart1Filename()).delete();
        new File(TransactionManagerServices.getConfiguration().getPrimaryDiskConfiguration().getLogPart2Filename()).delete();
        new File(TransactionManagerServices.getConfiguration().getSecondaryDiskConfiguration().getLogPart1Filename()).delete();
        new File(TransactionManagerServices.getConfiguration().getSecondaryDiskConfiguration().getLogPart2Filename()).delete();
    }


    private void reopenJournal() throws IOException {
        journal.close();
        journal.shutdown();

        journal.open();
    }
}
