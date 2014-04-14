package bitronix.tm.journal;

import bitronix.tm.*;
import bitronix.tm.mock.resource.MockXid;
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
            public void corrupt(JournalRecords allRecords, Journal diskJournal, DiskJournalConfiguration diskConfiguration) throws IOException {
                final Map<Uid, JournalRecord> committedRecords = allRecords.getCommittedRecords();
                assertEquals(1, committedRecords.size());
                for (JournalRecord theOnlyRecord: committedRecords.values()) {
                    diskJournal.log(Status.STATUS_COMMITTING, theOnlyRecord.getGtrid(), theOnlyRecord.getUniqueNames());
                    diskJournal.log(Status.STATUS_COMMITTED, theOnlyRecord.getGtrid(), theOnlyRecord.getUniqueNames());
                    diskJournal.force();

                    RandomAccessFile file = new RandomAccessFile(diskConfiguration.getLogPart1Filename(), "rw");
                    FileChannel fileChannel = file.getChannel();
                    fileChannel.position(TransactionLogHeader.CURRENT_POSITION_HEADER + 8);

                    ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE/8);
                    buffer.putInt(0xDEADBEEF);
                    buffer.flip();
                    fileChannel.write(buffer);
                    fileChannel.force(true);
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
            deleteJournalFiles(TransactionManagerServices.getConfiguration().getPrimaryDiskConfiguration());
        } else {
            deleteJournalFiles(TransactionManagerServices.getConfiguration().getSecondaryDiskConfiguration());
        }
        reopenJournal();

        TransactionManagerServices.getRecoverer().run();

        assertEquals(1, TransactionManagerServices.getRecoverer().getCommittedCount());
        assertEquals(0, TransactionManagerServices.getRecoverer().getRolledbackCount());
        assertEquals(0, xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN).length);
    }

    @Test
    public void testCorruptCommittedRecord() throws Exception {
        testCorruptedJournal(new JournalCorrupter() {
            public void corrupt(JournalRecords allRecords, Journal diskJournal, DiskJournalConfiguration diskConfiguration) throws IOException {
                final Map<Uid, JournalRecord> committedRecords = allRecords.getCommittedRecords();
                for (Uid uid : committedRecords.keySet()) {
                    JournalRecord record = committedRecords.get(uid);
                    diskJournal.log(Status.STATUS_COMMITTING, record.getGtrid(), record.getUniqueNames());
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
        testCorruptedJournal(new JournalCorrupter() {
            public void corrupt(JournalRecords allRecords, Journal diskJournal, DiskJournalConfiguration diskConfiguration) throws IOException {
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
        Xid xid0 = new MockXid(0, UidGenerator.generateUid().getArray(), BitronixXid.FORMAT_ID);

        Set names = new HashSet();
        names.add(pds.getUniqueName());
        final Uid gtrid = new Uid(xid0.getGlobalTransactionId());
        journal.log(Status.STATUS_COMMITTING, gtrid, names);
        journal.log(Status.STATUS_COMMITTED, gtrid, names);

        journal.force();

        journal.close();
        journal.shutdown();

        if (corruptBoth) {
            corruptJournal(TransactionManagerServices.getConfiguration().getPrimaryDiskConfiguration(), corrupter);
            corruptJournal(TransactionManagerServices.getConfiguration().getSecondaryDiskConfiguration(), corrupter);
        } else if (corruptPrimary) {
            corruptJournal(TransactionManagerServices.getConfiguration().getPrimaryDiskConfiguration(), corrupter);
        } else {
            corruptJournal(TransactionManagerServices.getConfiguration().getSecondaryDiskConfiguration(), corrupter);
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

    private void deleteJournalFiles(DiskJournalConfiguration diskConfiguration) {
        assertTrue(new File(diskConfiguration.getLogPart1Filename()).delete());
        assertTrue(new File(diskConfiguration.getLogPart2Filename()).delete());
    }

    private void corruptJournal(DiskJournalConfiguration diskConfiguration, JournalCorrupter corrupter) throws IOException {
        Journal diskJournal = new DiskJournal(diskConfiguration);
        diskJournal.open();

        final JournalRecords allRecords = diskJournal.collectAllRecords();
        diskJournal.close();
        diskJournal.shutdown();

        deleteJournalFiles(diskConfiguration);

        Journal newDiskJournal = new DiskJournal(diskConfiguration);
        newDiskJournal.open();

        corrupter.corrupt(allRecords, newDiskJournal, diskConfiguration);

        newDiskJournal.force();

        newDiskJournal.close();
        newDiskJournal.shutdown();
    }

    private interface JournalCorrupter {
        void corrupt(JournalRecords allRecords, Journal diskJournal, DiskJournalConfiguration diskConfiguration) throws IOException;
    }
}
