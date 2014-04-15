package bitronix.tm.testUtils;

import bitronix.tm.DiskJournalConfiguration;
import bitronix.tm.journal.DiskJournal;
import bitronix.tm.journal.Journal;
import bitronix.tm.journal.JournalRecords;

import java.io.IOException;

/**
 * Corrupts files at records level, i.e. delete/adds records, etc.
 */
public abstract class HighLevelJournalCorrupter implements JournalCorrupter {

    public void corrupt(Journal diskJournal, DiskJournalConfiguration diskConfiguration) throws IOException {
        final JournalRecords allRecords = diskJournal.collectAllRecords();
        diskJournal.close();
        diskJournal.shutdown();

        TestUtils.deleteJournalFiles(diskConfiguration, true);

        Journal newDiskJournal = new DiskJournal(diskConfiguration);
        newDiskJournal.open();

        corrupt(allRecords, newDiskJournal);

        newDiskJournal.force();

        newDiskJournal.close();
        newDiskJournal.shutdown();
    }

    protected abstract void corrupt(JournalRecords allRecords, Journal newDiskJournal) throws IOException;
}
