package bitronix.tm.testUtils;

import bitronix.tm.DiskJournalConfiguration;
import bitronix.tm.journal.Journal;

import java.io.IOException;

/**
 * Corrupts journal.
 *
* @author Ilya Labun
*/
public interface JournalCorrupter {
    void corrupt(Journal diskJournal, DiskJournalConfiguration diskConfiguration) throws IOException;
}
