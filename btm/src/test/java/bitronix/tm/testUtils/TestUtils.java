package bitronix.tm.testUtils;

import bitronix.tm.DiskJournalConfiguration;
import bitronix.tm.journal.DiskJournal;
import bitronix.tm.journal.Journal;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

/**
 * @author Ilya Labun
 */
public class TestUtils {
    private TestUtils(){}

    public static void corruptJournal(DiskJournalConfiguration diskConfiguration, JournalCorrupter corrupter) throws IOException {
        Journal diskJournal = new DiskJournal(diskConfiguration);
        diskJournal.open();

        corrupter.corrupt(diskJournal, diskConfiguration);

        diskJournal.close();
        diskJournal.shutdown();
    }


    public static void deleteJournalFiles(DiskJournalConfiguration diskConfiguration, boolean assertOnFail) {
        assertTrue(new File(diskConfiguration.getLogPart1Filename()).delete() || !assertOnFail);
        assertTrue(new File(diskConfiguration.getLogPart2Filename()).delete() || !assertOnFail);
    }

}
