package bitronix.tm.testUtils;

import java.io.IOException;

/**
 * Fills journal with test data.
 *
 * @author Ilya Labun
 *
 */
public interface JournalWriter {
    void write() throws IOException;
}
