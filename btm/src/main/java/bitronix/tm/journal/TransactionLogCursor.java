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
package bitronix.tm.journal;

import bitronix.tm.utils.Uid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.InvalidMarkException;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.Set;

/**
 * Used to read {@link TransactionLogRecord} objects from a log file.
 *
 * @author Ludovic Orban
 */
public class TransactionLogCursor {

    private final static Logger log = LoggerFactory.getLogger(TransactionLogCursor.class);

    private static final long INF = Long.MAX_VALUE;
    private static final long MINUS_INF = Long.MIN_VALUE;

    // private final RandomAccessFile randomAccessFile;
    private FileInputStream fis;
    private FileChannel fileChannel;
    private long currentPosition;
    private long endPosition;
    private ByteBuffer page;

    /**
     * Create a TransactionLogCursor that will read from the specified file.
     * This opens a new read-only file descriptor.
     * @param file the file to read logs from
     * @throws IOException if an I/O error occurs.
     */
    public TransactionLogCursor(File file) throws IOException {
        this.fis = new FileInputStream(file);
        this.fileChannel = fis.getChannel();
        this.page = ByteBuffer.allocate(8192);

        fileChannel.position(TransactionLogHeader.CURRENT_POSITION_HEADER);
        fileChannel.read(page);
        page.rewind();
        endPosition = page.getLong();
        currentPosition = TransactionLogHeader.CURRENT_POSITION_HEADER + 8;
    }

    /**
     * Fetch the next TransactionLogRecord from log, recalculating the CRC and checking it against the stored one.
     * InvalidChecksumException is thrown if the check fails.
     * @return the TransactionLogRecord or null if the end of the log file has been reached
     * @throws IOException if an I/O error occurs.
     */
    public TransactionLogRecord readLog() throws IOException {
        return readLog(false);
    }

    /**
     * Fetch the next TransactionLogRecord from log.
     * @param skipCrcCheck if set to false, the method will thow an InvalidChecksumException if the CRC on disk does
     *        not match the recalculated one. Otherwise, the CRC is not recalculated nor checked agains the stored one.
     * @return the TransactionLogRecord or null if the end of the log file has been reached
     * @throws IOException if an I/O error occurs.
     */
    public TransactionLogRecord readLog(boolean skipCrcCheck) throws IOException {
        if (currentPosition >= endPosition) {
            if (log.isDebugEnabled())
                log.debug("end of transaction log file reached at " + currentPosition);
            return null;
        }

        final int status = page.getInt();
        // currentPosition += 4;
        final int recordLength = page.getInt();
        if (recordLength < 0)
            throw new IllegalArgumentException("Record length is negative. It's impossible to read the rest of file.");

        // currentPosition += 4;
        currentPosition += 8;

        if (page.position() + recordLength + 8 > page.limit()) {
            page.compact();
            fileChannel.read(page);
            page.rewind();
        }

        final int endOfRecordPosition = page.position() + recordLength;
        if (status < 0) {
            skipRecord(endOfRecordPosition, 0);
            throw new CorruptedTransactionLogException(String.format("Status is negative on position %s. Record is corrupted", currentPosition - 8));
        }

        if (currentPosition + recordLength > endPosition) {
            page.position(page.position() + recordLength);
            currentPosition += recordLength;
            throw new CorruptedTransactionLogException("corrupted log found at position " + currentPosition
                    + " (record terminator outside of file bounds: " + currentPosition + recordLength + " of "
                    + endPosition + ", recordLength: " + recordLength + ")");
        }


        currentPosition += 4;
        final int headerLength = readInt(endOfRecordPosition, 1, INF, "header length");

        currentPosition += 8;
        final long time = readLong(endOfRecordPosition, 1, INF, "time");

        currentPosition += 4;
        final int sequenceNumber = readInt(endOfRecordPosition, 1, INF, "sequence number");

        currentPosition += 4;
        final int crc32 = page.getInt();

        currentPosition += 1;
        final byte gtridSize = readByte(endOfRecordPosition, 1, 64, "gtrid size");

        // check for log terminator
        page.mark();
        page.position(endOfRecordPosition - 4);
        int endCode = page.getInt();
        try {
            page.reset();
        } catch (InvalidMarkException e) {
            throw new CorruptedTransactionLogException("Page mark is invalid. Record is possibly corrupted.", e);
        }
        if (endCode != TransactionLogAppender.END_RECORD)
            throw new CorruptedTransactionLogException("corrupted log found at position " + currentPosition + " (no record terminator found)");

        // check that GTRID is not too long
        if (4 + 8 + 4 + 4 + 1 + gtridSize > recordLength) {
            page.position(endOfRecordPosition);
            throw new CorruptedTransactionLogException("corrupted log found at position " + currentPosition
                    + " (GTRID size too long)");
        }

        final byte[] gtridArray = new byte[gtridSize];
        page.get(gtridArray);
        currentPosition += gtridSize;
        Uid gtrid = new Uid(gtridArray);
        currentPosition += 4;
        final int uniqueNamesCount = readInt(endOfRecordPosition, 0, INF, "unique names count");
        Set<String> uniqueNames = new HashSet<String>();
        int currentReadCount = 4 + 8 + 4 + 4 + 1 + gtridSize + 4;

        for (int i = 0; i < uniqueNamesCount; i++) {
            currentPosition += 2;
            int length = readShort(endOfRecordPosition, 1, INF, String.format("length of name %s", i));

            // check that names aren't too long
            currentReadCount += 2 + length;
            if (currentReadCount > recordLength) {
                page.position(endOfRecordPosition);
                throw new CorruptedTransactionLogException("corrupted log found at position " + currentPosition
                        + " (unique names too long, " + (i + 1) + " out of " + uniqueNamesCount + ", length: " + length
                        + ", currentReadCount: " + currentReadCount + ", recordLength: " + recordLength + ")");
            }

            byte[] nameBytes = new byte[length];
            page.get(nameBytes);
            currentPosition += length;
            uniqueNames.add(new String(nameBytes, "US-ASCII"));
        }
        final int cEndRecord = page.getInt();
        currentPosition += 4;

        TransactionLogRecord tlog = new TransactionLogRecord(status, recordLength, headerLength, time, sequenceNumber,
                crc32, gtrid, uniqueNames, cEndRecord);

        // check that CRC is okay
        if (!skipCrcCheck && !tlog.isCrc32Correct()) {
            page.position(endOfRecordPosition);
            throw new CorruptedTransactionLogException("corrupted log found at position " + currentPosition
                    + "(invalid CRC, recorded: " + tlog.getCrc32() + ", calculated: " + tlog.calculateCrc32() + ")");
        }

        return tlog;
    }

    private void skipRecord(int endOfRecordPosition, int fieldLength) {
        currentPosition += endOfRecordPosition - page.position();
        page.position(endOfRecordPosition);
    }

    /**
     * Close the cursor and the underlying file
     * @throws IOException if an I/O error occurs.
     */
    public void close() throws IOException {
        fis.close();
        fileChannel.close();
    }

    private int readInt(int endOfRecordPosition, long lowerBound, long upperBound, String fieldName) throws CorruptedTransactionLogException {
        final int value = page.getInt();

        if (isOutOfBounds(value, lowerBound, upperBound)) {
            reportError(fieldName, endOfRecordPosition, value, Integer.SIZE / 8, lowerBound, upperBound);
        }

        return value;
    }

    private long readLong(int endOfRecordPosition, long lowerBound, long upperBound, String fieldName) throws CorruptedTransactionLogException {
        final long value = page.getLong();
        if (isOutOfBounds(value, lowerBound, upperBound)) {
            reportError(fieldName, endOfRecordPosition, value, Long.SIZE / 8, lowerBound, upperBound);
        }

        return value;
    }

    private byte readByte(int endOfRecordPosition, long lowerBound, long upperBound, String fieldName) throws CorruptedTransactionLogException {
        final byte value = page.get();
        if (isOutOfBounds(value, lowerBound, upperBound)) {
            reportError(fieldName, endOfRecordPosition, value, Byte.SIZE / 8, lowerBound, upperBound);
        }

        return value;
    }

    private short readShort(int endOfRecordPosition, long lowerBound, long upperBound, String fieldName) throws CorruptedTransactionLogException {
        final short value = page.getShort();
        if (isOutOfBounds(value, lowerBound, upperBound)) {
            reportError(fieldName, endOfRecordPosition, value, Short.SIZE / 8, lowerBound, upperBound);
        }

        return value;
    }

    private void reportError(String fieldName, int endOfRecordPosition, long value,
                             int fieldLength, long lowerBound, long upperBound) throws CorruptedTransactionLogException {
        skipRecord(endOfRecordPosition, fieldLength);
        throw new CorruptedTransactionLogException(String.format(
                "Record field [%s] with value %s on position %s is out of its bounds [%s, %s]",
                fieldName, value, currentPosition - fieldLength,
                lowerBound == MINUS_INF? "-inf": lowerBound,
                upperBound == INF? "inf": upperBound));
    }

    /**
     * Checks if some value is out of its bounds.
     * Value is out of its bounds when <code>value < lowerBound || value > upperBound</code>
     *
     * @param value value to check.
     * @param lowerBound lower bound.
     * @param upperBound upper bound.
     * @return true if <code>value</code> is out of its bounds.
     */
    private boolean isOutOfBounds(long value, long lowerBound, long upperBound) {
        if (lowerBound > upperBound)
            throw new IllegalArgumentException(String.format("Lower bound %s is greater than upper bound %s", lowerBound, upperBound));

        if (value < lowerBound || value > upperBound)
            return true;

        return false;
    }
}
