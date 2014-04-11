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
package bitronix.tm.mock.resource;

import bitronix.tm.journal.Journal;
import bitronix.tm.journal.JournalRecord;
import bitronix.tm.journal.JournalRecords;
import bitronix.tm.journal.TransactionLogRecord;
import bitronix.tm.mock.events.EventRecorder;
import bitronix.tm.mock.events.JournalLogEvent;
import bitronix.tm.utils.Uid;

import javax.transaction.Status;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Ludovic Orban
 */
public class MockJournal implements Journal {

    private JournalRecords records;

    private EventRecorder getEventRecorder() {
        return EventRecorder.getEventRecorder(this);
    }

    public void log(int status, Uid gtrid, Set<String> uniqueNames) throws IOException {
        TransactionLogRecord record = new TransactionLogRecord(status, gtrid, uniqueNames);
        if (status == Status.STATUS_COMMITTING) {
            records.addDanglingRecord(record);
        }
        if (status == Status.STATUS_COMMITTED) {
            records.removeDanglingRecord(gtrid);
            records.addComittedRecord(record);
        }
        getEventRecorder().addEvent(new JournalLogEvent(this, status, gtrid, uniqueNames));
    }

    public void open() throws IOException {
        records = new JournalRecords();
    }

    public void close() throws IOException {
        records = null;
    }

    public void force() throws IOException {
    }

    public Map<Uid, JournalRecord> collectDanglingRecords() throws IOException {
        return records.getDanglingRecords();
    }

    public JournalRecords collectAllRecords() throws IOException {
        return records;
    }

    public Iterator<JournalRecord> readRecords(boolean includeInvalid) throws IOException {
        return records.getDanglingRecords().values().iterator();
    }

    public void shutdown() {
    }
}
