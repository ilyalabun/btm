package bitronix.tm.journal;

import bitronix.tm.utils.Uid;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Ilya Labun
 */
public class JournalRecords {
    private final Map<Uid, JournalRecord> danglingRecords;

    private final Map<Uid, JournalRecord> committedRecords;

    public JournalRecords() {
        danglingRecords = new HashMap<Uid, JournalRecord>(64);
        committedRecords = new HashMap<Uid, JournalRecord>(64);
    }

    public JournalRecords(Map<Uid, JournalRecord> danglingRecords, Map<Uid, JournalRecord> committedRecords) {
        this.danglingRecords = danglingRecords;
        this.committedRecords = committedRecords;
    }

    public void addDanglingRecord(JournalRecord record) {
        danglingRecords.put(record.getGtrid(), record);
    }

    public JournalRecord getDanglingRecord(Uid gtrid) {
        return danglingRecords.get(gtrid);
    }

    public void removeDanglingRecord(Uid gtrid) {
        danglingRecords.remove(gtrid);
    }

    public void addComittedRecord(JournalRecord record) {
        committedRecords.put(record.getGtrid(), record);
    }

    public JournalRecord getCommitedRecord(Uid gtrid) {
        return committedRecords.get(gtrid);
    }

    public Map<Uid, JournalRecord> getDanglingRecords() {
        return danglingRecords;
    }

    public Map<Uid, JournalRecord> getCommittedRecords() {
        return committedRecords;
    }
}
