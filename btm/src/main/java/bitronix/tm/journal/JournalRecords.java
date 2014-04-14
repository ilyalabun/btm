package bitronix.tm.journal;

import bitronix.tm.utils.Uid;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Ilya Labun
 */
public class JournalRecords {
    private final Map<Uid, JournalRecord> danglingRecords;

    private final Map<Uid, JournalRecord> committedRecords;

    private final Set<Integer> corruptedRecords = new HashSet<Integer>(64);

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

    public void addCommittedRecord(JournalRecord record) {
        committedRecords.put(record.getGtrid(), record);
    }

    public JournalRecord getCommittedRecord(Uid gtrid) {
        return committedRecords.get(gtrid);
    }

    public Map<Uid, JournalRecord> getDanglingRecords() {
        return danglingRecords;
    }

    public Map<Uid, JournalRecord> getCommittedRecords() {
        return committedRecords;
    }

    public Set<Integer> getCorruptedRecords() {
        return corruptedRecords;
    }

    public void addCorruptedRecord(int recordNumber) {
        corruptedRecords.add(recordNumber);
    }
}
