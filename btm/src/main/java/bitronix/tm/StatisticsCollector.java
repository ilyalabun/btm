package bitronix.tm;

import bitronix.tm.internal.XAResourceHolderState;

import java.util.List;

/**
 * @author i.labun
 */
public interface StatisticsCollector {
    void onTransactionCommit(String uid,
                             long duration,
                             List<XAResourceHolderState> interestedResources,
                             List<XAResourceHolderState> notInterestedResources);

    void onResourceCommit(String uid, long duration, XAResourceHolderState resource);

    void onTransactionPrepare(String uid,
                              long duration,
                              List<XAResourceHolderState> resources,
                              boolean onePhase);

    void onResourcePrepare(String uid, long duration, XAResourceHolderState resource, String prepareStatus);

    StatisticsCollector VOID = new StatisticsCollector() {

        public void onTransactionCommit(String uid, long duration, List<XAResourceHolderState> interestedResources, List<XAResourceHolderState> notInterestedResources) {
            // Do nothing
        }

        public void onResourceCommit(String uid, long duration, XAResourceHolderState resource) {
            // Do nothing
        }

        public void onTransactionPrepare(String uid, long duration, List<XAResourceHolderState> resources, boolean onePhase) {
            // Do nothing
        }

        public void onResourcePrepare(String uid, long duration, XAResourceHolderState resource, String prepareStatus) {
            // Do nothing
        }

        @Override
        public String toString() {
            return "Void statistics collector";
        }
    };
}
