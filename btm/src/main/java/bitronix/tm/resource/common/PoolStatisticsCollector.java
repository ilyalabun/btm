package bitronix.tm.resource.common;

/**
 * @author i.labun
 */
public interface PoolStatisticsCollector {
    void onConnectionsNumberChanged(String poolId, int capacity, int inPool, int accessible, int notAccessible);

    void onConnectionAcquired(String poolId, long acquireDurationNs);

    void onConnectionReleased(String poolId, long timeOutOfPoolNs);

    PoolStatisticsCollector VOID = new PoolStatisticsCollector() {
        public void onConnectionsNumberChanged(String poolId, int capacity, int inPool, int accessible, int notAccessible) {
            // do nothing
        }

        public void onConnectionAcquired(String poolId, long acquireDurationNs) {
            // do nothing
        }

        public void onConnectionReleased(String poolId, long timeOutOfPoolNs) {
            // do nothing
        }
    };
}
