package bitronix.tm;

import bitronix.tm.journal.Journal;
import bitronix.tm.mock.events.EventRecorder;
import bitronix.tm.mock.resource.MockXAResource;
import bitronix.tm.mock.resource.jdbc.MockitoXADataSource;
import bitronix.tm.resource.ResourceRegistrar;
import bitronix.tm.resource.jdbc.PooledConnectionProxy;
import bitronix.tm.resource.jdbc.PoolingDataSource;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertTrue;

/**
 * @author Ilya Labun
 */
public abstract class BaseRecoveryTest {
    private static final String BITRONIX_TM_CONFIGURATION = "bitronix.tm.configuration";

    private static String oldConfigProperty;

    protected MockXAResource xaResource;
    protected PoolingDataSource pds;
    protected Journal journal;


    @BeforeClass
    public static void beforeClass() {
        oldConfigProperty = System.getProperty(BITRONIX_TM_CONFIGURATION);
        System.setProperty(BITRONIX_TM_CONFIGURATION, getPathToResource("bitronix-multiplexed.properties"));
        resetConfiguration();
    }

    @AfterClass
    public static void afterClass() {
        if (oldConfigProperty == null)
            System.clearProperty(BITRONIX_TM_CONFIGURATION);
        else
            System.setProperty(BITRONIX_TM_CONFIGURATION, oldConfigProperty);

        resetConfiguration();
    }


    @Before
    public void setUp() throws Exception {
        Iterator it = ResourceRegistrar.getResourcesUniqueNames().iterator();
        while (it.hasNext()) {
            String name = (String) it.next();
            ResourceRegistrar.unregister(ResourceRegistrar.get(name));
        }

        pds = new PoolingDataSource();
        pds.setClassName(MockitoXADataSource.class.getName());
        pds.setUniqueName("mock-xads");
        pds.setMinPoolSize(1);
        pds.setMaxPoolSize(1);
        pds.init();

        cleanupJournals();

        Connection connection1 = pds.getConnection();
        PooledConnectionProxy handle = (PooledConnectionProxy) connection1;
        xaResource = (MockXAResource) handle.getPooledConnection().getXAResource();
        connection1.close();

        // test the clustered recovery as its logic is more complex and covers the non-clustered logic
        TransactionManagerServices.getConfiguration().setCurrentNodeOnlyRecovery(true);

        // recoverer needs the journal to be open to be run manually
        journal = TransactionManagerServices.getJournal();
        journal.open();
    }


    @After
    public void tearDown() throws Exception {
        if (TransactionManagerServices.isTransactionManagerRunning())
            TransactionManagerServices.getTransactionManager().shutdown();

        journal.close();
        pds.close();
        TransactionManagerServices.getJournal().close();
        cleanupJournals();
        EventRecorder.clear();
    }

    protected static String getPathToResource(String resource) {
        return Thread.currentThread().getContextClassLoader().getResource(resource).getPath();
    }

    protected abstract void cleanupJournals();

    private static void resetConfiguration() {
        try {
            final ServicesInstance services = TransactionManagerServices.getAttachedServicesOrDefault();
            final Field configurationRef = ServicesInstance.class.getDeclaredField("configurationRef");
            configurationRef.setAccessible(true);
            final AtomicReference<Configuration> refValue = (AtomicReference<Configuration>) configurationRef.get(services);
            refValue.set(null);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to reset configuration", e);
        }
    }
}
