package bitronix.tm.tm;

import bitronix.tm.ServicesInstance;
import bitronix.tm.TransactionManagerServices;
import junit.framework.Assert;
import org.junit.Test;

/**
 * @author i.labun
 */
public class TransactionManagerServicesTest {

    private static final String KEY1 = "SERVICES1";
    private static final String KEY2 = "SERVICES2";

    @Test
    public void testDifferentServicesInstances() throws InterruptedException {
        final ServicesInstance[] instance1 = new ServicesInstance[1];
        final ServicesInstance[] instance2 = new ServicesInstance[1];

        Thread thread1 = new Thread() {
            public void run() {
                instance1[0] = TransactionManagerServices.attachToServices(KEY1);
            }
        };

        Thread thread2 = new Thread() {
            public void run() {
                instance2[0] = TransactionManagerServices.attachToServices(KEY2);
            }
        };

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        ServicesInstance serviceInstance1 = instance1[0];
        ServicesInstance serviceInstance2 = instance2[0];
        Assert.assertNotSame(serviceInstance1, serviceInstance2);
        Assert.assertEquals(KEY1, serviceInstance1.getKey());
        Assert.assertEquals(KEY2, serviceInstance2.getKey());
    }

    @Test
    public void testSameServicesInstances() throws InterruptedException {
        final ServicesInstance[] instance1 = new ServicesInstance[1];
        final ServicesInstance[] instance2 = new ServicesInstance[1];

        Thread thread1 = new Thread() {
            public void run() {
                instance1[0] = TransactionManagerServices.attachToServices(KEY1);
            }
        };

        Thread thread2 = new Thread() {
            public void run() {
                instance2[0] = TransactionManagerServices.attachToServices(KEY1);
            }
        };

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        ServicesInstance serviceInstance1 = instance1[0];
        ServicesInstance serviceInstance2 = instance2[0];
        Assert.assertSame(serviceInstance1, serviceInstance2);
        Assert.assertEquals(KEY1, serviceInstance1.getKey());
        Assert.assertEquals(KEY1, serviceInstance2.getKey());
    }
}
