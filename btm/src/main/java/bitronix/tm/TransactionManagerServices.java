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
package bitronix.tm;

import bitronix.tm.journal.Journal;
import bitronix.tm.recovery.Recoverer;
import bitronix.tm.resource.ResourceLoader;
import bitronix.tm.timer.TaskScheduler;
import bitronix.tm.twopc.executor.Executor;
import bitronix.tm.utils.ExceptionAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Container for all BTM key2services.
 * <p>The different key2services available are: {@link BitronixTransactionManager}, {@link BitronixTransactionSynchronizationRegistry}
 * {@link Configuration}, {@link Journal}, {@link TaskScheduler}, {@link ResourceLoader}, {@link Recoverer} and {@link Executor}.
 * They are used in all places of the TM so they must be globally reachable.</p>
 *
 * @author Ludovic Orban
 */
public class TransactionManagerServices {

    private final static Logger log = LoggerFactory.getLogger(TransactionManagerServices.class);

    public static final String DEFAULT_KEY = "DEFAULT_KEY";

    private static final Object MAPS_LOCK = new Object();

    private static ConcurrentHashMap<String, ServicesInstance> key2services = new ConcurrentHashMap<String, ServicesInstance>();
    private static ThreadLocal<ServicesInstance> servicesInstances = new ThreadLocal<ServicesInstance>();

    public static ServicesInstance attachToServices(String key) {
        final ServicesInstance currentInstance = servicesInstances.get();
        if (currentInstance != null) {
            log.warn(String.format("Thread %s is trying to attach itself to services twice. Key=%s", Thread.currentThread().getName(), key));
            if (!key.equals(currentInstance.getKey())) {
                throw new IllegalArgumentException(String.format("Thread %s is trying to attach itself to another key %s. Current key=%s",
                        Thread.currentThread().getName(), key, currentInstance.getKey()));

            }

            return currentInstance;
        }

        final ServicesInstance newInstance = new ServicesInstance(key);
        final ServicesInstance oldInstance = key2services.putIfAbsent(key, newInstance);
        final ServicesInstance instance = oldInstance == null ? newInstance : oldInstance;
        servicesInstances.set(instance);
        log.info(String.format("Thread %s is attached to Bitronix instance %s", Thread.currentThread().getName(), key));
        return instance;
    }

    public static void detachFromServices() {
        ServicesInstance instance = servicesInstances.get();
        if (instance == null) {
            log.warn(String.format("Thread %s is trying to detach from Bitronix services while it's not attached at all",
                    Thread.currentThread().getName()));
            return;
        }
        servicesInstances.remove();
        log.info(String.format("Thread %s is detached from Bitronix instance %s", Thread.currentThread().getName(), instance.getKey()));
    }

    public static Enumeration<String> getAllInstancesKeys() {
        return key2services.keys();
    }

    public static ServicesInstance getAttachedServices() {
        return servicesInstances.get();
    }

    public static boolean isAttachedToServices() {
        return servicesInstances.get() != null;
    }

    public static ServicesInstance getAttachedServicesOrDefault() {
        final ServicesInstance attachedServices = getAttachedServices();
        if (attachedServices == null)
            return attachToServices(DEFAULT_KEY);
        return attachedServices;
    }

    /**
     * Create an initialized transaction manager.
     * @return the transaction manager.
     */
    public static BitronixTransactionManager getTransactionManager() {
        return getAttachedServicesOrDefault().getTransactionManager();
    }

    /**
     * Create the JTA 1.1 TransactionSynchronizationRegistry.
     * @return the TransactionSynchronizationRegistry.
     */
    public static BitronixTransactionSynchronizationRegistry getTransactionSynchronizationRegistry() {
        return getAttachedServicesOrDefault().getTransactionSynchronizationRegistry();
    }

    /**
     * Create the configuration of all the components of the transaction manager.
     * @return the global configuration.
     */
    public static Configuration getConfiguration() {
        return getAttachedServicesOrDefault().getConfiguration();
    }

    /**
     * Create the transactions journal.
     * @return the transactions journal.
     */
    public static Journal getJournal() {
        return getAttachedServicesOrDefault().getJournal();
    }

    /**
     * Create the task scheduler.
     * @return the task scheduler.
     */
    public static TaskScheduler getTaskScheduler() {
        return getAttachedServicesOrDefault().getTaskScheduler();
    }

    /**
     * Create the resource loader.
     * @return the resource loader.
     */
    public static ResourceLoader getResourceLoader() {
        return getAttachedServicesOrDefault().getResourceLoader();
    }

    /**
     * Create the transaction recoverer.
     * @return the transaction recoverer.
     */
    public static Recoverer getRecoverer() {
        return getAttachedServicesOrDefault().getRecoverer();
    }

    /**
     * Create the 2PC executor.
     * @return the 2PC executor.
     */
    public static Executor getExecutor() {
        return getAttachedServicesOrDefault().getExecutor();
    }

    /**
     * Create the exception analyzer.
     * @return the exception analyzer.
     */
   public static ExceptionAnalyzer getExceptionAnalyzer() {
       return getAttachedServicesOrDefault().getExceptionAnalyzer();
   }

    /**
     * Check if the transaction manager has started.
     * @return true if the transaction manager has started.
     */
    public static boolean isTransactionManagerRunning() {
        return getAttachedServicesOrDefault().isTransactionManagerRunning();
    }

    /**
     * Check if the task scheduler has started.
     * @return true if the task scheduler has started.
     */
    public static boolean isTaskSchedulerRunning() {
        return getAttachedServicesOrDefault().isTaskSchedulerRunning();
    }

    /**
     * Clear services references. Called at the end of the shutdown procedure.
     */
    protected static void clear() {
        ServicesInstance attachedServices = getAttachedServices();
        if (attachedServices != null)
            attachedServices.clear();
        else
            log.warn(String.format("Thread %s is trying to clear services while it's not attached to any", Thread.currentThread().getName()));
    }
}
