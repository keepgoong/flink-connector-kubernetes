package org.example.informer;

import io.kubernetes.client.common.KubernetesListObject;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.informer.ListerWatcher;
import io.kubernetes.client.informer.ResourceEventHandler;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.TransformFunc;
import io.kubernetes.client.informer.cache.*;
import io.kubernetes.client.informer.impl.DefaultSharedIndexInformer;
import io.kubernetes.client.util.Watch;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.function.BiConsumer;
import java.util.function.Function;

// 参照DefaultSharedIndexInformer实现，主要修改了handleDeltas方法
public class FlinkSharedIndexInformer <
        ApiType extends KubernetesObject, ApiListType extends KubernetesListObject>
        implements SharedIndexInformer<ApiType> {

    private static final Logger log = LoggerFactory.getLogger(DefaultSharedIndexInformer.class);

    private static final long MINIMUM_RESYNC_PERIOD_MILLIS = 1000L;

    // resyncCheckPeriod is how often we want the reflector's resync timer to fire so it can call
    // shouldResync to check if any of our listeners need a resync.
    private long resyncCheckPeriodMillis;
    // defaultEventHandlerResyncPeriod is the default resync period for any handlers added via
    // AddEventHandler (i.e. they don't specify one and just want to use the shared informer's
    // default
    // value).
    private long defaultEventHandlerResyncPeriod;

    private Indexer<ApiType> indexer;

    private SharedProcessor<ApiType> processor;

    private Controller<ApiType, ApiListType> controller;

    private Thread controllerThread;

    private TransformFunc transform;

    private volatile boolean started = false;
    private volatile boolean stopped = false;

    BlockingDeque<Watch.Response> queue;

    public FlinkSharedIndexInformer(
            Class<ApiType> apiTypeClass,
            ListerWatcher<ApiType, ApiListType> listerWatcher,
            long resyncPeriod
    ) {
        this(apiTypeClass, listerWatcher, resyncPeriod, new Cache<>());
    }

    public FlinkSharedIndexInformer(
            Class<ApiType> apiTypeClass,
            ListerWatcher<ApiType, ApiListType> listerWatcher,
            long resyncPeriod,
            Cache<ApiType> cache) {
        this(
                apiTypeClass,
                listerWatcher,
                resyncPeriod,
                // down-casting should be safe here because one delta FIFO instance only serves one
                // resource
                // type
                new DeltaFIFO(
                        (Function<KubernetesObject, String>) cache.getKeyFunc(),
                        (Cache<KubernetesObject>) cache),
                cache,
                null);
    }

    public FlinkSharedIndexInformer(
            Class<ApiType> apiTypeClass,
            ListerWatcher<ApiType, ApiListType> listerWatcher,
            long resyncPeriod,
            Cache<ApiType> cache,
            BiConsumer<Class<ApiType>, Throwable> exceptionHandler,
            BlockingDeque<Watch.Response> queue) {
        this(
                apiTypeClass,
                listerWatcher,
                resyncPeriod,
                // down-casting should be safe here because one delta FIFO instance only serves one
                // resource
                // type
                new DeltaFIFO(
                        (Function<KubernetesObject, String>) cache.getKeyFunc(),
                        (Cache<KubernetesObject>) cache),
                cache,
                exceptionHandler,
                queue);
    }

    public FlinkSharedIndexInformer(
            Class<ApiType> apiTypeClass,
            ListerWatcher<ApiType, ApiListType> listerWatcher,
            long resyncPeriod,
            DeltaFIFO deltaFIFO,
            Indexer<ApiType> indexer,
            BiConsumer<Class<ApiType>, Throwable> exceptionHandler) {

        this.resyncCheckPeriodMillis = resyncPeriod;
        this.defaultEventHandlerResyncPeriod = resyncPeriod;

        this.processor = new SharedProcessor<>();
        this.indexer = indexer;
        this.controller =
                new Controller<>(
                        apiTypeClass,
                        deltaFIFO,
                        listerWatcher,
                        this::handleDeltas,
                        processor::shouldResync,
                        resyncCheckPeriodMillis,
                        exceptionHandler);
        this.queue = null;

        controllerThread =
                new Thread(controller::run, "informer-controller-" + apiTypeClass.getSimpleName());
    }

    public FlinkSharedIndexInformer(
            Class<ApiType> apiTypeClass,
            ListerWatcher<ApiType, ApiListType> listerWatcher,
            long resyncPeriod,
            DeltaFIFO deltaFIFO,
            Indexer<ApiType> indexer,
            BiConsumer<Class<ApiType>, Throwable> exceptionHandler,
            BlockingDeque<Watch.Response> queue) {

        this.resyncCheckPeriodMillis = resyncPeriod;
        this.defaultEventHandlerResyncPeriod = resyncPeriod;

        this.processor = new SharedProcessor<>();
        this.indexer = indexer;
        this.controller =
                new Controller<>(
                        apiTypeClass,
                        deltaFIFO,
                        listerWatcher,
                        this::handleDeltas,
                        processor::shouldResync,
                        resyncCheckPeriodMillis,
                        exceptionHandler);
        this.queue = queue;

        controllerThread =
                new Thread(controller::run, "informer-controller-" + apiTypeClass.getSimpleName());
    }

    @Override
    public void addEventHandler(ResourceEventHandler<ApiType> handler) {
        addEventHandlerWithResyncPeriod(handler, defaultEventHandlerResyncPeriod);
    }

    /** add event callback with a resync period */
    @Override
    public void addEventHandlerWithResyncPeriod(
            ResourceEventHandler<ApiType> handler, long resyncPeriodMillis) {
        if (stopped) {
            log.info(
                    "DefaultSharedIndexInformer#Handler was not added to shared informer because it has stopped already");
            return;
        }

        if (resyncPeriodMillis > 0) {
            if (resyncPeriodMillis < MINIMUM_RESYNC_PERIOD_MILLIS) {
                log.warn(
                        "DefaultSharedIndexInformer#resyncPeriod {} is too small. Changing it to the minimum allowed rule of {}",
                        resyncPeriodMillis,
                        MINIMUM_RESYNC_PERIOD_MILLIS);
                resyncPeriodMillis = MINIMUM_RESYNC_PERIOD_MILLIS;
            }

            if (resyncPeriodMillis < this.resyncCheckPeriodMillis) {
                if (started) {
                    log.warn(
                            "DefaultSharedIndexInformer#resyncPeriod {} is smaller than resyncCheckPeriod {} and the informer has already started. Changing it to {}",
                            resyncPeriodMillis,
                            resyncCheckPeriodMillis,
                            resyncCheckPeriodMillis);
                    resyncPeriodMillis = resyncCheckPeriodMillis;
                } else {
                    // if the event handler's resyncPeriod is smaller than the current
                    // resyncCheckPeriod,
                    // update resyncCheckPeriod to match resyncPeriod and adjust the resync periods
                    // of all
                    // the listeners accordingly
                    this.resyncCheckPeriodMillis = resyncPeriodMillis;
                }
            }
        }

        ProcessorListener<ApiType> listener =
                new ProcessorListener(
                        handler, determineResyncPeriod(resyncCheckPeriodMillis, this.resyncCheckPeriodMillis));
        if (!started) {
            this.processor.addListener(listener);
            return;
        }

        this.processor.addAndStartListener(listener);
        List<ApiType> objectList = this.indexer.list();
        for (Object item : objectList) {
            listener.add(new ProcessorListener.AddNotification(item));
        }
    }

    @Override
    public String lastSyncResourceVersion() {
        if (!started) {
            return "";
        }

        return this.controller.lastSyncResourceVersion();
    }

    @Override
    public void setTransform(TransformFunc transformFunc) {
        if (started) {
            throw new IllegalStateException("cannot set transform func to a running informer");
        }
        this.transform = transformFunc;
    }

    @Override
    public void run() {
        if (started) {
            return;
        }

        started = true;

        this.processor.run();

        controllerThread.start();
        System.out.println("informer starts running");
    }

    @Override
    public void stop() {
        if (!started) {
            return;
        }

        stopped = true;

        controller.stop();
        controllerThread.interrupt();

        processor.stop();
    }

    @Override
    public boolean hasSynced() {
        // Waiting for controller initialize.
        // Because informer.run() and hasSynced() are called by different threads.
        return controller != null && this.controller.hasSynced();
    }

    /**
     * handleDeltas handles deltas and call processor distribute.
     *
     * @param deltas deltas
     */
    public void handleDeltas(Deque<MutablePair<DeltaFIFO.DeltaType, KubernetesObject>> deltas) {
        if (CollectionUtils.isEmpty(deltas)) {
            return;
        }

        // from oldest to newest
        for (MutablePair<DeltaFIFO.DeltaType, KubernetesObject> delta : deltas) {
            DeltaFIFO.DeltaType deltaType = delta.getLeft();
            KubernetesObject obj = delta.getRight();
            if (transform != null) {
                obj = transform.transform(obj);
            }
            // System.out.println(deltaType);
            switch (deltaType) {
                case Sync:
                case Added:
                case Updated:
                    boolean isSync = deltaType == DeltaFIFO.DeltaType.Sync;
                    Object oldObj = this.indexer.get((ApiType) obj);
                    if (oldObj != null) {
                        this.indexer.update((ApiType) obj);
                        this.processor.distribute(
                                new ProcessorListener.UpdateNotification(oldObj, obj), isSync);
                        if(this.queue != null){
                            this.queue.add(new Watch.Response("UPDATE_BEFORE",oldObj));
                            this.queue.add(new Watch.Response("UPDATE_AFTER",obj));
                        }
                    } else {
                        this.indexer.add((ApiType) obj);
                        this.processor.distribute(new ProcessorListener.AddNotification(obj), isSync);
                        if(this.queue != null) this.queue.add(new Watch.Response("INSERT",obj));
                    }
                    break;
                case Deleted:
                    this.indexer.delete((ApiType) obj);
                    this.processor.distribute(new ProcessorListener.DeleteNotification(obj), false);
                    System.out.println(obj.getKind());
                    if(this.queue != null) this.queue.add(new Watch.Response("DELETE", obj));
                    break;
            }
        }
    }

    @Override
    public void addIndexers(Map<String, Function<ApiType, List<String>>> indexers) {
        if (started) {
            throw new IllegalStateException("cannot add indexers to a running informer");
        }
        indexer.addIndexers(indexers);
    }

    @Override
    public Indexer getIndexer() {
        return this.indexer;
    }

    private long determineResyncPeriod(long desired, long check) {
        if (desired == 0) {
            return desired;
        }
        if (check == 0) {
            return 0;
        }
        return Math.max(desired, check);
    }
}
