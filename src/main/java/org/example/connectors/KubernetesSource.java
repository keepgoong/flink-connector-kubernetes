package org.example.connectors;

import com.google.gson.reflect.TypeToken;
import io.kubernetes.client.common.KubernetesListObject;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.informer.ListerWatcher;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.informer.cache.Cache;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.JSON;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.CallGeneratorParams;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.util.Watch;
import okhttp3.OkHttpClient;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.UserCodeClassLoader;
import org.example.connectors.KubernetesSource.DummyCheckpoint;
import org.example.connectors.KubernetesSource.DummySplit;
import org.example.informer.FlinkSharedIndexInformer;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;

public class KubernetesSource
    implements Source<RowData, DummySplit,DummyCheckpoint> {

    private final String configFile;
    private final String sourceType;
    private final String nameSpace;
    private final String fieldSelector;
    private final String labelSelector;
    private final DeserializationSchema<RowData> deserializer;

    public KubernetesSource(
            String configFile,
            String sourceType,
            String nameSpace,
            String fieldSelector,
            String labelSelector,
            DeserializationSchema<RowData> deserializer
    ){
        this.configFile = configFile;
        this.sourceType = sourceType;
        this.nameSpace = nameSpace;
        this.fieldSelector = fieldSelector;
        this.labelSelector = labelSelector;
        this.deserializer = deserializer;
    }

    @Override
    public Boundedness getBoundedness() {
        // 标识stream是否有界
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<DummySplit, DummyCheckpoint> createEnumerator(SplitEnumeratorContext<DummySplit> enumContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator<DummySplit, DummyCheckpoint> restoreEnumerator(SplitEnumeratorContext<DummySplit> enumContext, DummyCheckpoint checkpoint) throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<DummySplit> getSplitSerializer() {
        return new NoOpDummySplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<DummyCheckpoint> getEnumeratorCheckpointSerializer() {
        return null;
    }

    @Override
    public SourceReader<RowData, DummySplit> createReader(SourceReaderContext readerContext) throws Exception {
        System.out.println("createReader");
        Preconditions.checkState(
                readerContext.currentParallelism() == 1,
                "KubernetesSource can only work with a parallelism of 1");
        deserializer.open(
                new DeserializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        return readerContext.metricGroup().addGroup("deserializer");
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return readerContext.getUserCodeClassLoader();
                    }
                }
        );
        return new KubernetesReader();
    }

    public static class DummySplit implements SourceSplit {
        @Override
        public String splitId() {
            return "dummy";
        }
    }
    public static class DummyCheckpoint {}

    public class KubernetesReader implements SourceReader<RowData,DummySplit> {

        public BlockingDeque<Watch.Response> queue;

        @Override
        public void start() {
            queue = new LinkedBlockingDeque<>();
            // 创建并启动对应的informer
            try {
                ApiClient client = ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new FileReader(configFile))).build();
                // 设置client为默认ApiClient
                Configuration.setDefaultApiClient(client);

                CoreV1Api coreV1Api = new CoreV1Api();
                AppsV1Api appsV1Api = new AppsV1Api();
                BatchV1Api batchV1Api = new BatchV1Api();
                ApiClient apiClient = coreV1Api.getApiClient();
                OkHttpClient httpClient = apiClient.getHttpClient().newBuilder().readTimeout(0, TimeUnit.SECONDS).build();
                apiClient.setHttpClient(httpClient);

                // 创建informerFactory并重写SharedIndexInformer方法
                SharedInformerFactory factory = new SharedInformerFactory(apiClient){
                    @Override
                    public synchronized <ApiType extends KubernetesObject, ApiListType extends KubernetesListObject> SharedIndexInformer<ApiType> sharedIndexInformerFor(ListerWatcher<ApiType, ApiListType> listerWatcher, Class<ApiType> apiTypeClass, long resyncPeriodInMillis, BiConsumer<Class<ApiType>, Throwable> exceptionHandler) {
                        SharedIndexInformer<ApiType> informer =
                                new FlinkSharedIndexInformer<>(
                                        apiTypeClass, listerWatcher, resyncPeriodInMillis, new Cache<>(), exceptionHandler,queue);
                        this.informers.putIfAbsent(TypeToken.get(apiTypeClass).getType(), informer);
                        return informer;
                    }
                };

                // 根据订阅的资源类型创建对应的informer
                switch (sourceType){
                    case "Namespace":
                        factory.sharedIndexInformerFor(
                                (CallGeneratorParams params) -> {
                                    return coreV1Api.listNamespaceCall(
                                            null,
                                            null,
                                            null,
                                            fieldSelector,
                                            labelSelector,
                                            null,
                                            params.resourceVersion,
                                            null,
                                            params.timeoutSeconds,
                                            params.watch,
                                            null);
                                },
                                V1Namespace.class,
                                V1NamespaceList.class
                        );
                        break;
                    case "Pod":
                        System.out.println("Pod");
                        factory.sharedIndexInformerFor(
                                (CallGeneratorParams params) -> {
                                    return coreV1Api.listNamespacedPodCall(
                                            nameSpace,
                                            null,
                                            null,
                                            null,
                                            fieldSelector,
                                            labelSelector,
                                            null,
                                            params.resourceVersion,
                                            null,
                                            params.timeoutSeconds,
                                            params.watch,
                                            null);
                                },
                                V1Pod.class,
                                V1PodList.class
                        );
                        break;
                    case "Node":
                        System.out.println("Node");
                        factory.sharedIndexInformerFor(
                                (CallGeneratorParams params) -> {
                                    return coreV1Api.listNodeCall(
                                            null,
                                            null,
                                            null,
                                            fieldSelector,
                                            labelSelector,
                                            null,
                                            params.resourceVersion,
                                            null,
                                            params.timeoutSeconds,
                                            params.watch,
                                            null);
                                },
                                V1Node.class,
                                V1NodeList.class
                        );
                        break;
                    case "Service":
                        System.out.println("Service");
                        factory.sharedIndexInformerFor(
                                (CallGeneratorParams params) -> {
                                    return coreV1Api.listNamespacedServiceCall(
                                            nameSpace,
                                            null,
                                            null,
                                            null,
                                            fieldSelector,
                                            labelSelector,
                                            null,
                                            params.resourceVersion,
                                            null,
                                            params.timeoutSeconds,
                                            params.watch,
                                            null);
                                },
                                V1Service.class,
                                V1ServiceList.class
                        );
                        break;
                    case "Deployment":
                        System.out.println("Service");
                        factory.sharedIndexInformerFor(
                                (CallGeneratorParams params) -> {
                                    return appsV1Api.listNamespacedDeploymentCall(
                                            nameSpace,
                                            null,
                                            null,
                                            null,
                                            fieldSelector,
                                            labelSelector,
                                            null,
                                            params.resourceVersion,
                                            null,
                                            params.timeoutSeconds,
                                            params.watch,
                                            null);
                                },
                                V1Deployment.class,
                                V1DeploymentList.class
                        );
                        break;
                    case "Job":
                        System.out.println("Job");
                        factory.sharedIndexInformerFor(
                                (CallGeneratorParams params) -> {
                                    return batchV1Api.listNamespacedJobCall(
                                            nameSpace,
                                            null,
                                            null,
                                            null,
                                            fieldSelector,
                                            labelSelector,
                                            null,
                                            params.resourceVersion,
                                            null,
                                            params.timeoutSeconds,
                                            params.watch,
                                            null);
                                },
                                V1Job.class,
                                V1JobList.class
                        );
                        break;
                    default:
                        System.out.println("Unsupported resource type!");
                        throw new Exception("Unsupported resource type!");
                }

                // 启动informer
                factory.startAllRegisteredInformers();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
            JSON json = new JSON();
            Watch.Response data = queue.take();
            String serializeData = json.serialize(data);
            System.out.println(serializeData);
            try {
                output.collect(deserializer.deserialize(serializeData.getBytes()));
            } catch (Exception e) {
                System.err.printf("\n" + e);
            }
            return InputStatus.MORE_AVAILABLE;
        }

        @Override
        public List<DummySplit> snapshotState(long checkpointId) {
            return Collections.emptyList();
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return null;
        }

        @Override
        public void addSplits(List<DummySplit> splits) {

        }

        @Override
        public void notifyNoMoreSplits() {

        }

        @Override
        public void close() throws Exception {
            try {
                queue.clear();
            } catch (Throwable t) {
                System.out.println(t);
            }
        }
    }

    private static class NoOpDummySplitSerializer implements SimpleVersionedSerializer<DummySplit> {
        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(DummySplit obj) throws IOException {
            return new byte[0];
        }

        @Override
        public DummySplit deserialize(int version, byte[] serialized) throws IOException {
            return new DummySplit();
        }
    }
}
