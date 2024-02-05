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
import io.kubernetes.client.util.credentials.AccessTokenAuthentication;
import io.kubernetes.client.util.credentials.ClientCertificateAuthentication;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;
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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;

public class KubernetesSource
    implements Source<RowData, DummySplit,DummyCheckpoint> {

    private final String authenticationType;
    private final String basePath;
    private Boolean ssl;
    private String caPath;
    final String clientCaPath;
    final String clientKeyPath;
    private String tokenPath;
    private final String configFile;
    private final String sourceType;
    private final String nameSpace;
    private final String fieldSelector;
    private final String labelSelector;
    private SharedInformerFactory informerFactory;
    private final DeserializationSchema<RowData> deserializer;

    public KubernetesSource(
            String authenticationType,
            String basePath,
            Boolean ssl,
            String caPath,
            String clientCaPath,
            String clientKeyPath,
            String tokenPath,
            String configFile,
            String sourceType,
            String nameSpace,
            String fieldSelector,
            String labelSelector,
            DeserializationSchema<RowData> deserializer
    ){
        this.authenticationType = authenticationType;
        this.basePath =  basePath;
        this.ssl = ssl;
        this.caPath = caPath;
        this.clientCaPath = clientCaPath;
        this.clientKeyPath = clientKeyPath;
        this.tokenPath = tokenPath;
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
                ApiClient client;
                // 根据不同的认证方式创建client
                switch(authenticationType){
                    case "Kubeconfig":
                        System.out.println("authenticationType : Kubeconfig");
                        client = ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new FileReader(configFile))).build();
                        // 设置client为默认ApiClient
                        Configuration.setDefaultApiClient(client);
                        break;
                    case "ClientCertificate":
                        System.out.println("authenticationType : ClientCertificate");
                        if(ssl){
                            client = new ClientBuilder()
                                    .setCertificateAuthority(Files.readAllBytes(Paths.get(caPath)))
                                    .setBasePath(basePath)
                                    .setAuthentication(new ClientCertificateAuthentication(Files.readAllBytes(Paths.get(clientCaPath)), Files.readAllBytes(Paths.get(clientKeyPath))))
                                    .setVerifyingSsl(true)
                                    .build();
                        }
                        else{
                            client = new ClientBuilder()
                                    .setBasePath(basePath)
                                    .setAuthentication(new ClientCertificateAuthentication(Files.readAllBytes(Paths.get(clientCaPath)), Files.readAllBytes(Paths.get(clientKeyPath))))
                                    .setVerifyingSsl(false)
                                    .build();
                        }
                        // 设置client为默认ApiClient
                        Configuration.setDefaultApiClient(client);
                        break;
                    case "AccessToken":
                        System.out.println("authenticationType : AccessToken");
                        if(ssl){
                            System.out.println("use ssl!");
                            client = new ClientBuilder()
                                    .setCertificateAuthority(Files.readAllBytes(Paths.get(caPath)))
                                    .setBasePath(basePath)
                                    .setAuthentication(new AccessTokenAuthentication(new BufferedReader(new FileReader(tokenPath)).readLine()))
                                    .setVerifyingSsl(true)
                                    .build();
                        }
                        else{
                            System.out.println("no ssl!");
                            client = new ClientBuilder()
                                    .setBasePath(basePath)
                                    .setAuthentication(new AccessTokenAuthentication(new BufferedReader(new FileReader(tokenPath)).readLine()))
                                    .setVerifyingSsl(false)
                                    .build();
                        }
                        // 设置client为默认ApiClient
                        Configuration.setDefaultApiClient(client);
                        break;
                    default:
                        System.out.println("Unsupported authentication type!");
                        throw new Exception("Unsupported authentication type!");
                }

                CoreV1Api coreV1Api = new CoreV1Api();
                AppsV1Api appsV1Api = new AppsV1Api();
                BatchV1Api batchV1Api = new BatchV1Api();

                ApiClient coreV1ApiClient = coreV1Api.getApiClient();
                OkHttpClient coreV1HttpClient = coreV1ApiClient.getHttpClient().newBuilder().readTimeout(0, TimeUnit.SECONDS).build();
                coreV1ApiClient.setHttpClient(coreV1HttpClient);

                ApiClient appsV1ApiClient = appsV1Api.getApiClient();
                OkHttpClient appsV1HttpClient = appsV1ApiClient.getHttpClient().newBuilder().readTimeout(0, TimeUnit.SECONDS).build();
                appsV1ApiClient.setHttpClient(appsV1HttpClient);

                ApiClient batchV1ApiClient = batchV1Api.getApiClient();
                OkHttpClient batchV1HttpClient = batchV1ApiClient.getHttpClient().newBuilder().readTimeout(0, TimeUnit.SECONDS).build();
                batchV1ApiClient.setHttpClient(batchV1HttpClient);


                // 创建informerFactory并重写SharedIndexInformer方法
                informerFactory = new SharedInformerFactory(client){
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
                        System.out.println("sourceType : Namespace");
                        informerFactory.sharedIndexInformerFor(
                                (CallGeneratorParams params) -> {
                                    return coreV1Api.listNamespaceCall(
                                            null,
                                            null,
                                            null,
                                            StringUtils.isEmpty(fieldSelector) ? null : fieldSelector,
                                            StringUtils.isEmpty(fieldSelector) ? null : fieldSelector,
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
                        System.out.println("sourceType : Pod");
                        informerFactory.sharedIndexInformerFor(
                                (CallGeneratorParams params) -> {
                                    return coreV1Api.listNamespacedPodCall(
                                            nameSpace,
                                            null,
                                            null,
                                            null,
                                            StringUtils.isEmpty(fieldSelector) ? null : fieldSelector,
                                            StringUtils.isEmpty(fieldSelector) ? null : fieldSelector,
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
                        System.out.println("sourceType : Node");
                        informerFactory.sharedIndexInformerFor(
                                (CallGeneratorParams params) -> {
                                    return coreV1Api.listNodeCall(
                                            null,
                                            null,
                                            null,
                                            StringUtils.isEmpty(fieldSelector) ? null : fieldSelector,
                                            StringUtils.isEmpty(fieldSelector) ? null : fieldSelector,
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
                        System.out.println("sourceType : Service");
                        informerFactory.sharedIndexInformerFor(
                                (CallGeneratorParams params) -> {
                                    return coreV1Api.listNamespacedServiceCall(
                                            nameSpace,
                                            null,
                                            null,
                                            null,
                                            StringUtils.isEmpty(fieldSelector) ? null : fieldSelector,
                                            StringUtils.isEmpty(fieldSelector) ? null : fieldSelector,
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
                        System.out.println("sourceType : Deployment");
                        informerFactory.sharedIndexInformerFor(
                                (CallGeneratorParams params) -> {
                                    return appsV1Api.listNamespacedDeploymentCall(
                                            nameSpace,
                                            null,
                                            null,
                                            null,
                                            StringUtils.isEmpty(fieldSelector) ? null : fieldSelector,
                                            StringUtils.isEmpty(fieldSelector) ? null : fieldSelector,
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
                        System.out.println("sourceType : Job");
                        informerFactory.sharedIndexInformerFor(
                                (CallGeneratorParams params) -> {
                                    return batchV1Api.listNamespacedJobCall(
                                            nameSpace,
                                            null,
                                            null,
                                            null,
                                            StringUtils.isEmpty(fieldSelector) ? null : fieldSelector,
                                            StringUtils.isEmpty(fieldSelector) ? null : fieldSelector,
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
                informerFactory.startAllRegisteredInformers();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
            JSON json = new JSON();
            Watch.Response data = queue.take();
            String serializeData = json.serialize(data);
            //System.out.println("data:" + serializeData);
            try {
                output.collect(deserializer.deserialize(serializeData.getBytes()));
                //System.out.println("byte:" + serializeData.getBytes());
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
                informerFactory.stopAllRegisteredInformers();
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
