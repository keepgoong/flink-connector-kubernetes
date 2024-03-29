package org.example.connectors;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class KubernetesDynamicTableSource implements ScanTableSource {

    private final String authenticationType;
    private final String basePath;
    private final Boolean ssl;
    private final String caPath;
    final String clientCaPath;
    final String clientKeyPath;
    private final String tokenPath;
    private final String configFile;
    private final String sourceType;
    private final String nameSpace;
    private final String fieldSelector;
    private final String labelSelector;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private final DataType producedDataType;

    public KubernetesDynamicTableSource(
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
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            DataType producedDataType){
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
        this.decodingFormat = decodingFormat;
        this.producedDataType = producedDataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return decodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
                final DeserializationSchema<RowData> deserializer =
                        decodingFormat.createRuntimeDecoder(
                                runtimeProviderContext, producedDataType);
                final KubernetesSource kubernetesSource = new KubernetesSource(authenticationType, basePath, ssl, caPath, clientCaPath, clientKeyPath, tokenPath, configFile, sourceType, nameSpace, fieldSelector, labelSelector, deserializer);
                return execEnv.fromSource(
                                kubernetesSource, WatermarkStrategy.noWatermarks(), "kubernetesSource")
                        // kubernetesSource can only work with a parallelism of 1.
                        .setParallelism(1);
            }

            @Override
            public boolean isBounded() {
                return false;
            }
        };
    }

    @Override
    public DynamicTableSource copy() {
        return new KubernetesDynamicTableSource(authenticationType, basePath, ssl, caPath, clientCaPath, clientKeyPath, tokenPath, configFile, sourceType, nameSpace, fieldSelector, labelSelector, decodingFormat, producedDataType);
    }

    @Override
    public String asSummaryString() {
        return "Kubernetes Table Source";
    }
}
