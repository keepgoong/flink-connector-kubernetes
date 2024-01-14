package org.example.connectors;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class KubernetesDynamicTableFactory implements DynamicTableSourceFactory {

    // config file path 配置文件路径
    public static final ConfigOption<String> CONFIG_PATH =
            ConfigOptions.key("config-path").stringType().defaultValue("~/.kube/config");

    // source type 资源类型
    public static final ConfigOption<String> SOURCE_TYPE =
            ConfigOptions.key("source-type").stringType().noDefaultValue();

    // namespace 订阅的资源的命名空间
    public static final ConfigOption<String> NAMESPACE =
            ConfigOptions.key("namespace").stringType().noDefaultValue();

    // field selector 字段选择器（过滤资源）
    public static final ConfigOption<String> FIELD_SELECTOR =
            ConfigOptions.key("field-selector").stringType().noDefaultValue();
    // label selector 标签选择器（过滤资源）
    public static final ConfigOption<String> LABEL_SELECTOR =
            ConfigOptions.key("label-selector").stringType().noDefaultValue();

    // connector标识，用于匹配Factory
    public static final String IDENTIFIER = "kubernetes";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(CONFIG_PATH);
        options.add(SOURCE_TYPE);
        options.add(NAMESPACE);
        options.add(FIELD_SELECTOR);
        options.add(LABEL_SELECTOR);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        // 生成helper对象
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        // 获取合适的解码器
        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(DeserializationFormatFactory.class, FactoryUtil.FORMAT);
//        // 验证所有选项
//        helper.validate();

        // 获取验证后的选项
//        final ReadableConfig options = helper.getOptions();
//        final String configPath = options.get(CONFIG_PATH);
//        final String sourceType = options.get(SOURCE_TYPE);
        // 验证并获取所有选项
        Map<String, String> connectorOptions = context.getCatalogTable().getOptions();
        final String configPath = connectorOptions.get("config-path");
        final String sourceType = connectorOptions.get("source-type");
        final String nameSpace = connectorOptions.get("namespace");
        final String fieldSelector = connectorOptions.get("field-selector");
        final String labelSelector = connectorOptions.get("label-selector");

        final DataType producedDataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
        return new KubernetesDynamicTableSource(configPath, sourceType, nameSpace, fieldSelector, labelSelector, decodingFormat, producedDataType);
    }
}
