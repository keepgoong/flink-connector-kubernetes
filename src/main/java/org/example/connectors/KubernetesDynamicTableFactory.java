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
    // authentication type 认证方式（支持Kubeconfig/ClientCertificate/AccessToken）
    public static final ConfigOption<String>  AUTHENTICATION_TYPE=
            ConfigOptions.key("authentication-type").stringType().defaultValue("Kubeconfig");

    // config file path 配置文件路径（认证方式为Kubeconfig时使用）
    public static final ConfigOption<String> CONFIG_PATH =
            ConfigOptions.key("config-path").stringType().defaultValue("~/.kube/config");

    // base path Api-server地址（例：https://10.15.0.21:6443）
    public static final ConfigOption<String>  BASE_PATH=
            ConfigOptions.key("base-path").stringType().defaultValue("https://localhost:6443");

    // 是否使用SSL认证（true/false）
    public static final ConfigOption<Boolean>  SSL=
            ConfigOptions.key("ssl").booleanType().defaultValue(true);

    // ca file path CA证书文件地址
    public static final ConfigOption<String> CA_PATH =
            ConfigOptions.key("ca-path").stringType().noDefaultValue();

    // client ca file path client CA证书文件地址
    public static final ConfigOption<String> CLIENT_CERTIFICATE_DATA =
            ConfigOptions.key("client-ca-path").stringType().noDefaultValue();

    // client key file path client key文件地址
    public static final ConfigOption<String> CLIENT_KEY_DATA =
            ConfigOptions.key("client-key-path").stringType().noDefaultValue();

    // token file path Token文件地址（认证方式为AccessToken时使用）
    public static final ConfigOption<String> TOKEN_PATH =
            ConfigOptions.key("token-path").stringType().noDefaultValue();

    // source type 资源类型
    public static final ConfigOption<String> SOURCE_TYPE =
            ConfigOptions.key("source-type").stringType().noDefaultValue();

    // namespace 订阅的资源的命名空间
    public static final ConfigOption<String> NAMESPACE =
            ConfigOptions.key("namespace").stringType().defaultValue("");

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
        options.add(AUTHENTICATION_TYPE);
        options.add(SOURCE_TYPE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BASE_PATH);
        options.add(SSL);
        options.add(CA_PATH);
        options.add(CLIENT_CERTIFICATE_DATA);
        options.add(CLIENT_KEY_DATA);
        options.add(TOKEN_PATH);
        options.add(CONFIG_PATH);
        options.add(NAMESPACE);
        options.add(FIELD_SELECTOR);
        options.add(LABEL_SELECTOR);
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
        final String authenticationType = connectorOptions.get("authentication-type");
        final String basePath = connectorOptions.get("base-path");
        final Boolean ssl = Boolean.valueOf(connectorOptions.get("ssl"));
        final String caPath = connectorOptions.get("ca-path");
        final String clientCaPath = connectorOptions.get("client-ca-path");
        final String clientKeyPath = connectorOptions.get("client-key-path");
        final String tokenPath = connectorOptions.get("token-path");
        final String configPath = connectorOptions.get("config-path");
        final String sourceType = connectorOptions.get("source-type");
        final String nameSpace = connectorOptions.get("namespace");
        final String fieldSelector = connectorOptions.get("field-selector");
        final String labelSelector = connectorOptions.get("label-selector");

        final DataType producedDataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
        return new KubernetesDynamicTableSource(authenticationType, basePath, ssl, caPath, clientCaPath, clientKeyPath, tokenPath, configPath, sourceType, nameSpace, fieldSelector, labelSelector, decodingFormat, producedDataType);
    }
}
