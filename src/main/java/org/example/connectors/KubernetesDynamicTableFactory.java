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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class KubernetesDynamicTableFactory implements DynamicTableSourceFactory {
    // 认证方式（支持Kubeconfig/ClientCertificate/AccessToken）
    public static final ConfigOption<String>  AUTHENTICATION_TYPE=
            ConfigOptions.key("authentication-type").stringType().defaultValue("Kubeconfig");

    // config配置文件路径（认证方式为Kubeconfig时使用）
    public static final ConfigOption<String> CONFIG_PATH =
            ConfigOptions.key("config-path").stringType().defaultValue("src/main/resources/data/config.txt");

    // config文件内容（写入src/main/resources/data/config.txt）
    public static final ConfigOption<String> CONFIG =
            ConfigOptions.key("config").stringType().noDefaultValue();

    // Api-server地址（例：https://10.15.0.21:6443）
    public static final ConfigOption<String>  BASE_PATH=
            ConfigOptions.key("base-path").stringType().defaultValue("https://localhost:6443");

    // 是否使用SSL认证（true/false）
    public static final ConfigOption<Boolean>  SSL=
            ConfigOptions.key("ssl").booleanType().defaultValue(true);

    // CA证书文件地址
    public static final ConfigOption<String> CA_PATH =
            ConfigOptions.key("ca-path").stringType().noDefaultValue();

    // CA证书文件内容（写入src/main/resources/data/ca.txt）
    public static final ConfigOption<String> CA =
            ConfigOptions.key("ca").stringType().noDefaultValue();

    // 客户端CA证书文件地址
    public static final ConfigOption<String> CLIENT_CERTIFICATE_DATA =
            ConfigOptions.key("client-ca-path").stringType().noDefaultValue();

    // 客户端ca证书内容（写入src/main/resources/data/client-ca.txt）
    public static final ConfigOption<String> CLIENT_CA =
            ConfigOptions.key("client-ca").stringType().noDefaultValue();

    // 客户端key文件地址
    public static final ConfigOption<String> CLIENT_KEY_DATA =
            ConfigOptions.key("client-key-path").stringType().noDefaultValue();

    // 客户端key文件内容（写入src/main/resources/data/client-key.txt）
    public static final ConfigOption<String> CLIENT_KEY =
            ConfigOptions.key("client-key").stringType().noDefaultValue();

    // Token文件地址（认证方式为AccessToken时使用）
    public static final ConfigOption<String> TOKEN_PATH =
            ConfigOptions.key("token-path").stringType().noDefaultValue();

    // Token文件内容（写入src/main/resources/data/token.txt）
    public static final ConfigOption<String> TOKEN =
            ConfigOptions.key("token").stringType().noDefaultValue();

    // 订阅的资源类型（例：Pod,Node,Namespace等）
    public static final ConfigOption<String> SOURCE_TYPE =
            ConfigOptions.key("source-type").stringType().noDefaultValue();

    // 指定订阅资源的命名空间
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
        options.add(CA);
        options.add(CLIENT_CERTIFICATE_DATA);
        options.add(CLIENT_CA);
        options.add(CLIENT_KEY_DATA);
        options.add(CLIENT_KEY);
        options.add(TOKEN_PATH);
        options.add(TOKEN);
        options.add(CONFIG_PATH);
        options.add(CONFIG);
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

        // 验证并获取所有选项
        Map<String, String> connectorOptions = context.getCatalogTable().getOptions();
        final String authenticationType = connectorOptions.get("authentication-type") != null ? connectorOptions.get("authentication-type") : "Kubeconfig";
        final String basePath = connectorOptions.get("base-path") != null ? connectorOptions.get("base-path") : "";
        final Boolean ssl = connectorOptions.get("ssl") != null ? Boolean.valueOf(connectorOptions.get("ssl")) : false;
        if(ssl && connectorOptions.get("ca") == null && connectorOptions.get("ca-path") == null){
            // System.out.println("ssl为true，但是没有传入ca证书");
            throw new RuntimeException("ssl option is true but don't have CA!");
        }
        if(Objects.equals(authenticationType, "Kubeconfig") && (connectorOptions.get("config") == null && connectorOptions.get("config-path") == null)){
            // System.out.println("认证类型为Kubeconfig，但是没有传入config");
            throw new RuntimeException("authenticationType option is Kubeconfig but don't pass in the config,you need to pass in a config or config-path!");
        }
        if(Objects.equals(authenticationType, "ClientCertificate") && (
                (connectorOptions.get("client-ca") == null && connectorOptions.get("client-ca-path") == null) ||
                        (connectorOptions.get("client-key") == null && connectorOptions.get("client-key-path") == null))
        ){
            // System.out.println("认证类型为ClientCertificate，但是没有传入client-ca或者client-key");
            throw new RuntimeException("authenticationType option is ClientCertificate but don't pass in the client-ca or client-key!");
        }
        if(Objects.equals(authenticationType, "AccessToken") && (connectorOptions.get("token") == null && connectorOptions.get("token-path") == null)){
            // System.out.println("认证类型为AccessToken，但是没有传入token");
            throw new RuntimeException("authenticationType option is AccessToken but don't pass in the token,you need to pass in a token or token-path!");
        }
        final String configPath = getFilePath(connectorOptions.get("config"),connectorOptions.get("config-path"),"src/main/resources/data/config.txt");
        final String caPath = getFilePath(connectorOptions.get("ca"),connectorOptions.get("ca-path"),"src/main/resources/data/ca.txt");
        final String clientCaPath = getFilePath(connectorOptions.get("client-ca"),connectorOptions.get("client-ca-path"),"src/main/resources/data/client-ca.txt");
        final String clientKeyPath = getFilePath(connectorOptions.get("client-key"),connectorOptions.get("client-key-path"),"src/main/resources/data/client-key.txt");
        final String tokenPath = getFilePath(connectorOptions.get("token"),connectorOptions.get("token-path"),"src/main/resources/data/token.txt");
        final String sourceType = connectorOptions.get("source-type") != null ? connectorOptions.get("source-type") : "Pod";
        final String nameSpace = connectorOptions.get("namespace") != null ? connectorOptions.get("namespace") : "default";
        final String fieldSelector = connectorOptions.get("field-selector");
        final String labelSelector = connectorOptions.get("label-selector");

        final DataType producedDataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
        return new KubernetesDynamicTableSource(authenticationType, basePath, ssl, caPath, clientCaPath, clientKeyPath, tokenPath, configPath, sourceType, nameSpace, fieldSelector, labelSelector, decodingFormat, producedDataType);
    }

    String getFilePath(String data,String path,String defaultPath){
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(defaultPath))) {
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(data != null){
            // System.out.println(defaultPath + "文件写入");
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(defaultPath))) {
                writer.write(data);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return defaultPath;
        }
        else{
            if(path == null) {
                // System.out.println("使用默认路径:" + defaultPath);
                return defaultPath;
            }
            else{
                // System.out.println("使用传入的路径：" + path);
                return path;
            }
        }
    }
}
