package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KubernetesConnectorExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String authenticationType = "ClientCertificate";
        boolean ssl = true;
        String caPath = "./ca.txt";
        String clientCapath = "./client-ca.txt";
        String clientKeypath = "./client-key.txt";
        String basePath = "https://10.15.0.21:6443";
        String tokenPath = "./token.txt";
        String configPath = "./config";
        String sourceType = "Pod";
//        String nameSpace = "";
        String nameSpace = "default";
//        String fieldSelector = "status.phase=Running";
//        String labelSelector = "app=nginx";
        String fieldSelector = "";
        String labelSelector = "";


        tEnv.executeSql(
                "CREATE TABLE Nodes (type STRING,object ROW<apiVersion STRING,kind STRING,metadata ROW<annotations MAP<STRING,STRING>,creationTimestamp STRING,deletionGracePeriodSeconds BIGINT,deletionTimestamp STRING,finalizers ARRAY<STRING>,generateName STRING,generation BIGINT,labels Map<STRING,STRING>,managedFields ARRAY<STRING>,name STRING,namespace STRING,ownerReferences ARRAY<STRING>,resourceVersion STRING,selfLink STRING,uid STRING>,spec ROW<activeDeadlineSeconds BIGINT,backoffLimit INT,completionMode STRING,completions INT,manualSelector BOOLEAN,parallelism INT,podFailurePolicy STRING,selector STRING,suspend BOOLEAN,template STRING,ttlSecondsAfterFinished INT>,status ROW<active INT,completedIndexes STRING,completionTime STRING,conditions ARRAY<STRING>,failed INT,ready INT,startTime STRING,succeeded INT,uncountedTerminatedPods STRING>>)\n"
                        + "WITH (\n"
                        + "  'connector' = 'kubernetes',\n"
                        + "  'authentication-type' = '"
                        + authenticationType
                        + "',\n"
                        + "  'base-path' = '"
                        + basePath
                        // config文件路径
                        + "',\n"
                        + "  'config-path' = '"
                        + configPath
                        + "',\n"
                        + "  'ssl' = '"
                        + ssl
                        + "',\n"
                        + "  'ca-path' = '"
                        + caPath
                        + "',\n"
                        + "  'client-ca-path' = '"
                        + clientCapath
                        + "',\n"
                        + "  'client-key-path' = '"
                        + clientKeypath
                        + "',\n"
                        + "  'token-path' = '"
                        + tokenPath
                        + "',\n"
                        + "  'source-type' = '"
                        + sourceType
                        + "',\n"
                        + "  'namespace' = '"
                        + nameSpace
                        + "',\n"
                        + "  'field-selector' = '"
                        + fieldSelector
                        + "',\n"
                        + "  'label-selector' = '"
                        + labelSelector
                        + "',\n"
                        + "  'format' = 'json',\n"
                        + "  'json.fail-on-missing-field' = 'false',\n"
                        + "  'json.ignore-parse-errors' = 'false'\n"
                        + ")");

        final Table result = tEnv.sqlQuery("SELECT type,object FROM Nodes GROUP BY type,object");

        tEnv.toChangelogStream(result).print();

        env.execute();
    }
}
