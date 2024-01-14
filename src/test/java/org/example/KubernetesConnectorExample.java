package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KubernetesConnectorExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String configPath = "C:/Users/wangjw52547/Downloads/config";
        String sourceType = "Pod";
        String nameSpace = "default";
        String fieldSelector = "status.phase=Running";
        String labelSelector = "app=nginx";


        tEnv.executeSql(
                "CREATE TABLE Nodes (type STRING,object ROW<apiVersion STRING,kind STRING,metadata ROW<annotations MAP<STRING,STRING>,creationTimestamp STRING,deletionGracePeriodSeconds BIGINT,deletionTimestamp STRING,finalizers ARRAY<STRING>,generateName STRING,generation BIGINT,labels Map<STRING,STRING>,managedFields ARRAY<STRING>,name STRING,namespace STRING,ownerReferences ARRAY<STRING>,resourceVersion STRING,selfLink STRING,uid STRING>,spec ROW<activeDeadlineSeconds BIGINT,backoffLimit INT,completionMode STRING,completions INT,manualSelector BOOLEAN,parallelism INT,podFailurePolicy STRING,selector STRING,suspend BOOLEAN,template STRING,ttlSecondsAfterFinished INT>,status ROW<active INT,completedIndexes STRING,completionTime STRING,conditions ARRAY<STRING>,failed INT,ready INT,startTime STRING,succeeded INT,uncountedTerminatedPods STRING>>)\n"
                        + "WITH (\n"
                        + "  'connector' = 'kubernetes',\n"
                        + "  'config-path' = '"
                        + configPath
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
