package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class AccessTokenAuthenticationExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String authenticationType = "AccessToken";
        boolean ssl = true;
        String caPath = "./ca.txt";
        String ca = "-----BEGIN CERTIFICATE-----\n" +
                "MIIC/jCCAeagAwIBAgIBADANBgkqhkiG9w0BAQsFADAVMRMwEQYDVQQDEwprdWJl\n" +
                "cm5ldGVzMB4XDTIzMDcxOTE2MzAzNFoXDTMzMDcxNjE2MzAzNFowFTETMBEGA1UE\n" +
                "AxMKa3ViZXJuZXRlczCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALpg\n" +
                "WYnY92HMCrdNJiGiV/eTfprIWCJXNHXFI/F6VXzrpQauOnEs43XXI1XXd1EsFcrE\n" +
                "RI7Bl2uQ7W6+YxEm4AQ26Bnq9XHkmWenEt+VDifkdfo5mzaXMgG59orOSOoF9NEe\n" +
                "u+Sxm6QQw8iT3xl2MOTQRhIBT5SmiyqG+/K2K3P+qSK/QteH/W/big+UcwmVc8C+\n" +
                "9l4Ilg05HPaFONoPsbubMjoQcsQj3xFJcTd1JjBMPV8K6i1dzVRJU5/abnY+Q8OQ\n" +
                "Nn2OCqDcAN1XesGbv+tpslFENoqeIs+DKI1qltPRaOvcX5CAcg4i7j9JDl+3L/fr\n" +
                "GEvyNAZSjmYlR2FAzW8CAwEAAaNZMFcwDgYDVR0PAQH/BAQDAgKkMA8GA1UdEwEB\n" +
                "/wQFMAMBAf8wHQYDVR0OBBYEFMWpRWqK4tDPYa3OZj5tjnwl3toKMBUGA1UdEQQO\n" +
                "MAyCCmt1YmVybmV0ZXMwDQYJKoZIhvcNAQELBQADggEBALI7y8YkuhUG2a0y8sOH\n" +
                "aw49G2tUDbyA8YmUrCHpWmHA2ZrRyfTAxDPaPhtLdJ99ntDK27seQVKFZmOphZk6\n" +
                "9NeQG2vCayez9pZHAEdmVE/XNb8CDcXqUROooUX+v3Bg3TlvW6iML8r041JqDmbr\n" +
                "4HZfj+2rCwRa21dFNAGrTNqarcYRlnsqBvYcZ4NyuiUzcob+JEPY8JRpyWl3CLaA\n" +
                "Vf3hxVPaPxfSEQkdLWHmYYOhPKZEnl5fn8Jz8D+cE4H93Ferf4gzPNZUEhAnpkya\n" +
                "TJrvHnTlEBBwijflf0daKOmTRZG9/wLvtAYtVh2BmJf8PlR2xG9/KChJW8G5AzvX\n" +
                "k1E=\n" +
                "-----END CERTIFICATE-----\n";
        String basePath = "https://10.15.0.20:6443";
        String tokenPath = "./token.txt";
        String token = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImRmWGhiYWo5OE9fRUxLU2NuV3NhOGROSTRyRlVqNVNScHFDTWxsMWtfVUEifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJkZWZhdWx0Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZWNyZXQubmFtZSI6Im15LWV4dGVybmFsLWFjY2Vzcy10b2tlbiIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50Lm5hbWUiOiJteS1zZXJ2aWNlLWFjY291bnQiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC51aWQiOiJiMDY4MGFlOC02Y2UxLTRiYjUtOGUwZS1lYTQ2YWY3NDEwNzgiLCJzdWIiOiJzeXN0ZW06c2VydmljZWFjY291bnQ6ZGVmYXVsdDpteS1zZXJ2aWNlLWFjY291bnQifQ.VHrH0eLKJH62zggBwkdq9fJxmhZx0gOVK_9H1q24yW3rn0J_YHewWWsL4RfqM71nflRoGdpGfbvPMzECBoA3xMUaS7cAS5n0d0tLynWcIhs6jG9mrfo98MAwlqLYQoKQBhNSZp-hXA0tGgF3rfYXAM4qsHMfu-y-T24qWmQNJVkEPmntUC_COU7GMtqQll5HaINIJsgvpgOvJE6HjfxzH93ipXcH3mzG88VFm-b9_Df0KVSFjrzEi8ZXd6roLeWT41i1cmf2kFLSTPmUh1g_d0XNr076EAvFMHP1hFoRsiFaIsTV_i0LtaVlNhe6MZ7E4iLEDUk9Hnh7mkwDQIGoAg";
        String sourceType = "Pod";
//        String nameSpace = "";
        String nameSpace = "default";
//        String fieldSelector = "status.phase=Running";
//        String labelSelector = "app=nginx";
        String fieldSelector = "";
        String labelSelector = "";


        tEnv.executeSql(
                "CREATE TABLE Pods (type STRING,object ROW<apiVersion STRING,kind STRING,metadata ROW<annotations MAP<STRING,STRING>,creationTimestamp STRING,deletionGracePeriodSeconds BIGINT,deletionTimestamp STRING,finalizers ARRAY<STRING>,generateName STRING, generation BIGINT,labels Map<STRING,STRING>,managedFields ARRAY<STRING>,name STRING,namespace STRING,ownerReferences ARRAY<STRING>,resourceVersion STRING,selfLink STRING,uid STRING>,spec ROW<activeDeadlineSeconds BIGINT,affinity STRING,automountServiceAccountToken BOOLEAN,containers ARRAY<STRING>,dnsConfig STRING,dnsPolicy STRING,enableServiceLinks BOOLEAN,ephemeralContainers ARRAY<STRING>,hostAliases ARRAY<STRING>,hostIPC BOOLEAN,hostNetwork BOOLEAN,hostPID BOOLEAN,hostUsers BOOLEAN,hostname STRING,imagePullSecrets ARRAY<STRING>,initContainers ARRAY<STRING>,nodeName STRING,nodeSelector MAP<STRING,STRING>,os STRING,overhead MAP<STRING,STRING>,preemptionPolicy STRING,priority INT,priorityClassName STRING,readinessGates ARRAY<STRING>,restartPolicy STRING,runtimeClassName STRING,schedulerName STRING,securityContext STRING,serviceAccount STRING,serviceAccountName STRING,setHostnameAsFQDN BOOLEAN,shareProcessNamespace BOOLEAN,subdomain STRING,terminationGracePeriodSeconds BIGINT,tolerations ARRAY<STRING>,topologySpreadConstraints ARRAY<STRING>,volumes ARRAY<STRING>>,status ROW<conditions ARRAY<STRING>,containerStatuses ARRAY<STRING>,ephemeralContainerStatuses ARRAY<STRING>,hostIP STRING,initContainerStatuses ARRAY<STRING>,message STRING,nominatedNodeName STRING,phase STRING,podIP STRING,podIPs ARRAY<STRING>,qosClass STRING,reason STRING,startTime STRING>>)\n"
                        + "WITH (\n"
                        + "  'connector' = 'kubernetes',\n"
                        + "  'authentication-type' = '"
                        + authenticationType
                        + "',\n"
                        + "  'base-path' = '"
                        + basePath
                        + "',\n"
                        + "  'ssl' = '"
                        + ssl
//                        + "',\n"
//                        + "  'ca-path' = '"
//                        + caPath
                        + "',\n"
                        + "  'ca' = '"
                        + ca
//                        + "',\n"
//                        + "  'token-path' = '"
//                        + tokenPath
                        + "',\n"
                        + "  'token' = '"
                        + token
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

        final Table result = tEnv.sqlQuery("SELECT type,object FROM Pods GROUP BY type,object");

        tEnv.toChangelogStream(result).print();

        env.execute();
    }
}
