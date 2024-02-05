package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ClientCertificateAuthenticationExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String authenticationType = "ClientCertificate";
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
        String clientCapath = "./client-ca.txt";
        String clientCa = "-----BEGIN CERTIFICATE-----\n" +
                "MIIDITCCAgmgAwIBAgIIbTOXFIhdo+swDQYJKoZIhvcNAQELBQAwFTETMBEGA1UE\n" +
                "AxMKa3ViZXJuZXRlczAeFw0yMzA3MTkxNjMwMzRaFw0yNDA3MTgxNjMwMzVaMDQx\n" +
                "FzAVBgNVBAoTDnN5c3RlbTptYXN0ZXJzMRkwFwYDVQQDExBrdWJlcm5ldGVzLWFk\n" +
                "bWluMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAs9YsQttYQOR14SPs\n" +
                "tr1SyiGI7N+sPLfb3YiKyLrw4/0j0Wk72cRP56lyzTZYSgcc5yrOWl1UuCfhXDNd\n" +
                "y4NT3t9tbTxZBFXR1026HcVxtUCcL45E6Th27AP+W3CthjNPLjpETUI88Kw/02CB\n" +
                "mvr24dKixxsvScbDq41UnfSEif5mq7P5uQccWoraxK+YDYEujPplLpZomk6VES8P\n" +
                "dI+RqmV4T9PcTfcALMB5kzEpqgib99WCL6NzQekzoZY1oiss8ZJyqKLuc/VuWqto\n" +
                "5Yrs2H2Ug1+P8oXcHydCdb+9L/n22wfRqgfl0uNSlSWdermPn6N18fKrGb0cnEuS\n" +
                "wbZVywIDAQABo1YwVDAOBgNVHQ8BAf8EBAMCBaAwEwYDVR0lBAwwCgYIKwYBBQUH\n" +
                "AwIwDAYDVR0TAQH/BAIwADAfBgNVHSMEGDAWgBTFqUVqiuLQz2GtzmY+bY58Jd7a\n" +
                "CjANBgkqhkiG9w0BAQsFAAOCAQEAlNTNzZH2dqn/12JW4nhVQvTJpOqJxnMSsrUy\n" +
                "nZ39XXMjNZ5N9joO65874u092qsE5SUAeCJUgDvV6Eb7z1aEbwptXUjG7cE4dL3m\n" +
                "Bg4iYyv9q+y0f9SPNkg5pFuz/Bi37Zm+eAilQXQ0h06UP2derJ2cl09mb/5jVq4n\n" +
                "Km+W6BM1AK48Ni0llInWK1VWCeD3H7ImXxhW25PWua7zxCQyW+jOejF4UWVF+tt+\n" +
                "Ai8JBal/22hZgRDvdTy3uC+0gQzmIay2NbzlxJLKK/vIbgbE1LoSVFzwOU+1bR78\n" +
                "gQamuaFGBzyxIwC9vXVV85QHyCHjHIssPaAcUOJK3RAao3ILkA==\n" +
                "-----END CERTIFICATE-----\n";
        String clientKeypath = "./client-key.txt";
        String clientKey = "-----BEGIN RSA PRIVATE KEY-----\n" +
                "MIIEpQIBAAKCAQEAs9YsQttYQOR14SPstr1SyiGI7N+sPLfb3YiKyLrw4/0j0Wk7\n" +
                "2cRP56lyzTZYSgcc5yrOWl1UuCfhXDNdy4NT3t9tbTxZBFXR1026HcVxtUCcL45E\n" +
                "6Th27AP+W3CthjNPLjpETUI88Kw/02CBmvr24dKixxsvScbDq41UnfSEif5mq7P5\n" +
                "uQccWoraxK+YDYEujPplLpZomk6VES8PdI+RqmV4T9PcTfcALMB5kzEpqgib99WC\n" +
                "L6NzQekzoZY1oiss8ZJyqKLuc/VuWqto5Yrs2H2Ug1+P8oXcHydCdb+9L/n22wfR\n" +
                "qgfl0uNSlSWdermPn6N18fKrGb0cnEuSwbZVywIDAQABAoIBAFX+Mcf+buMI270+\n" +
                "mnB/c1koU/tAsXGPUVlLc2Gs0oeSglAei/oDv1m9UyIBvJIZeJ6pf2cCfgJZP5AB\n" +
                "F/a6454NJJw5YS+Lgf38MuUNhuPu80BiPV1wHJ2bA0PiS3eCjXhDcGmpJM7I7vQ6\n" +
                "j372RpuRRzkCLMiQfPxJZo7pX39EEo7hsyZKenhtKJlKGS2XF2cgiJWSrxckio3t\n" +
                "a0HmYk8aE1ebq+pWDleWYib034jVIj73EhmyuZCaO1EgtUkpSqcQXpU8DlDb04T3\n" +
                "hYiVEUPNBCd1Gj6JXxPyIoU/P1OZysxd58oFXFRZGMEb9SFoIODDzS6SQL5i5Kc5\n" +
                "sSBJIMECgYEA74BMbll4VaJvd5mwqwO0wZsDYflx646sYSxnBi6xCQYtuYmFf7cn\n" +
                "y1mi99mesmYQWKPeusOofN2LvJchOWdfv9nA+i/fgg1fxeuj7B/dsMc6p1Muz4sy\n" +
                "hTBxGHyHoi6XC0+Jb5DmmYvYzZeeys+Gg/0IU6qai8mqWyO/836w7rsCgYEAwDmq\n" +
                "WagmhG208JYBA2yCryDrua+NgO9v7uT+dvjF8YC+rVQHp5zz5VwN7CO+juBV0mYj\n" +
                "LUyayWcYX06WEPrgLUpPhWX4Lrei3055t/KRjzAjUBg2W6JCg7ygL4A0nu2CoP5L\n" +
                "hZGMSuhRbwfH9eYODc0DjOnpcCGxUuT2JJiprDECgYEA3jzSL29sPyJMhtGhYv88\n" +
                "Yo7B887xC+DHSiBWtSDse/A0y761lw5QqdxSYeSY4vfcYTQmQG3AUaWOvgAK/2ZS\n" +
                "LgcUj9OOdfKNFW4UHM5w+1HTtmJ0xHdo+Lg7qbocbb0HwaIbaOAvavmewx/XZGBv\n" +
                "dNh+OozLfb0zDAE/Y+YCrnMCgYEAhRRnN4MOh4hcSGPRd8lcAFrOV9OOJ8GcwMuQ\n" +
                "0FUS1UvItr8CPayPvi6pBN8KQmhVgkBsAiSS1PSnDvSdOEw3f6N+wmACHnXMMMVn\n" +
                "x04MMBGnoP/iQjZnzkR8seYUiCLu2P600lgdxI5qMnA0o60N9cgFuT0H/DWXSkhx\n" +
                "kIyQi0ECgYEA0ypiNX2KcbkcKf52Q5Jd84BYv/nTPFd2PP+UbKrgk1cvAkiMd70Y\n" +
                "wHK3n46xSpMXjFVvLXMm5nLrEB54lg6mXM7rpU+OUr7r0nHJPvQgbEj2P7dafO2h\n" +
                "MnA6NgG33CClvo/mZfnVQW5tYm+PMKH9MpTVz0i8/I0Q8EUivigq4Co=\n" +
                "-----END RSA PRIVATE KEY-----\n";
        String basePath = "https://10.15.0.20:6443";
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
//                        + "  'client-ca-path' = '"
//                        + clientCapath
                        + "',\n"
                        + "  'client-ca' = '"
                        + clientCa
//                        + "',\n"
//                        + "  'client-key-path' = '"
//                        + clientKeypath
                        + "',\n"
                        + "  'client-key' = '"
                        + clientKey
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
