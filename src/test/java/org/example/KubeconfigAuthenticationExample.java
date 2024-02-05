package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KubeconfigAuthenticationExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 指定认证方式（支持Kubeconfig/ClientCertificate/AccessToken）
        String authenticationType = "Kubeconfig";
        // Kubeconfig认证方式需要config文件内容，传入文件路径或者文件内容都可
        // config 文件路径
        String configPath = "./config.txt";
        // config 文件内容
        String config = "apiVersion: v1\n" +
                "clusters:\n" +
                "- cluster:\n" +
                "    certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUMvakNDQWVhZ0F3SUJBZ0lCQURBTkJna3Foa2lHOXcwQkFRc0ZBREFWTVJNd0VRWURWUVFERXdwcmRXSmwKY201bGRHVnpNQjRYRFRJek1EY3hPVEUyTXpBek5Gb1hEVE16TURjeE5qRTJNekF6TkZvd0ZURVRNQkVHQTFVRQpBeE1LYTNWaVpYSnVaWFJsY3pDQ0FTSXdEUVlKS29aSWh2Y05BUUVCQlFBRGdnRVBBRENDQVFvQ2dnRUJBTHBnCldZblk5MkhNQ3JkTkppR2lWL2VUZnBySVdDSlhOSFhGSS9GNlZYenJwUWF1T25FczQzWFhJMVhYZDFFc0ZjckUKUkk3QmwydVE3VzYrWXhFbTRBUTI2Qm5xOVhIa21XZW5FdCtWRGlma2RmbzVtemFYTWdHNTlvck9TT29GOU5FZQp1K1N4bTZRUXc4aVQzeGwyTU9UUVJoSUJUNVNtaXlxRysvSzJLM1ArcVNLL1F0ZUgvVy9iaWcrVWN3bVZjOEMrCjlsNElsZzA1SFBhRk9Ob1BzYnViTWpvUWNzUWozeEZKY1RkMUpqQk1QVjhLNmkxZHpWUkpVNS9hYm5ZK1E4T1EKTm4yT0NxRGNBTjFYZXNHYnYrdHBzbEZFTm9xZUlzK0RLSTFxbHRQUmFPdmNYNUNBY2c0aTdqOUpEbCszTC9mcgpHRXZ5TkFaU2ptWWxSMkZBelc4Q0F3RUFBYU5aTUZjd0RnWURWUjBQQVFIL0JBUURBZ0trTUE4R0ExVWRFd0VCCi93UUZNQU1CQWY4d0hRWURWUjBPQkJZRUZNV3BSV3FLNHREUFlhM09aajV0am53bDN0b0tNQlVHQTFVZEVRUU8KTUF5Q0NtdDFZbVZ5Ym1WMFpYTXdEUVlKS29aSWh2Y05BUUVMQlFBRGdnRUJBTEk3eThZa3VoVUcyYTB5OHNPSAphdzQ5RzJ0VURieUE4WW1VckNIcFdtSEEyWnJSeWZUQXhEUGFQaHRMZEo5OW50REsyN3NlUVZLRlptT3BoWms2CjlOZVFHMnZDYXllejlwWkhBRWRtVkUvWE5iOENEY1hxVVJPb29VWCt2M0JnM1Rsdlc2aU1MOHIwNDFKcURtYnIKNEhaZmorMnJDd1JhMjFkRk5BR3JUTnFhcmNZUmxuc3FCdlljWjROeXVpVXpjb2IrSkVQWThKUnB5V2wzQ0xhQQpWZjNoeFZQYVB4ZlNFUWtkTFdIbVlZT2hQS1pFbmw1Zm44Sno4RCtjRTRIOTNGZXJmNGd6UE5aVUVoQW5wa3lhClRKcnZIblRsRUJCd2lqZmxmMGRhS09tVFJaRzkvd0x2dEFZdFZoMkJtSmY4UGxSMnhHOS9LQ2hKVzhHNUF6dlgKazFFPQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==\n" +
                "    server: https://10.15.0.20:6443\n" +
                "  name: kubernetes\n" +
                "contexts:\n" +
                "- context:\n" +
                "    cluster: kubernetes\n" +
                "    user: kubernetes-admin\n" +
                "  name: kubernetes-admin@kubernetes\n" +
                "current-context: kubernetes-admin@kubernetes\n" +
                "kind: Config\n" +
                "preferences: {}\n" +
                "users:\n" +
                "- name: kubernetes-admin\n" +
                "  user:\n" +
                "    client-certificate-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURJVENDQWdtZ0F3SUJBZ0lJYlRPWEZJaGRvK3N3RFFZSktvWklodmNOQVFFTEJRQXdGVEVUTUJFR0ExVUUKQXhNS2EzVmlaWEp1WlhSbGN6QWVGdzB5TXpBM01Ua3hOak13TXpSYUZ3MHlOREEzTVRneE5qTXdNelZhTURReApGekFWQmdOVkJBb1REbk41YzNSbGJUcHRZWE4wWlhKek1Sa3dGd1lEVlFRREV4QnJkV0psY201bGRHVnpMV0ZrCmJXbHVNSUlCSWpBTkJna3Foa2lHOXcwQkFRRUZBQU9DQVE4QU1JSUJDZ0tDQVFFQXM5WXNRdHRZUU9SMTRTUHMKdHIxU3lpR0k3TitzUExmYjNZaUt5THJ3NC8wajBXazcyY1JQNTZseXpUWllTZ2NjNXlyT1dsMVV1Q2ZoWEROZAp5NE5UM3Q5dGJUeFpCRlhSMTAyNkhjVnh0VUNjTDQ1RTZUaDI3QVArVzNDdGhqTlBManBFVFVJODhLdy8wMkNCCm12cjI0ZEtpeHhzdlNjYkRxNDFVbmZTRWlmNW1xN1A1dVFjY1dvcmF4SytZRFlFdWpQcGxMcFpvbWs2VkVTOFAKZEkrUnFtVjRUOVBjVGZjQUxNQjVrekVwcWdpYjk5V0NMNk56UWVrem9aWTFvaXNzOFpKeXFLTHVjL1Z1V3F0bwo1WXJzMkgyVWcxK1A4b1hjSHlkQ2RiKzlML24yMndmUnFnZmwwdU5TbFNXZGVybVBuNk4xOGZLckdiMGNuRXVTCndiWlZ5d0lEQVFBQm8xWXdWREFPQmdOVkhROEJBZjhFQkFNQ0JhQXdFd1lEVlIwbEJBd3dDZ1lJS3dZQkJRVUgKQXdJd0RBWURWUjBUQVFIL0JBSXdBREFmQmdOVkhTTUVHREFXZ0JURnFVVnFpdUxRejJHdHptWStiWTU4SmQ3YQpDakFOQmdrcWhraUc5dzBCQVFzRkFBT0NBUUVBbE5UTnpaSDJkcW4vMTJKVzRuaFZRdlRKcE9xSnhuTVNzclV5Cm5aMzlYWE1qTlo1Tjlqb082NTg3NHUwOTJxc0U1U1VBZUNKVWdEdlY2RWI3ejFhRWJ3cHRYVWpHN2NFNGRMM20KQmc0aVl5djlxK3kwZjlTUE5rZzVwRnV6L0JpMzdabStlQWlsUVhRMGgwNlVQMmRlckoyY2wwOW1iLzVqVnE0bgpLbStXNkJNMUFLNDhOaTBsbEluV0sxVldDZUQzSDdJbVh4aFcyNVBXdWE3enhDUXlXK2pPZWpGNFVXVkYrdHQrCkFpOEpCYWwvMjJoWmdSRHZkVHkzdUMrMGdRem1JYXkyTmJ6bHhKTEtLL3ZJYmdiRTFMb1NWRnp3T1UrMWJSNzgKZ1FhbXVhRkdCenl4SXdDOXZYVlY4NVFIeUNIakhJc3NQYUFjVU9KSzNSQWFvM0lMa0E9PQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==\n" +
                "    client-key-data: LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQpNSUlFcFFJQkFBS0NBUUVBczlZc1F0dFlRT1IxNFNQc3RyMVN5aUdJN04rc1BMZmIzWWlLeUxydzQvMGowV2s3CjJjUlA1Nmx5elRaWVNnY2M1eXJPV2wxVXVDZmhYRE5keTROVDN0OXRiVHhaQkZYUjEwMjZIY1Z4dFVDY0w0NUUKNlRoMjdBUCtXM0N0aGpOUExqcEVUVUk4OEt3LzAyQ0JtdnIyNGRLaXh4c3ZTY2JEcTQxVW5mU0VpZjVtcTdQNQp1UWNjV29yYXhLK1lEWUV1alBwbExwWm9tazZWRVM4UGRJK1JxbVY0VDlQY1RmY0FMTUI1a3pFcHFnaWI5OVdDCkw2TnpRZWt6b1pZMW9pc3M4Wkp5cUtMdWMvVnVXcXRvNVlyczJIMlVnMStQOG9YY0h5ZENkYis5TC9uMjJ3ZlIKcWdmbDB1TlNsU1dkZXJtUG42TjE4ZktyR2IwY25FdVN3YlpWeXdJREFRQUJBb0lCQUZYK01jZitidU1JMjcwKwptbkIvYzFrb1UvdEFzWEdQVVZsTGMyR3Mwb2VTZ2xBZWkvb0R2MW05VXlJQnZKSVplSjZwZjJjQ2ZnSlpQNUFCCkYvYTY0NTROSkp3NVlTK0xnZjM4TXVVTmh1UHU4MEJpUFYxd0hKMmJBMFBpUzNlQ2pYaERjR21wSk03STd2UTYKajM3MlJwdVJSemtDTE1pUWZQeEpabzdwWDM5RUVvN2hzeVpLZW5odEtKbEtHUzJYRjJjZ2lKV1NyeGNraW8zdAphMEhtWWs4YUUxZWJxK3BXRGxlV1lpYjAzNGpWSWo3M0VobXl1WkNhTzFFZ3RVa3BTcWNRWHBVOERsRGIwNFQzCmhZaVZFVVBOQkNkMUdqNkpYeFB5SW9VL1AxT1p5c3hkNThvRlhGUlpHTUViOVNGb0lPRER6UzZTUUw1aTVLYzUKc1NCSklNRUNnWUVBNzRCTWJsbDRWYUp2ZDVtd3F3TzB3WnNEWWZseDY0NnNZU3huQmk2eENRWXR1WW1GZjdjbgp5MW1pOTltZXNtWVFXS1BldXNPb2ZOMkx2SmNoT1dkZnY5bkEraS9mZ2cxZnhldWo3Qi9kc01jNnAxTXV6NHN5CmhUQnhHSHlIb2k2WEMwK0piNURtbVl2WXpaZWV5cytHZy8wSVU2cWFpOG1xV3lPLzgzNnc3cnNDZ1lFQXdEbXEKV2FnbWhHMjA4SllCQTJ5Q3J5RHJ1YStOZ085djd1VCtkdmpGOFlDK3JWUUhwNXp6NVZ3TjdDTytqdUJWMG1ZagpMVXlheVdjWVgwNldFUHJnTFVwUGhXWDRMcmVpMzA1NXQvS1JqekFqVUJnMlc2SkNnN3lnTDRBMG51MkNvUDVMCmhaR01TdWhSYndmSDllWU9EYzBEak9ucGNDR3hVdVQySkppcHJERUNnWUVBM2p6U0wyOXNQeUpNaHRHaFl2ODgKWW83Qjg4N3hDK0RIU2lCV3RTRHNlL0EweTc2MWx3NVFxZHhTWWVTWTR2ZmNZVFFtUUczQVVhV092Z0FLLzJaUwpMZ2NVajlPT2RmS05GVzRVSE01dysxSFR0bUoweEhkbytMZzdxYm9jYmIwSHdhSWJhT0F2YXZtZXd4L1haR0J2CmROaCtPb3pMZmIwekRBRS9ZK1lDcm5NQ2dZRUFoUlJuTjRNT2g0aGNTR1BSZDhsY0FGck9WOU9PSjhHY3dNdVEKMEZVUzFVdkl0cjhDUGF5UHZpNnBCTjhLUW1oVmdrQnNBaVNTMVBTbkR2U2RPRXczZjZOK3dtQUNIblhNTU1Wbgp4MDRNTUJHbm9QL2lRalpuemtSOHNlWVVpQ0x1MlA2MDBsZ2R4STVxTW5BMG82ME45Y2dGdVQwSC9EV1hTa2h4CmtJeVFpMEVDZ1lFQTB5cGlOWDJLY2JrY0tmNTJRNUpkODRCWXYvblRQRmQyUFArVWJLcmdrMWN2QWtpTWQ3MFkKd0hLM240NnhTcE1YakZWdkxYTW01bkxyRUI1NGxnNm1YTTdycFUrT1VyN3IwbkhKUHZRZ2JFajJQN2RhZk8yaApNbkE2TmdHMzNDQ2x2by9tWmZuVlFXNXRZbStQTUtIOU1wVFZ6MGk4L0kwUThFVWl2aWdxNENvPQotLS0tLUVORCBSU0EgUFJJVkFURSBLRVktLS0tLQo=\n";
        // 订阅的资源类型（支持Namespace,Node,Pod,Service,Deployment,Job）,不同资源的TABLE定义不同
        String sourceType = "Pod";
        // 指定资源的命名空间
//        String nameSpace = "";
        String nameSpace = "default";
        // fieldSelector,labelSelector可以对资源进行筛选
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
                        // config文件路径
//                        + "',\n"
//                        + "  'config-path' = '"
//                        + configPath
                        // config
                        + "',\n"
                        + "  'config' = '"
                        + config
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
