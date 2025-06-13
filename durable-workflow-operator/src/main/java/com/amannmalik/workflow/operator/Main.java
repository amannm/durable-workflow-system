package com.amannmalik.workflow.operator;

import com.amannmalik.workflow.operator.reconciler.DurableWorkflowReconciler;
import com.amannmalik.workflow.operator.service.WorkflowApiServer;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.javaoperatorsdk.operator.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        log.info("Starting Durable Workflow Operator");
        var clientConfig = new ConfigBuilder()
                .build();
        try (var client = new KubernetesClientBuilder().withConfig(clientConfig).build()) {
            var operator = new Operator(o -> o.withKubernetesClient(client));
            operator.register(new DurableWorkflowReconciler());
            var apiServer = new WorkflowApiServer(client);
            apiServer.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    apiServer.stop();
                } catch (Exception e) {
                    log.error("Failed to stop API server", e);
                }
            }));
            operator.start();
        } catch (Exception e) {
            log.error("Operator failed", e);
        }
    }
}
