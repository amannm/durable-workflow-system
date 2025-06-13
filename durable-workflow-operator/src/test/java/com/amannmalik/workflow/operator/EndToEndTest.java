package com.amannmalik.workflow.operator;

import com.amannmalik.workflow.operator.reconciler.DurableWorkflowReconciler;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.javaoperatorsdk.operator.Operator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

@EnableKubernetesMockClient(crud = true)
public class EndToEndTest {

    KubernetesMockServer server;
    KubernetesClient client;

    @AfterEach
    void cleanup() {
        server.clearExpectations();
    }

    @Test
    void testNewDeployment() {
        // TODO: mock all required http calls to mock server
        // server.expect() ... something something
        var operator = new Operator(o -> o
                .withKubernetesClient(client)
                .withStopOnInformerErrorDuringStartup(false)
                .withCloseClientOnStop(false));
        operator.register(new DurableWorkflowReconciler());
        operator.start();
        //TODO: post to servlet endpoint then wait for reconciler to reconcile and update the status to the mock server
        operator.stop();
    }
}
