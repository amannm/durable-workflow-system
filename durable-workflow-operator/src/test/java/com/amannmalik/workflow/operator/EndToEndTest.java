package com.amannmalik.workflow.operator;

import com.amannmalik.workflow.operator.model.DurableWorkflow;
import com.amannmalik.workflow.operator.service.WorkflowServlet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@EnableKubernetesMockClient(crud = true)
public class EndToEndTest {

    KubernetesMockServer server;
    KubernetesClient client;

    @AfterEach
    void cleanup() {
        server.clearExpectations();
    }

    @Test
    void testNewDeployment() throws Exception {
        var servlet = new WorkflowServlet(client);
        String yaml = "apiVersion: amannmalik.com/v1alpha1\n" +
                "kind: DurableWorkflow\n" +
                "metadata:\n  name: wf\n" +
                "spec:\n  definition: {}\n";
        HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
        ServletInputStream sis = new ServletInputStream() {
            private final java.io.InputStream in = new java.io.ByteArrayInputStream(yaml.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            @Override public int read() throws java.io.IOException { return in.read(); }
            @Override public boolean isFinished() {
                try {
                    return in.available() == 0;
                } catch (java.io.IOException e) {
                    return true;
                }
            }
            @Override public boolean isReady() { return true; }
            @Override public void setReadListener(jakarta.servlet.ReadListener l) { }
        };
        Mockito.when(req.getInputStream()).thenReturn(sis);
        Mockito.when(req.getMethod()).thenReturn("POST");
        HttpServletResponse resp = Mockito.mock(HttpServletResponse.class);
        servlet.service(req, resp);

        Mockito.verify(resp).setStatus(Mockito.anyInt());
    }
}
