package com.amannmalik.workflow.operator.service;

import com.amannmalik.workflow.operator.model.DurableWorkflow;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;

public class WorkflowServlet extends HttpServlet {
    @Serial
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(WorkflowServlet.class);
    private transient KubernetesClient client;

    public WorkflowServlet(KubernetesClient client) {
        this.client = client;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) {
        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            mapper.registerModule(new JavaTimeModule());
            DurableWorkflow workflow = mapper.readValue(req.getInputStream(), DurableWorkflow.class);
            String ns = java.util.Optional.ofNullable(workflow.getMetadata().getNamespace())
                            .filter(s -> !s.isBlank())
                            .orElse("default");
            workflow.getMetadata().setNamespace(ns);
            client.resource(workflow).inNamespace(ns).createOrReplace();
            resp.setStatus(HttpServletResponse.SC_CREATED);
        } catch (Exception e) {
            log.error("Failed to create workflow", e);
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        }
    }
}
