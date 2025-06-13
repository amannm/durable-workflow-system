package com.amannmalik.workflow.operator.service;

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
        // TODO: parse request for DurableWorkflow, map it to the resource class, then call kubernetes control plane and post it for the operator to pick up on next reconciliation

    }
}
