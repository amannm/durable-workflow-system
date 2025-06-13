package com.amannmalik.workflow.operator.service;

import io.fabric8.kubernetes.client.KubernetesClient;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;

public class WorkflowApiServer {
    private final Server server;
    private final int port;

    public WorkflowApiServer(KubernetesClient client) {
        this(client, 8080);
    }

    public WorkflowApiServer(KubernetesClient client, int port) {
        this.port = port;
        this.server = new Server(port);
        var context = new ServletContextHandler();
        context.setContextPath("/");
        context.addServlet(new ServletHolder(new WorkflowServlet(client)), "/workflows");
        server.setHandler(context);
    }

    public void start() throws Exception {
        server.start();
    }

    public void stop() throws Exception {
        server.stop();
    }

    public int getPort() {
        if (port == 0) {
            return server.getURI().getPort();
        }
        return port;
    }
}
