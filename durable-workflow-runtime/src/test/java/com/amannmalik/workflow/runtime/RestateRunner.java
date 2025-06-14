// Copyright (c) 2023 - Restate Software, Inc., Restate GmbH
//
// This file is part of the Restate Java SDK,
// which is released under the MIT license.
//
// You can find a copy of the license in file LICENSE in the root
// directory of this repository or package, or at
// https://github.com/restatedev/sdk-java/blob/main/LICENSE
package com.amannmalik.workflow.runtime;

import dev.restate.sdk.endpoint.Endpoint;
import dev.restate.sdk.http.vertx.RestateHttpServer;
import io.vertx.core.http.HttpServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class RestateRunner implements AutoCloseable {

    private static final Logger LOG = LogManager.getLogger(RestateRunner.class);

    private final HttpServer server;
    private final Path configLocation;
    private final int ingressPortNumber;
    private final int adminPortNumber;
    private final Map<String, String> additionalEnvs;
    private Process process;

    public RestateRunner(Endpoint endpoint, Path configLocation, int ingressPortNumber, int adminPortNumber, Map<String, String> additionalEnvs) {
        this.server = RestateHttpServer.fromEndpoint(endpoint);
        this.configLocation = configLocation;
        this.ingressPortNumber = ingressPortNumber;
        this.adminPortNumber = adminPortNumber;
        this.additionalEnvs = additionalEnvs;
    }

    /**
     * Run restate, run the embedded service endpoint server, and register the services.
     */
    public void start() {
        // Start listening the local server
        try {
            server.listen(0).toCompletionStage().toCompletableFuture().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        // Expose the server port
        int serviceEndpointPort = server.actualPort();
        LOG.debug("Started embedded service endpoint server on port {}", serviceEndpointPort);

        var processBuilder = new ProcessBuilder().command("restate-server");
        var envs = processBuilder.environment();
        envs.putAll(Map.of(
                "RUST_LOG", "warn",
                "RESTATE_WORKER__INGRESS__BIND_ADDRESS", "0.0.0.0:" + ingressPortNumber,
                "RESTATE_META__REST_ADDRESS", "0.0.0.0:" + adminPortNumber,
                "RESTATE_CONFIG", configLocation.toAbsolutePath().toString()));
        envs.putAll(additionalEnvs);
        // Now create the runtime container and deploy it
        try {
            this.process = processBuilder.start();
            // TODO: wait until /health 200 on RESTATE_ADMIN_ENDPOINT_PORT
            // TODO: wait until /restate/health 200 on RESTATE_INGRESS_ENDPOINT_PORT
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        LOG.debug("Started Restate container");
    }

    @Override
    public void close() {
        process.destroy();
        LOG.debug("Stopped Restate container");
        server.close().toCompletionStage().toCompletableFuture().join();
        LOG.debug("Stopped Embedded Service endpoint server");
    }
}
