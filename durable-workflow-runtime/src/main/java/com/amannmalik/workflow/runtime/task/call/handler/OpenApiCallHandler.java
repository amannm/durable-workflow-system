package com.amannmalik.workflow.runtime.task.call.handler;

import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
import io.serverlessworkflow.api.types.CallOpenAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenApiCallHandler implements CallHandler<CallOpenAPI> {
    private static final Logger log = LoggerFactory.getLogger(OpenApiCallHandler.class);
    private final StateKey<Object> resultKey;

    public OpenApiCallHandler(StateKey<Object> resultKey) {
        this.resultKey = resultKey;
    }

    @Override
    public void handle(WorkflowContext ctx, CallOpenAPI call) {
        log.info("Call OpenAPI not implemented: {}", call);
    }
}
