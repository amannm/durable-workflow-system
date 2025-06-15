package com.amannmalik.workflow.runtime.task.run.handler;

import dev.restate.sdk.WorkflowContext;
import io.serverlessworkflow.api.types.RunContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainerRunHandler implements RunHandler<RunContainer> {
    private static final Logger log = LoggerFactory.getLogger(ContainerRunHandler.class);

    @Override
    public void handle(WorkflowContext ctx, RunContainer run) {
        log.info("Run container not implemented: {}", run);
    }
}
