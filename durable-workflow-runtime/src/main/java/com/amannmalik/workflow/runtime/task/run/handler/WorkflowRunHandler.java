package com.amannmalik.workflow.runtime.task.run.handler;

import com.amannmalik.workflow.runtime.Services;
import com.amannmalik.workflow.runtime.WorkflowRegistry;
import dev.restate.sdk.WorkflowContext;
import io.serverlessworkflow.api.types.RunWorkflow;
import io.serverlessworkflow.api.types.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkflowRunHandler implements RunHandler<RunWorkflow> {
    private static final Logger log = LoggerFactory.getLogger(WorkflowRunHandler.class);

    @Override
    public void handle(WorkflowContext ctx, RunWorkflow run) {
        var cfg = run.getWorkflow();
        if (cfg == null) {
            return;
        }

        Workflow wf = WorkflowRegistry.get(cfg.getNamespace(), cfg.getName(), cfg.getVersion());
        if (wf == null) {
            log.warn(
                    "Sub-workflow not found: {}:{}:{}", cfg.getNamespace(), cfg.getName(), cfg.getVersion());
            return;
        }
        Services.callService(ctx, "WorkflowRunner", "runInternal", wf, Void.class).await();
    }
}
