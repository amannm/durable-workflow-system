package com.amannmalik.workflow.runtime.task.run;

import com.amannmalik.workflow.runtime.WorkflowRegistry;
import com.amannmalik.workflow.runtime.WorkflowRunner;
import dev.restate.sdk.WorkflowContext;
import io.serverlessworkflow.api.types.RunContainer;
import io.serverlessworkflow.api.types.RunScript;
import io.serverlessworkflow.api.types.RunShell;
import io.serverlessworkflow.api.types.RunTask;
import io.serverlessworkflow.api.types.RunTaskConfigurationUnion;
import io.serverlessworkflow.api.types.RunWorkflow;
import io.serverlessworkflow.api.types.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@dev.restate.sdk.annotation.Service
public class RunTaskService {

    private static final Logger log = LoggerFactory.getLogger(RunTaskService.class);

    @dev.restate.sdk.annotation.Handler
    public void execute(WorkflowContext ctx, RunTask task) {
        RunTaskConfigurationUnion run = task.getRun();
        switch (run.get()) {
            case RunContainer r -> log.info("Run container not implemented: {}", r);
            case RunScript r -> log.info("Run script not implemented: {}", r);
            case RunShell r -> {
                if (r.getShell() != null && r.getShell().getCommand() != null) {
                    try {
                        List<String> cmd = new java.util.ArrayList<>();
                        cmd.add(r.getShell().getCommand());
                        if (r.getShell().getArguments() != null) {
                            r.getShell().getArguments().getAdditionalProperties().values().forEach(v -> cmd.add(v.toString()));
                        }
                        ProcessBuilder pb = new ProcessBuilder(cmd);
                        if (r.getShell().getEnvironment() != null) {
                            r.getShell().getEnvironment().getAdditionalProperties().forEach((k, v) -> pb.environment().put(k, v.toString()));
                        }
                        pb.start().waitFor();
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                }
            }
            case RunWorkflow r -> handleRunWorkflow(ctx, r);
            default -> throw new UnsupportedOperationException();
        }
    }


    private static void handleRunWorkflow(WorkflowContext ctx, RunWorkflow run) {
        var cfg = run.getWorkflow();
        if (cfg == null) {
            return;
        }


        Workflow wf = WorkflowRegistry.get(cfg.getNamespace(), cfg.getName(), cfg.getVersion());
        if (wf == null) {
            log.warn("Sub-workflow not found: {}:{}:{}", cfg.getNamespace(), cfg.getName(), cfg.getVersion());
            return;
        }
        new WorkflowRunner().runInternal(ctx, wf);
    }

}
