package com.amannmalik.workflow.runtime.task.run;

import com.amannmalik.workflow.runtime.DefinitionHelper;
import com.amannmalik.workflow.runtime.task.run.handler.*;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.endpoint.definition.ServiceDefinition;
import io.serverlessworkflow.api.types.RunContainer;
import io.serverlessworkflow.api.types.RunScript;
import io.serverlessworkflow.api.types.RunShell;
import io.serverlessworkflow.api.types.RunTask;
import io.serverlessworkflow.api.types.RunTaskConfigurationUnion;
import io.serverlessworkflow.api.types.RunWorkflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunTaskService {

    private static final Logger log = LoggerFactory.getLogger(RunTaskService.class);
    public static final ServiceDefinition DEFINITION =
            DefinitionHelper.taskService(RunTaskService.class, RunTask.class, RunTaskService::execute);

    public static void execute(WorkflowContext ctx, RunTask task) {
        Object obj = task.getRun().get();
        switch (obj) {
            case RunContainer r -> new ContainerRunHandler().handle(ctx, r);
            case RunScript r -> new ScriptRunHandler().handle(ctx, r);
            case RunShell r -> new ShellRunHandler().handle(ctx, r);
            case RunWorkflow r -> new WorkflowRunHandler().handle(ctx, r);
            default -> throw new UnsupportedOperationException();
        }
    }
}
