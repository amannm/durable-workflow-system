package com.amannmalik.workflow.runtime.task.run;

import com.amannmalik.workflow.runtime.DefinitionHelper;
import com.amannmalik.workflow.runtime.task.run.handler.ContainerRunHandler;
import com.amannmalik.workflow.runtime.task.run.handler.RunHandler;
import com.amannmalik.workflow.runtime.task.run.handler.ScriptRunHandler;
import com.amannmalik.workflow.runtime.task.run.handler.ShellRunHandler;
import com.amannmalik.workflow.runtime.task.run.handler.WorkflowRunHandler;
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

import java.util.List;
import java.util.Map;

public class RunTaskService {

    private static final Logger log = LoggerFactory.getLogger(RunTaskService.class);
    public static final ServiceDefinition DEFINITION = DefinitionHelper.taskService(
            RunTaskService.class,
            RunTask.class,
            RunTaskService::execute
    );

    private static final Map<Class<?>, RunHandler<?>> HANDLERS = Map.of(
            RunContainer.class, new ContainerRunHandler(),
            RunScript.class, new ScriptRunHandler(),
            RunShell.class, new ShellRunHandler(),
            RunWorkflow.class, new WorkflowRunHandler()
    );

    @SuppressWarnings("unchecked")
    public static void execute(WorkflowContext ctx, RunTask task) {
        RunTaskConfigurationUnion run = task.getRun();
        Object obj = run.get();
        RunHandler<Object> handler = (RunHandler<Object>) HANDLERS.get(obj.getClass());
        if (handler != null) {
            handler.handle(ctx, obj);
        } else {
            throw new UnsupportedOperationException();
        }
    }

}
