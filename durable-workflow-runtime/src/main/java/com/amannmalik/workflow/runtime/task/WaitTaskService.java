package com.amannmalik.workflow.runtime.task;

import com.amannmalik.workflow.runtime.DefinitionHelper;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.endpoint.definition.ServiceDefinition;
import io.serverlessworkflow.api.types.WaitTask;

import java.time.Duration;

public class WaitTaskService {

    public static final ServiceDefinition DEFINITION =
            DefinitionHelper.taskService(WaitTaskService.class, WaitTask.class, WaitTaskService::execute);

    public static void execute(WorkflowContext ctx, WaitTask task) {
        var w = task.getWait();
        var dur = w.getDurationExpression();
        var i = w.getDurationInline();
        Duration d = (dur != null)
                ? Duration.parse(dur)
                : Duration.ZERO
                        .plusDays(i.getDays())
                        .plusHours(i.getHours())
                        .plusMinutes(i.getMinutes())
                        .plusSeconds(i.getSeconds())
                        .plusMillis(i.getMilliseconds());
        ctx.sleep(d);
    }
}
