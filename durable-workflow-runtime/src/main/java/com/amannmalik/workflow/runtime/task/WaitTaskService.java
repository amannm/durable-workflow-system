package com.amannmalik.workflow.runtime.task;

import dev.restate.sdk.WorkflowContext;
import io.serverlessworkflow.api.types.WaitTask;

import java.time.Duration;

@dev.restate.sdk.annotation.Service
public class WaitTaskService {

    @dev.restate.sdk.annotation.Handler
    public void execute(WorkflowContext ctx, WaitTask task) {
        var wtc = task.getWait();
        var de = wtc.getDurationExpression();
        Duration resolvedDuration = Duration.ZERO;
        if (de != null) {
            resolvedDuration = Duration.parse(de);
        } else {
            var duri = wtc.getDurationInline();
            resolvedDuration = resolvedDuration.plusDays(duri.getDays())
                    .plusHours(duri.getHours())
                    .plusMinutes(duri.getMinutes())
                    .plusSeconds(duri.getSeconds())
                    .plusMillis(duri.getMilliseconds());
        }
        ctx.sleep(resolvedDuration);
    }
}
