package com.amannmalik.workflow.runtime.task;

import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
import io.serverlessworkflow.api.types.FlowDirective;
import io.serverlessworkflow.api.types.SwitchCase;
import io.serverlessworkflow.api.types.SwitchItem;
import io.serverlessworkflow.api.types.SwitchTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@dev.restate.sdk.annotation.Service
public class SwitchTaskService {

    private static final Logger log = LoggerFactory.getLogger(SwitchTaskService.class);

    @dev.restate.sdk.annotation.Handler
    public void execute(WorkflowContext ctx, SwitchTask task) {
        Pattern p = Pattern.compile("\\.(\\w+)\\s*==\\s*\"([^\"]*)\"");
        for (SwitchItem aSwitch : task.getSwitch()) {
            SwitchCase switchCase = aSwitch.getSwitchCase();
            String when = switchCase.getWhen();
            boolean match = false;
            if (when != null) {
                Matcher m = p.matcher(when);
                if (m.matches()) {
                    var key = m.group(1);
                    var expected = m.group(2);
                    var sk = StateKey.of(key, String.class);
                    match = ctx.get(sk).map(expected::equals).orElse(false);
                }
            }
            if (match) {
                FlowDirective then = switchCase.getThen();
                if (then.getFlowDirectiveEnum() != null) {
                    return;
                }
            }
        }
    }
}
