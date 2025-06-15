package com.amannmalik.workflow.runtime.task;

import com.amannmalik.workflow.runtime.DefinitionHelper;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
import dev.restate.sdk.endpoint.definition.ServiceDefinition;
import io.serverlessworkflow.api.types.FlowDirective;
import io.serverlessworkflow.api.types.SwitchCase;
import io.serverlessworkflow.api.types.SwitchItem;
import io.serverlessworkflow.api.types.SwitchTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SwitchTaskService {

  public static final StateKey<String> NEXT = StateKey.of("switch-next", String.class);
  public static final ServiceDefinition DEFINITION =
          DefinitionHelper.taskService(
                  SwitchTaskService.class, SwitchTask.class, SwitchTaskService::execute);
  private static final Logger log = LoggerFactory.getLogger(SwitchTaskService.class);

  public static void execute(WorkflowContext ctx, SwitchTask task) {
    Pattern p = Pattern.compile("\\.(\\w+)\\s*==\\s*\"([^\"]*)\"");
    String defaultTarget = null;
    for (SwitchItem aSwitch : task.getSwitch()) {
      SwitchCase switchCase = aSwitch.getSwitchCase();
      String when = switchCase.getWhen();
      boolean match = false;
      if (when == null) {
        FlowDirective then = switchCase.getThen();
        if (then != null) {
          if (then.getFlowDirectiveEnum() != null) {
            defaultTarget = then.getFlowDirectiveEnum().name();
          } else if (then.getString() != null) {
            defaultTarget = then.getString();
          }
        }
        continue;
      } else {
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
        if (then != null) {
          if (then.getFlowDirectiveEnum() != null) {
            ctx.set(NEXT, then.getFlowDirectiveEnum().name());
          } else if (then.getString() != null) {
            ctx.set(NEXT, then.getString());
          }
        }
        return;
      }
    }
    if (defaultTarget != null) {
      ctx.set(NEXT, defaultTarget);
    }
  }
}
