package com.amannmalik.workflow.runtime.task;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amannmalik.workflow.runtime.testutil.FakeWorkflowContext;
import dev.restate.sdk.common.StateKey;
import io.serverlessworkflow.api.types.FlowDirective;
import io.serverlessworkflow.api.types.SwitchCase;
import io.serverlessworkflow.api.types.SwitchItem;
import io.serverlessworkflow.api.types.SwitchTask;
import java.util.List;
import org.junit.jupiter.api.Test;

class SwitchTaskServiceTest {

  @Test
  void matchesCase() {
    FakeContext ctx = new FakeContext();
    ctx.set(StateKey.of("foo", String.class), "bar");
    SwitchCase sc = new SwitchCase();
    sc.setWhen(".foo == \"bar\"");
    FlowDirective fd = new FlowDirective();
    fd.setString("NEXT");
    sc.setThen(fd);
    SwitchItem si = new SwitchItem("c1", sc);
    SwitchTask task = new SwitchTask();
    task.setSwitch(List.of(si));
    SwitchTaskService.execute(ctx, task);
    assertEquals("NEXT", ctx.get(SwitchTaskService.NEXT).orElse(null));
  }

  @Test
  void usesDefaultCase() {
    FakeContext ctx = new FakeContext();
    SwitchCase sc1 = new SwitchCase();
    sc1.setWhen(".foo == \"bar\"");
    FlowDirective fd1 = new FlowDirective();
    fd1.setString("CASE");
    sc1.setThen(fd1);
    SwitchCase scDef = new SwitchCase();
    FlowDirective fdDef = new FlowDirective();
    fdDef.setString("DEFAULT");
    scDef.setThen(fdDef);
    SwitchItem si1 = new SwitchItem("c1", sc1);
    SwitchItem si2 = new SwitchItem("default", scDef);
    SwitchTask task = new SwitchTask();
    task.setSwitch(List.of(si1, si2));
    SwitchTaskService.execute(ctx, task);
    assertEquals("DEFAULT", ctx.get(SwitchTaskService.NEXT).orElse(null));
  }

  static class FakeContext extends FakeWorkflowContext {}
}
