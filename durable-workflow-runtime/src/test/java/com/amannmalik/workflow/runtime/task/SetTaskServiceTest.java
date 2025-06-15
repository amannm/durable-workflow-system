package com.amannmalik.workflow.runtime.task;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amannmalik.workflow.runtime.testutil.FakeWorkflowContext;
import dev.restate.sdk.common.StateKey;
import io.serverlessworkflow.api.types.SetTask;
import io.serverlessworkflow.api.types.SetTaskConfiguration;
import org.junit.jupiter.api.Test;

class SetTaskServiceTest {
  @Test
  void setsVariousTypes() {
    FakeContext ctx = new FakeContext();
    SetTask st = new SetTask();
    io.serverlessworkflow.api.types.Set s = new io.serverlessworkflow.api.types.Set();
    s.setString("foo");
    st.setSet(s);
    SetTaskService.execute(ctx, st);
    assertEquals("foo", ctx.get(StateKey.of("foo", String.class)).orElse(null));
  }

  @Test
  void setsMapValues() {
    FakeContext ctx = new FakeContext();
    SetTask st = new SetTask();
    io.serverlessworkflow.api.types.Set s = new io.serverlessworkflow.api.types.Set();
    SetTaskConfiguration cfg = new SetTaskConfiguration();
    cfg.setAdditionalProperty("a", "b");
    cfg.setAdditionalProperty("n", 1);
    s.setSetTaskConfiguration(cfg);
    st.setSet(s);
    SetTaskService.execute(ctx, st);
    assertEquals("b", ctx.get(StateKey.of("a", String.class)).orElse(null));
    assertEquals(1, ctx.get(StateKey.of("n", Integer.class)).orElse(null));
  }

  static class FakeContext extends FakeWorkflowContext {}
}
