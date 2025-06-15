package com.amannmalik.workflow.runtime.task;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amannmalik.workflow.runtime.testutil.FakeWorkflowContext;
import io.serverlessworkflow.api.types.DurationInline;
import io.serverlessworkflow.api.types.TimeoutAfter;
import io.serverlessworkflow.api.types.WaitTask;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class WaitTaskServiceTest {
  @Test
  void waitWithDurationExpression() {
    FakeContext ctx = new FakeContext();
    TimeoutAfter ta = new TimeoutAfter();
    ta.setDurationExpression("PT2S");
    WaitTask wt = new WaitTask();
    wt.setWait(ta);
    WaitTaskService.execute(ctx, wt);
    assertEquals(Duration.parse("PT2S"), ctx.sleeps.getFirst());
  }

  @Test
  void waitWithDurationInline() {
    FakeContext ctx = new FakeContext();
    TimeoutAfter ta = new TimeoutAfter();
    DurationInline di = new DurationInline();
    di.setSeconds(1);
    di.setMilliseconds(500);
    ta.setDurationInline(di);
    WaitTask wt = new WaitTask();
    wt.setWait(ta);
    WaitTaskService.execute(ctx, wt);
    assertEquals(Duration.ofSeconds(1).plusMillis(500), ctx.sleeps.getFirst());
  }

  static class FakeContext extends FakeWorkflowContext {}
}
