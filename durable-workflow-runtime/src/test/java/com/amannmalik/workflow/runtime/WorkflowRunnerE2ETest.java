package com.amannmalik.workflow.runtime;

import com.amannmalik.workflow.runtime.cron.CronJob;
import com.amannmalik.workflow.runtime.cron.CronJobInfo;
import com.amannmalik.workflow.runtime.cron.CronJobRequest;
import com.amannmalik.workflow.runtime.task.ForkTaskService;
import com.amannmalik.workflow.runtime.task.WaitTaskService;
import com.amannmalik.workflow.runtime.task.WorkflowTaskService;
import com.amannmalik.workflow.runtime.task.run.RunTaskService;
import com.amannmalik.workflow.runtime.testutil.FakeWorkflowContext;
import com.amannmalik.workflow.runtime.testutil.SimpleAsyncResult;
import com.amannmalik.workflow.runtime.testutil.SimpleDurableFuture;
import com.amannmalik.workflow.runtime.testutil.SimpleInvocationHandle;
import dev.restate.common.Request;
import dev.restate.common.Target;
import dev.restate.common.function.ThrowingSupplier;
import dev.restate.sdk.CallDurableFuture;
import dev.restate.sdk.DurableFuture;
import dev.restate.sdk.InvocationHandle;
import dev.restate.sdk.common.RetryPolicy;
import dev.restate.sdk.endpoint.definition.AsyncResult;
import dev.restate.sdk.endpoint.definition.HandlerContext;
import dev.restate.serde.TypeTag;
import io.serverlessworkflow.api.types.Document;
import io.serverlessworkflow.api.types.ForkTask;
import io.serverlessworkflow.api.types.ForkTaskConfiguration;
import io.serverlessworkflow.api.types.RunTask;
import io.serverlessworkflow.api.types.RunTaskConfigurationUnion;
import io.serverlessworkflow.api.types.RunWorkflow;
import io.serverlessworkflow.api.types.SubflowConfiguration;
import io.serverlessworkflow.api.types.Task;
import io.serverlessworkflow.api.types.TaskItem;
import io.serverlessworkflow.api.types.TimeoutAfter;
import io.serverlessworkflow.api.types.WaitTask;
import io.serverlessworkflow.api.types.Workflow;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class WorkflowRunnerE2ETest {

  static Workflow waitWorkflow() {
    TimeoutAfter ta = new TimeoutAfter().withDurationExpression("PT1S");
    WaitTask wt = new WaitTask().withWait(ta);
    Task t = new Task();
    t.setWaitTask(wt);
    TaskItem ti = new TaskItem("w", t);
    Document doc = new Document().withNamespace("ns").withName("wait").withVersion("v1");
    return new Workflow(doc, List.of(ti));
  }

  static Workflow forkWorkflow() {
    TimeoutAfter ta = new TimeoutAfter().withDurationExpression("PT0S");
    WaitTask wt = new WaitTask().withWait(ta);
    Task t = new Task();
    t.setWaitTask(wt);
    TaskItem ti1 = new TaskItem("a", t);
    TaskItem ti2 = new TaskItem("b", t);
    ForkTaskConfiguration cfg = new ForkTaskConfiguration(List.of(ti1, ti2));
    ForkTask ft = new ForkTask().withFork(cfg);
    Task forkTask = new Task();
    forkTask.setForkTask(ft);
    TaskItem ti = new TaskItem("fork", forkTask);
    Document doc = new Document().withNamespace("ns").withName("fork").withVersion("v1");
    return new Workflow(doc, List.of(ti));
  }

  static Workflow subWorkflow() {
    TimeoutAfter ta = new TimeoutAfter().withDurationExpression("PT0S");
    WaitTask wt = new WaitTask().withWait(ta);
    Task t = new Task();
    t.setWaitTask(wt);
    TaskItem ti = new TaskItem("s", t);
    Document doc = new Document().withNamespace("ns2").withName("sub").withVersion("v1");
    return new Workflow(doc, List.of(ti));
  }

  static Workflow runWorkflow() {
    WorkflowRegistry.register(subWorkflow());
    SubflowConfiguration sc =
            new SubflowConfiguration().withNamespace("ns2").withName("sub").withVersion("v1");
    RunWorkflow rw = new RunWorkflow();
    rw.setWorkflow(sc);
    RunTaskConfigurationUnion union = new RunTaskConfigurationUnion();
    union.setRunWorkflow(rw);
    RunTask rt = new RunTask().withRun(union);
    Task t = new Task();
    t.setRunTask(rt);
    TaskItem ti = new TaskItem("run", t);
    Document doc = new Document().withNamespace("ns").withName("run").withVersion("v1");
    return new Workflow(doc, List.of(ti));
  }

  static Stream<Arguments> scenarios() {
    return Stream.of(
            Arguments.of(
                    "wait",
                    (Scenario)
                            ctx -> {
                              WorkflowRunner.runInternal(ctx, waitWorkflow());
                              assertEquals(Duration.parse("PT1S"), ctx.sleeps.getFirst());
                            }),
            Arguments.of(
                    "fork",
                    (Scenario)
                            ctx -> {
                              WorkflowRunner.runInternal(ctx, forkWorkflow());
                              assertEquals(2, ctx.sleeps.size());
                            }),
            Arguments.of(
                    "runWorkflow",
                    (Scenario)
                            ctx -> {
                              WorkflowRunner.runInternal(ctx, runWorkflow());
                              assertEquals(1, ctx.sleeps.size());
                            }),
            Arguments.of(
                    "cronJob",
                    (Scenario)
                            ctx -> {
                              CronJobRequest req =
                                      new CronJobRequest(
                                              "* * * * *",
                                              "S",
                                              "M",
                                              Optional.empty(),
                                              Optional.empty(),
                                              Optional.empty());
                              CronJobInfo info = CronJob.initiate(ctx, req);
                              assertNotNull(info);
                              CronJob.execute(ctx);
                              assertFalse(ctx.state.isEmpty());
                            }));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("scenarios")
  void runScenarios(String name, Scenario sc) {
    FakeContext ctx = new FakeContext();
    sc.run(ctx);
  }

  interface Scenario {
    void run(FakeContext ctx);
  }

  static class FakeContext extends FakeWorkflowContext {
    int counter = 0;

    @Override
    public <T, R> CallDurableFuture<R> call(Request<T, R> request) {
      Target t = request.getTarget();
      String svc = t.getService();
      String m = t.getHandler();
      Object req = request.getRequest();
      if ("WaitTaskService".equals(svc) && "execute".equals(m)) {
        WaitTaskService.execute(this, (WaitTask) req);
        return completed();
      } else if ("ForkTaskService".equals(svc) && "execute".equals(m)) {
        ForkTaskService.execute(this, (ForkTask) req);
        return completed();
      } else if ("RunTaskService".equals(svc) && "execute".equals(m)) {
        RunTaskService.execute(this, (RunTask) req);
        return completed();
      } else if ("WorkflowTaskService".equals(svc) && "execute".equals(m)) {
        WorkflowTaskService.execute(this, (Task) req);
        return completed();
      } else if ("WorkflowRunner".equals(svc) && "runInternal".equals(m)) {
        WorkflowRunner.runInternal(this, (Workflow) req);
        return completed();
      }
      return null;
    }

    @Override
    public <T, R> InvocationHandle<R> send(Request<T, R> request, Duration delay) {
      return new SimpleInvocationHandle<>(
              "inv-" + (counter++));
    }

    @Override
    public <R> InvocationHandle<R> invocationHandle(String id, TypeTag<R> typeTag) {
      return new SimpleInvocationHandle<>(id);
    }

    @Override
    public DurableFuture<Void> timer(String id, Duration duration) {
      return new SimpleDurableFuture<>(null);
    }

    @Override
    public <T> DurableFuture<T> runAsync(
            String name,
            TypeTag<T> typeTag,
            RetryPolicy policy,
            ThrowingSupplier<T> supplier) {
      try {
        return new SimpleDurableFuture<>(supplier.get());
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    }

    private <R> CallDurableFuture<R> completed() {
      try {
        var ctor =
                CallDurableFuture.class.getDeclaredConstructor(
                        HandlerContext.class, AsyncResult.class, DurableFuture.class);
        ctor.setAccessible(true);
        return (CallDurableFuture<R>)
                ctor.newInstance(
                        null,
                        new SimpleAsyncResult<>(null),
                        new SimpleDurableFuture<>("id"));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
