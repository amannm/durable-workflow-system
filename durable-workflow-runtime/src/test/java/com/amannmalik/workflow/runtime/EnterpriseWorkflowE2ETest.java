package com.amannmalik.workflow.runtime;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.amannmalik.workflow.runtime.task.EmitTaskService;
import com.amannmalik.workflow.runtime.task.ForkTaskService;
import com.amannmalik.workflow.runtime.task.ListenTaskService;
import com.amannmalik.workflow.runtime.task.SetTaskService;
import com.amannmalik.workflow.runtime.task.SwitchTaskService;
import com.amannmalik.workflow.runtime.task.TryTaskService;
import com.amannmalik.workflow.runtime.task.WaitTaskService;
import com.amannmalik.workflow.runtime.task.WorkflowTaskService;
import com.amannmalik.workflow.runtime.task.call.CallTaskService;
import com.amannmalik.workflow.runtime.task.run.RunTaskService;
import com.amannmalik.workflow.runtime.testutil.FakeWorkflowContext;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import dev.restate.common.Request;
import dev.restate.common.Target;
import dev.restate.sdk.CallDurableFuture;
import dev.restate.sdk.DurableFuture;
import dev.restate.sdk.InvocationHandle;
import dev.restate.sdk.common.HandlerRequest;
import dev.restate.sdk.common.RetryPolicy;
import dev.restate.sdk.endpoint.definition.AsyncResult;
import dev.restate.sdk.endpoint.definition.HandlerContext;
import dev.restate.serde.TypeTag;
import io.serverlessworkflow.api.types.CallTask;
import io.serverlessworkflow.api.types.EmitTask;
import io.serverlessworkflow.api.types.ForkTask;
import io.serverlessworkflow.api.types.ListenTask;
import io.serverlessworkflow.api.types.RunTask;
import io.serverlessworkflow.api.types.SetTask;
import io.serverlessworkflow.api.types.SwitchTask;
import io.serverlessworkflow.api.types.Task;
import io.serverlessworkflow.api.types.TryTask;
import io.serverlessworkflow.api.types.WaitTask;
import io.serverlessworkflow.api.types.Workflow;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EnterpriseWorkflowE2ETest {
  private WireMockServer server;

  @BeforeEach
  void setup() {
    server = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
    server.start();
    WireMock.configureFor("localhost", server.port());
    server.stubFor(any(anyUrl()).willReturn(aResponse().withStatus(200).withBody("{}")));
  }

  @AfterEach
  void tearDown() {
    server.stop();
  }

  @Test
  void runEnterpriseWorkflow() throws Exception {
    String yaml = Files.readString(Path.of("src/test/resources/enterprise.yaml"));
    yaml = yaml.replaceAll("https?://[A-Za-z0-9\\.-]+", server.baseUrl());
    Workflow wf =
        WorkflowLoader.fromYaml(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)));
    FakeContext ctx = new FakeContext();
    WorkflowRunner.runInternal(ctx, wf);
    assertNotNull(ctx);
  }

  static class FakeContext extends FakeWorkflowContext {
    int counter = 0;

    @Override
    public HandlerRequest request() {
      return null;
    }

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
      } else if ("SetTaskService".equals(svc) && "execute".equals(m)) {
        SetTaskService.execute(this, (SetTask) req);
        return completed();
      } else if ("SwitchTaskService".equals(svc) && "execute".equals(m)) {
        SwitchTaskService.execute(this, (SwitchTask) req);
        return completed();
      } else if ("CallTaskService".equals(svc) && "execute".equals(m)) {
        CallTaskService.execute(this, (CallTask) req);
        return completed();
      } else if ("ListenTaskService".equals(svc) && "execute".equals(m)) {
        ListenTaskService.execute(this, (ListenTask) req);
        return completed();
      } else if ("EmitTaskService".equals(svc) && "execute".equals(m)) {
        EmitTaskService.execute(this, (EmitTask) req);
        return completed();
      } else if ("TryTaskService".equals(svc) && "execute".equals(m)) {
        TryTaskService.execute(this, (TryTask) req);
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
      return new com.amannmalik.workflow.runtime.testutil.SimpleInvocationHandle<>(
          "inv-" + (counter++));
    }

    @Override
    public <R> InvocationHandle<R> invocationHandle(String id, TypeTag<R> typeTag) {
      return new com.amannmalik.workflow.runtime.testutil.SimpleInvocationHandle<>(id);
    }

    @Override
    public DurableFuture<Void> timer(String id, Duration duration) {
      return new com.amannmalik.workflow.runtime.testutil.SimpleDurableFuture<>(null);
    }

    @Override
    public <T> DurableFuture<T> runAsync(
        String name,
        TypeTag<T> typeTag,
        RetryPolicy policy,
        dev.restate.common.function.ThrowingSupplier<T> supplier) {
      try {
        return new com.amannmalik.workflow.runtime.testutil.SimpleDurableFuture<>(supplier.get());
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
                new com.amannmalik.workflow.runtime.testutil.SimpleAsyncResult<>(null),
                new com.amannmalik.workflow.runtime.testutil.SimpleDurableFuture<>("id"));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
